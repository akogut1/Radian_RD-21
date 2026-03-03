"""
Radian RD-21 direct serial reader.
This script connects directly to the Radian RD-21 over a serial port,
sends the command to read all instant metrics for 3 phases, parses the 
response, and saves the data to a CSV file. It uses pyserial for communication 
and includes error handling and logging.
"""

import argparse
import csv
import logging
import struct
import sys
import time
from dataclasses import dataclass, fields
from datetime import datetime
from typing import Optional

import serial
import serial.tools.list_ports


# ── Logging ────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("radian_rd21")


# ── Protocol constants ──────────────────────────────────────────────────────────

SYNC_BYTE        = 0xA6
CMD_IDENTIFY     = 0x02
CMD_GET_INSTANT  = 0x0D

# Payload for the "read all instant metrics" command (matches what the main GUI sends)
INSTANT_PAYLOAD  = bytes([0x00, 0x24, 0x00, 0x00, 0x00, 0x14, 0xFF, 0xFD])

BYTES_PER_FLOAT  = 4
FLOATS_PER_PHASE = 8    # volt, amp, watt, va, var, freq, phase_angle, power_factor
NUM_PHASES       = 3
PHASE_LABELS     = ("A", "B", "C")

# Minimum payload bytes for a valid 3-phase response
MIN_PAYLOAD_3PH  = NUM_PHASES * FLOATS_PER_PHASE * BYTES_PER_FLOAT  # 96 bytes


# ── TI DSP float → Python float ────────────────────────────────────────────────

def ti_float_to_python(raw: bytes) -> float:
    """
    Convert a 4-byte TI DSP floating-point value to a standard Python float.

    TI format:
        byte[0]        = exponent, biased by 128  (0 → value is zero)
        byte[1] bit 7  = sign (1 = negative)
        byte[1] bits 6-0 + byte[2] + byte[3] = 23-bit mantissa (MSB first)
        Hidden leading 1 is implicit (same as IEEE-754 normal numbers).

    This is the same logic used by ti_float_to_ieee_single in the original
    project's src/util/data_conversion.py.
    """
    if len(raw) < 4:
        return 0.0

    exp = raw[0]
    if exp == 0:
        return 0.0

    sign     = (raw[1] & 0x80) >> 7
    # Reconstruct full 24-bit mantissa: hidden 1 + 23 explicit bits
    mantissa = ((raw[1] & 0x7F) | 0x80) << 16 | (raw[2] << 8) | raw[3]
    # exp is biased 128; mantissa has 23 fractional bits → subtract 23 from bias
    value    = float(mantissa) * (2.0 ** (exp - 128 - 23))
    return -value if sign else value


# ── Packet framing ─────────────────────────────────────────────────────────────

def _checksum(data: bytes) -> int:
    """XOR checksum over all bytes in data."""
    result = 0
    for b in data:
        result ^= b
    return result


def build_frame(cmd: int, payload: bytes = b"") -> bytes:
    """
    Build a complete Radian serial frame including sync byte and checksum.

    Frame layout:
        [0xA6] [cmd] [len_lo] [len_hi] [payload...] [checksum]

    The checksum covers bytes 1 (cmd) through the last payload byte.
    """
    length_lo = len(payload) & 0xFF
    length_hi = (len(payload) >> 8) & 0xFF
    inner = bytes([cmd, length_lo, length_hi]) + payload
    cs    = _checksum(inner)
    return bytes([SYNC_BYTE]) + inner + bytes([cs])


def read_response(ser: serial.Serial, timeout_s: float = 2.0) -> Optional[bytes]:
    """
    Read one complete response frame from the serial port.

    Returns the raw payload bytes (everything between the length field and the
    checksum byte), or None on timeout / framing error.
    """
    deadline = time.monotonic() + timeout_s

    # Wait for sync byte
    while time.monotonic() < deadline:
        b = ser.read(1)
        if not b:
            continue
        if b[0] == SYNC_BYTE:
            break
    else:
        log.warning("Timeout waiting for sync byte 0xA6")
        return None

    # Read 3-byte header: [cmd, len_lo, len_hi]
    header = _read_exact(ser, 3, deadline)
    if header is None:
        log.warning("Timeout reading response header")
        return None

    _cmd_echo = header[0]
    data_len  = header[1] | (header[2] << 8)

    # Read payload + checksum
    body = _read_exact(ser, data_len + 1, deadline)
    if body is None:
        log.warning(f"Timeout reading response payload ({data_len} bytes expected)")
        return None

    payload  = body[:-1]
    recv_cs  = body[-1]

    # Verify checksum (covers cmd byte + len bytes + payload)
    calc_cs = _checksum(header + payload)
    if calc_cs != recv_cs:
        log.warning(f"Checksum mismatch: calculated 0x{calc_cs:02X}, received 0x{recv_cs:02X}")
        # Return payload anyway — checksum errors can be noise-related; let caller decide

    return payload


def _read_exact(ser: serial.Serial, n: int, deadline: float) -> Optional[bytes]:
    """Read exactly n bytes before deadline, returning None on timeout."""
    buf = b""
    while len(buf) < n and time.monotonic() < deadline:
        chunk = ser.read(n - len(buf))
        if chunk:
            buf += chunk
    return buf if len(buf) == n else None


# ── Phase data container ────────────────────────────────────────────────────────

@dataclass
class PhaseReading:
    phase:       str   = ""
    volt:        float = 0.0   # Volts RMS
    amp:         float = 0.0   # Current RMS (Amps)
    watt:        float = 0.0   # Real power (Watts)
    var:         float = 0.0   # Reactive power (VARs)
    va:          float = 0.0   # Apparent power (VA)
    frequency:   float = 0.0   # Hz
    phase_angle: float = 0.0   # Degrees
    power_factor: float = 0.0  # unitless

    def display_line(self) -> str:
        return (
            f"  Phase {self.phase}  |"
            f"  V = {self.volt:9.3f} V  |"
            f"  A = {self.amp:9.4f} A  |"
            f"  W = {self.watt:10.3f} W  |"
            f"  VAR = {self.var:10.3f} VAR"
        )


# ── Parsing ────────────────────────────────────────────────────────────────────

def parse_3phase_payload(payload: bytes) -> Optional[list[PhaseReading]]:
    """
    Parse a 3-phase instant-metrics payload into PhaseReading objects.

    Expects 3 × 8 TI floats = 96 bytes minimum.
    If the device returns more floats per phase (e.g. 9 or 10), the extras
    are safely ignored.
    """
    if len(payload) < MIN_PAYLOAD_3PH:
        log.warning(
            f"Payload too short for 3-phase read: {len(payload)} bytes "
            f"(need at least {MIN_PAYLOAD_3PH}). "
            f"Got {len(payload) // BYTES_PER_FLOAT} floats."
        )
        # Attempt best-effort parse with however many floats we have
        if len(payload) < BYTES_PER_FLOAT * FLOATS_PER_PHASE:
            return None  # not even one phase worth of data

    # Decode every float in the payload
    num_floats = len(payload) // BYTES_PER_FLOAT
    floats: list[float] = []
    for i in range(num_floats):
        raw   = payload[i * BYTES_PER_FLOAT : (i + 1) * BYTES_PER_FLOAT]
        floats.append(ti_float_to_python(raw))

    log.debug(f"Decoded {num_floats} TI floats from payload")

    results: list[PhaseReading] = []
    floats_per_phase = num_floats // NUM_PHASES if num_floats >= MIN_PAYLOAD_3PH // BYTES_PER_FLOAT else FLOATS_PER_PHASE

    for idx, label in enumerate(PHASE_LABELS):
        base = idx * floats_per_phase

        def f(offset: int) -> float:
            i = base + offset
            return floats[i] if i < len(floats) else 0.0

        reading = PhaseReading(
            phase        = label,
            volt         = f(0),
            amp          = f(1),
            watt         = f(2),
            va           = f(3),
            var          = f(4),
            frequency    = f(5),
            phase_angle  = f(6),
            power_factor = f(7),
        )
        results.append(reading)

    return results


# ── Radian serial driver ────────────────────────────────────────────────────────

class RadianRD21:
    """
    Direct serial driver for the Radian RD-21 Dytronic power analyzer.
    No backend, no REST — just pyserial.
    """

    def __init__(self, port: str, baud: int = 9600, read_timeout: float = 2.0):
        self.port         = port
        self.baud         = baud
        self.read_timeout = read_timeout
        self._ser: Optional[serial.Serial] = None

    # ── connection ────────────────────────────────────────────────────────────

    def connect(self) -> bool:
        """Open the serial port and verify the Radian responds to IDENTIFY."""
        try:
            self._ser = serial.Serial(
                port     = self.port,
                baudrate = self.baud,
                bytesize = serial.EIGHTBITS,
                parity   = serial.PARITY_NONE,
                stopbits = serial.STOPBITS_ONE,
                timeout  = 0.1,   # short read timeout — we handle blocking ourselves
            )
            log.info(f"Serial port {self.port} opened @ {self.baud} baud")
        except serial.SerialException as e:
            log.error(f"Could not open {self.port}: {e}")
            return False

        # Flush any stale data
        self._ser.reset_input_buffer()
        self._ser.reset_output_buffer()

        # Send IDENTIFY and wait for response
        frame = build_frame(CMD_IDENTIFY)
        self._ser.write(frame)
        log.debug(f"IDENTIFY sent: {frame.hex().upper()}")

        payload = read_response(self._ser, timeout_s=self.read_timeout)
        if payload is None:
            log.error("Radian did not respond to IDENTIFY — check device, cable, and baud rate")
            self.disconnect()
            return False

        ident = payload.decode("ascii", errors="replace").strip()
        log.info(f"Radian identified: {ident!r}")
        return True

    def disconnect(self) -> None:
        if self._ser and self._ser.is_open:
            self._ser.close()
            log.info("Serial port closed")
        self._ser = None

    def is_connected(self) -> bool:
        return self._ser is not None and self._ser.is_open

    # ── read ──────────────────────────────────────────────────────────────────

    def read_instant_3phase(self) -> Optional[list[PhaseReading]]:
        """
        Send the instant-metrics command and return a list of 3 PhaseReading
        objects [Phase A, Phase B, Phase C].
        Returns None on communication error or parse failure.
        """
        if not self.is_connected():
            log.error("Not connected — call connect() first")
            return None

        frame = build_frame(CMD_GET_INSTANT, INSTANT_PAYLOAD)
        log.debug(f"GET_INSTANT sent: {frame.hex().upper()}")

        self._ser.reset_input_buffer()
        self._ser.write(frame)

        payload = read_response(self._ser, timeout_s=self.read_timeout)
        if payload is None:
            log.warning("No response to GET_INSTANT command")
            return None

        log.debug(f"Response payload ({len(payload)} bytes): {payload.hex().upper()}")
        return parse_3phase_payload(payload)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.disconnect()


# ── CSV writer ─────────────────────────────────────────────────────────────────

CSV_HEADER = [
    "timestamp",
    "phase_A_volts",  "phase_A_amps",  "phase_A_watts",  "phase_A_vars",
    "phase_A_va",     "phase_A_freq",  "phase_A_angle",  "phase_A_pf",
    "phase_B_volts",  "phase_B_amps",  "phase_B_watts",  "phase_B_vars",
    "phase_B_va",     "phase_B_freq",  "phase_B_angle",  "phase_B_pf",
    "phase_C_volts",  "phase_C_amps",  "phase_C_watts",  "phase_C_vars",
    "phase_C_va",     "phase_C_freq",  "phase_C_angle",  "phase_C_pf",
]


def readings_to_row(timestamp: str, phases: list[PhaseReading]) -> list:
    """Flatten 3 PhaseReading objects into a single CSV row."""
    row = [timestamp]
    for p in phases:
        row += [
            f"{p.volt:.5f}",
            f"{p.amp:.6f}",
            f"{p.watt:.5f}",
            f"{p.var:.5f}",
            f"{p.va:.5f}",
            f"{p.frequency:.4f}",
            f"{p.phase_angle:.4f}",
            f"{p.power_factor:.6f}",
        ]
    return row


def default_csv_name() -> str:
    return f"radian_rd21_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"


# ── Main polling loop ───────────────────────────────────────────────────────────

def run(port: str, baud: int, count: int, interval: float, out_path: str) -> None:
    """
    Connect to the Radian, poll `count` times (0 = unlimited), write CSV.
    """
    log.info("=" * 60)
    log.info(f"Radian RD-21  3-Phase Reader")
    log.info(f"  Port     : {port}")
    log.info(f"  Baud     : {baud}")
    log.info(f"  Readings : {'unlimited (Ctrl-C to stop)' if count == 0 else count}")
    log.info(f"  Interval : {interval} s")
    log.info(f"  CSV out  : {out_path}")
    log.info("=" * 60)

    with RadianRD21(port, baud) as radian:
        if not radian.connect():
            sys.exit(1)

        with open(out_path, "w", newline="", encoding="utf-8") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(CSV_HEADER)
            csv_file.flush()

            log.info("Connected. Starting readings — press Ctrl-C to stop early.\n")

            reading_num = 0
            errors      = 0

            try:
                while True:
                    if count > 0 and reading_num >= count:
                        break

                    phases = radian.read_instant_3phase()
                    ts     = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

                    if phases:
                        reading_num += 1
                        errors = 0   # reset consecutive error counter on success

                        # Console output
                        print(f"\n[{ts}]  Reading #{reading_num}")
                        for p in phases:
                            print(p.display_line())

                        # CSV row
                        row = readings_to_row(ts, phases)
                        writer.writerow(row)
                        csv_file.flush()   # write immediately so file is usable mid-run

                    else:
                        errors += 1
                        log.warning(f"Read failed (consecutive failures: {errors})")
                        if errors >= 5:
                            log.error("5 consecutive read failures — stopping.")
                            break

                    if count == 0 or reading_num < count:
                        time.sleep(interval)

            except KeyboardInterrupt:
                print()
                log.info("Stopped by user (Ctrl-C)")

    # Summary
    print()
    log.info(f"Done. {reading_num} reading(s) written to: {out_path}")


# ── Entry point ─────────────────────────────────────────────────────────────────

def list_ports_helper():
    ports = [p.device for p in serial.tools.list_ports.comports()]
    if ports:
        print("Available serial ports:")
        for p in ports:
            print(f"  {p}")
    else:
        print("No serial ports detected.")


def main():
    parser = argparse.ArgumentParser(
        description="Read Phase A/B/C Volts, Amps, Watts, VARs from a Radian RD-21 and save to CSV.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--port", "-p",
        help="Serial port, e.g. COM5 or /dev/ttyUSB0. Use --list-ports to see options.",
    )
    parser.add_argument(
        "--baud", "-b",
        type=int, default=9600,
        help="Baud rate.",
    )
    parser.add_argument(
        "--count", "-n",
        type=int, default=10,
        help="Number of readings to take. Use 0 for unlimited (stop with Ctrl-C).",
    )
    parser.add_argument(
        "--interval", "-i",
        type=float, default=1.0,
        help="Seconds between each reading.",
    )
    parser.add_argument(
        "--out", "-o",
        default=None,
        help="Output CSV file path. Defaults to radian_rd21_YYYYMMDD_HHMMSS.csv",
    )
    parser.add_argument(
        "--list-ports",
        action="store_true",
        help="List available serial ports and exit.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable DEBUG-level logging (shows raw hex frames).",
    )

    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    if args.list_ports:
        list_ports_helper()
        sys.exit(0)

    if not args.port:
        parser.error("--port is required. Use --list-ports to see available ports.")

    out_path = args.out or default_csv_name()

    run(
        port     = args.port,
        baud     = args.baud,
        count    = args.count,
        interval = args.interval,
        out_path = out_path,
    )


if __name__ == "__main__":
    main()