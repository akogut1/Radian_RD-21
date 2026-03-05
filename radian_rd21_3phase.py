"""
radian_rd21_3phase.py
=====================
Standalone script to communicate directly with a Radian RD-21 Dytronic
power analyzer over serial (RS-232 / USB-serial adapter).

Reads per-phase instant metrics for Phase A, B, and C every second:
    - Volts (V)
    - Current / Amps (A)
    - Watts (W)
    - VARs (VAR)

Runs continuously until the user types a command or presses Ctrl-C.
All readings are written to a timestamped CSV file.

NO backend, NO REST API, NO existing project imports required.
Only dependency beyond the standard library: pyserial

Install:
    pip install pyserial

Usage:
    python radian_rd21_3phase.py --port COM5 --baud 9600
    python radian_rd21_3phase.py --port COM5 --baud 9600 --out my_test.csv
    python radian_rd21_3phase.py --list-ports

Commands (type while readings are running, then press Enter):
    stop   / quit  — save CSV and exit
    pause          — temporarily halt readings (port stays open)
    resume         — resume after a pause
    status         — print reading count, elapsed time, and last values
    save           — manually flush/checkpoint the CSV right now
    help           — show this command list

─────────────────────────────────────────────────────────────────────────────
Radian Serial Protocol Overview
─────────────────────────────────────────────────────────────────────────────
The RD-21 uses a simple framed packet format over RS-232.

Packet structure (sent and received):
    Byte 0      : 0xA6  — sync / start-of-frame marker
    Byte 1      : Command byte
    Byte 2      : Data length low byte  (little-endian 16-bit length)
    Byte 3      : Data length high byte
    Byte 4..N-1 : Data payload (N = 4 + data_length bytes of payload)
    Byte N      : Checksum — XOR of bytes 1 through N-1

Commands used here:
    0x02  IDENTIFY    — no data, device responds with firmware/model string
    0x0D  GET_INSTANT — request instant measurement packet

The instant-metrics command payload for "all channels, all phases":
    Data = [0x00, 0x26, 0x00, 0x00, 0x00, 0x14, 0xFF, 0xFD]
    (8 bytes → length field = 0x0008)
    Full frame before checksum: A6 0D 08 00  00 26 00 00 00 14 FF FD
    Checksum = XOR(0x0D, 0x08, 0x00, 0x00, 0x26, 0x00, 0x00, 0x00, 0x14, 0xFF, 0xFD)

    Control word 0x0026 = bits 1+2+5 set:
      Bit 1 = Volts
      Bit 2 = Amps, Watts, VA, VARs
      Bit 5 = Frequency, Phase Angle, Power Factor
    (Original 0x0024 was missing bit 1, so Volts were never returned)

Response payload contains TI DSP 32-bit floats.
For a 3-phase meter the payload is 3 blocks of 8 floats (96 bytes):
    Block 0 (Phase A): volt, amp, watt, va, var, freq, phase_angle, power_factor
    Block 1 (Phase B): same order
    Block 2 (Phase C): same order

TI 32-bit float format (NOT standard IEEE-754):
    Byte 0 : exponent  (biased by 128)
    Byte 1 : sign (MSB) + mantissa bits 22..16
    Byte 2 : mantissa bits 15..8
    Byte 3 : mantissa bits 7..0
─────────────────────────────────────────────────────────────────────────────
"""

import argparse
import csv
import logging
import queue
import sys
import threading
import time
from dataclasses import dataclass
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

POLL_INTERVAL_S = 1.0   # seconds between each read from the device

HELP_TEXT = """
  Commands (press Enter after typing):
    stop  / quit   — save CSV and exit
    pause          — pause readings (port stays open)
    resume         — resume after pause
    status         — show reading count, elapsed time, last values
    save           — flush / checkpoint the CSV right now
    help           — show this list
"""


# ── Protocol constants ──────────────────────────────────────────────────────────

SYNC_BYTE        = 0xA6
CMD_IDENTIFY     = 0x02
CMD_GET_INSTANT  = 0x0D

# Payload for the "read all instant metrics" command (matches what the main GUI sends)
INSTANT_PAYLOAD  = bytes([0x00, 0x26, 0x00, 0x00, 0x00, 0x14, 0xFF, 0xFD])

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

    Same logic as ti_float_to_ieee_single in the original project's
    src/util/data_conversion.py.
    """
    if len(raw) < 4:
        return 0.0

    exp = raw[0]
    if exp == 0:
        return 0.0

    sign     = (raw[1] & 0x80) >> 7
    mantissa = ((raw[1] & 0x7F) | 0x80) << 16 | (raw[2] << 8) | raw[3]
    value    = float(mantissa) * (2.0 ** (exp - 128 - 23))
    return -value if sign else value


# ── Packet framing ─────────────────────────────────────────────────────────────

def build_frame(cmd: int, payload: bytes = b"") -> bytes:
    """
    Build a complete Radian serial frame.

    Frame layout:  [0xA6] [cmd] [len_hi] [len_lo] [payload...]
    Length is big-endian 16-bit.  NO checksum byte — the RD-21 does not use one.

    Verified against the known-working commands from the original project:
      IDENTIFY:  A6 02 00 00
      INSTANT:   A6 0D 00 08  00 26 00 00 00 14 FF FD
    """
    length_hi = (len(payload) >> 8) & 0xFF
    length_lo = len(payload) & 0xFF
    return bytes([SYNC_BYTE, cmd, length_hi, length_lo]) + payload


def read_response(ser: serial.Serial, timeout_s: float = 2.0) -> Optional[bytes]:
    """
    Read one complete response frame from the serial port.

    Frame layout:  [0xA6] [cmd_echo] [len_hi] [len_lo] [payload...]
    Big-endian 16-bit length.  No checksum byte.

    Returns the raw payload bytes, or None on timeout / framing error.
    """
    deadline = time.monotonic() + timeout_s

    # Wait for sync byte 0xA6
    while time.monotonic() < deadline:
        b = ser.read(1)
        if not b:
            continue
        if b[0] == SYNC_BYTE:
            break
    else:
        log.warning("Timeout waiting for sync byte 0xA6")
        return None

    # Read 3-byte header: [cmd_echo] [len_hi] [len_lo]
    header = _read_exact(ser, 3, deadline)
    if header is None:
        log.warning("Timeout reading response header")
        return None

    # Length is big-endian
    data_len = (header[1] << 8) | header[2]
    log.debug(f"Response: cmd_echo=0x{header[0]:02X} payload_len={data_len}")

    if data_len == 0:
        return b""

    payload = _read_exact(ser, data_len, deadline)
    if payload is None:
        log.warning(f"Timeout reading response payload ({data_len} bytes expected)")
        return None

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
    phase:        str   = ""
    volt:         float = 0.0   # Volts RMS
    amp:          float = 0.0   # Current RMS (Amps)
    watt:         float = 0.0   # Real power (Watts)
    var:          float = 0.0   # Reactive power (VARs)
    va:           float = 0.0   # Apparent power (VA)
    frequency:    float = 0.0   # Hz
    phase_angle:  float = 0.0   # Degrees
    power_factor: float = 0.0   # unitless

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


# ── Background input thread ─────────────────────────────────────────────────────

def _input_thread(cmd_queue: queue.Queue) -> None:
    """
    Runs in a daemon thread. Blocks on input() waiting for the user to type
    a command, then puts the lowercased string onto cmd_queue.
    The poll loop drains this queue every second without ever blocking on input.
    """
    while True:
        try:
            line = input()          # blocks here until user presses Enter
            cmd_queue.put(line.strip().lower())
        except EOFError:
            # stdin closed (e.g. piped input finished) — treat as stop
            cmd_queue.put("stop")
            break


# ── Command handler ─────────────────────────────────────────────────────────────

def handle_command(
    cmd: str,
    *,
    paused: list,                     # mutable single-element list used as a flag
    reading_num: list,
    start_time: float,
    last_phases: list,
    csv_file,
) -> bool:
    """
    Process one user command. Returns True if the session should stop.

    Uses mutable lists as simple pass-by-reference flags so this function
    can update state that lives in run().
    """
    if cmd in ("stop", "quit", "q", "exit"):
        print("\n[CMD] Stopping — saving CSV and disconnecting...")
        return True  # signal run() to exit the loop

    elif cmd == "pause":
        if paused[0]:
            print("[CMD] Already paused. Type 'resume' to continue.")
        else:
            paused[0] = True
            print("[CMD] Paused. Readings halted. Type 'resume' to continue.")

    elif cmd == "resume":
        if not paused[0]:
            print("[CMD] Not currently paused.")
        else:
            paused[0] = False
            print("[CMD] Resumed. Readings continuing every second.")

    elif cmd == "status":
        elapsed = time.monotonic() - start_time
        mins, secs = divmod(int(elapsed), 60)
        hrs,  mins = divmod(mins, 60)
        print(f"\n[STATUS] ── {datetime.now().strftime('%H:%M:%S')} ─────────────────")
        print(f"  Readings recorded : {reading_num[0]}")
        print(f"  Elapsed time      : {hrs:02d}:{mins:02d}:{secs:02d}")
        print(f"  State             : {'PAUSED' if paused[0] else 'RUNNING'}")
        if last_phases[0]:
            print("  Last values:")
            for p in last_phases[0]:
                print(f"  {p.display_line()}")
        else:
            print("  Last values       : none yet")
        print()

    elif cmd == "save":
        if csv_file:
            csv_file.flush()
            print(f"[CMD] CSV flushed — {reading_num[0]} row(s) saved to disk.")
        else:
            print("[CMD] No CSV file open.")

    elif cmd in ("help", "h", "?"):
        print(HELP_TEXT)

    elif cmd == "":
        pass  # user just pressed Enter — ignore silently

    else:
        print(f"[CMD] Unknown command: '{cmd}'.  Type 'help' for a list.")

    return False  # keep running


# ── Main polling loop ───────────────────────────────────────────────────────────

def run(port: str, baud: int, out_path: str) -> None:
    """
    Connect to the Radian, poll every second, write CSV.
    Runs until the user types 'stop'/'quit' or presses Ctrl-C.
    """
    log.info("=" * 60)
    log.info("Radian RD-21  3-Phase Continuous Reader")
    log.info(f"  Port    : {port}")
    log.info(f"  Baud    : {baud}")
    log.info(f"  CSV out : {out_path}")
    log.info("=" * 60)

    with RadianRD21(port, baud) as radian:
        if not radian.connect():
            sys.exit(1)

        with open(out_path, "w", newline="", encoding="utf-8") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(CSV_HEADER)
            csv_file.flush()

            print(HELP_TEXT)
            log.info("Connected. Polling every second — type a command and press Enter.\n")

            # Shared state (mutable containers so handle_command can modify them)
            reading_num  = [0]       # [int]
            paused       = [False]   # [bool]
            last_phases  = [None]    # [Optional[list[PhaseReading]]]
            consec_errs  = 0
            start_time   = time.monotonic()

            # Start the background input thread — it's a daemon so it dies
            # automatically when the main thread exits.
            cmd_queue: queue.Queue = queue.Queue()
            t = threading.Thread(target=_input_thread, args=(cmd_queue,), daemon=True)
            t.start()

            try:
                while True:
                    # ── drain any pending commands first ──────────────────────
                    stop_requested = False
                    while not cmd_queue.empty():
                        cmd = cmd_queue.get_nowait()
                        stop_requested = handle_command(
                            cmd,
                            paused=paused,
                            reading_num=reading_num,
                            start_time=start_time,
                            last_phases=last_phases,
                            csv_file=csv_file,
                        )
                        if stop_requested:
                            break
                    if stop_requested:
                        break

                    # ── skip read if paused ───────────────────────────────────
                    if paused[0]:
                        time.sleep(POLL_INTERVAL_S)
                        continue

                    # ── poll the device ───────────────────────────────────────
                    phases = radian.read_instant_3phase()
                    ts     = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

                    if phases:
                        reading_num[0] += 1
                        consec_errs     = 0
                        last_phases[0]  = phases

                        # Console
                        print(f"\n[{ts}]  Reading #{reading_num[0]}")
                        for p in phases:
                            print(p.display_line())

                        # CSV — flushed immediately so file is usable mid-run
                        writer.writerow(readings_to_row(ts, phases))
                        csv_file.flush()

                    else:
                        consec_errs += 1
                        log.warning(f"Read failed (consecutive failures: {consec_errs})")
                        if consec_errs >= 5:
                            log.error("5 consecutive read failures — stopping.")
                            break

                    # ── wait 1 second before next poll ───────────────────────
                    # Sleep in small increments so a 'stop' command is noticed
                    # within ~0.1 s rather than waiting out the full second.
                    next_poll = time.monotonic() + POLL_INTERVAL_S
                    while time.monotonic() < next_poll:
                        time.sleep(0.05)
                        # Peek at the queue mid-sleep so stop responds instantly
                        if not cmd_queue.empty():
                            peek = cmd_queue.queue[0]
                            if peek in ("stop", "quit", "q", "exit"):
                                break

            except KeyboardInterrupt:
                print()
                log.info("Stopped by Ctrl-C")

    # ── summary ───────────────────────────────────────────────────────────────
    elapsed = time.monotonic() - start_time
    mins, secs = divmod(int(elapsed), 60)
    hrs,  mins = divmod(mins, 60)
    print()
    log.info(f"Session complete.")
    log.info(f"  Readings written : {reading_num[0]}")
    log.info(f"  Elapsed time     : {hrs:02d}:{mins:02d}:{secs:02d}")
    log.info(f"  CSV saved to     : {out_path}")


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
        description=(
            "Continuously read Phase A/B/C Volts, Amps, Watts, VARs from a Radian RD-21 "
            "every second and save to CSV. Type commands while running (see --help)."
        ),
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
        out_path = out_path,
    )


if __name__ == "__main__":
    main()