"""
radian_rd21_3phase.py
=====================
Serial driver and data types for the Radian RD-21 3-phase power analyser.

Protocol reference
------------------
Frame format (request and response):
  SYNC(0xA6)  CMD  LEN_LOW  LEN_HIGH  DATA...  CHECKSUM
  CHECKSUM = XOR of all bytes from CMD through the last DATA byte.

Commands
  0x02  IDENTIFY    – firmware / model string
  0x0D  GET_INSTANT – instant-metrics snapshot

GET_INSTANT request payload (8 bytes):
  00 24 00 00 00 14 FF FD

Response payload: 3 × 8 × 4-byte TI DSP floats = 96 bytes.
Phase order: A, B, C.  Field order per phase:
  volt, amp, watt, va, var, frequency, phase_angle, power_factor

TI 32-bit float (NOT IEEE-754):
  Byte 0: exponent (biased by 128)
  Byte 1: sign[7] + mantissa[22:16]
  Byte 2: mantissa[15:8]
  Byte 3: mantissa[7:0]
  value = mantissa_signed × 2^(exponent − 151)
  where mantissa_signed is the 24-bit 2's-complement value
  (implicit leading 1 for positive numbers).

Dependencies: pyserial
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

import serial

log = logging.getLogger(__name__)

POLL_INTERVAL_S: float = 1.0

CSV_HEADER = [
    "timestamp",
    "phase_A_volt", "phase_A_amp", "phase_A_watt", "phase_A_va", "phase_A_var",
    "phase_A_freq", "phase_A_angle", "phase_A_pf",
    "phase_B_volt", "phase_B_amp", "phase_B_watt", "phase_B_va", "phase_B_var",
    "phase_B_freq", "phase_B_angle", "phase_B_pf",
    "phase_C_volt", "phase_C_amp", "phase_C_watt", "phase_C_va", "phase_C_var",
    "phase_C_freq", "phase_C_angle", "phase_C_pf",
]


@dataclass
class PhaseReading:
    phase: str
    volt: float
    amp: float
    watt: float
    va: float
    var: float
    frequency: float
    phase_angle: float
    power_factor: float


def default_csv_name() -> str:
    return datetime.now().strftime("radian_rd21_%Y%m%d_%H%M%S.csv")


def readings_to_row(timestamp: str, phases: List[PhaseReading]) -> list:
    row = [timestamp]
    for p in phases:
        row.extend([
            p.volt, p.amp, p.watt, p.va, p.var,
            p.frequency, p.phase_angle, p.power_factor,
        ])
    return row


def _ti_float(b: bytes) -> float:
    """Decode one TI DSP 32-bit float to a Python float.

    The 24-bit mantissa (sign + 23 fraction bits) is stored in 2's-complement.
    For positive numbers the implicit leading 1 is added before the fraction.
    value = mantissa_signed × 2^(exponent − 151)
    """
    exp  = b[0]
    sign = (b[1] >> 7) & 1
    frac = ((b[1] & 0x7F) << 16) | (b[2] << 8) | b[3]

    if exp == 0 and frac == 0:
        return 0.0

    if sign:
        mantissa = frac - (1 << 23)   # 2's-complement negative
    else:
        mantissa = (1 << 23) | frac   # add implicit leading 1

    return float(mantissa) * (2.0 ** (exp - 151))


class RadianRD21:
    SYNC             = 0xA6
    CMD_IDENTIFY     = 0x02
    CMD_GET_INSTANT  = 0x0D

    _INSTANT_PAYLOAD  = bytes([0x00, 0x24, 0x00, 0x00, 0x00, 0x14, 0xFF, 0xFD])
    _PHASES           = ("A", "B", "C")
    _FLOATS_PER_PHASE = 8
    _PAYLOAD_BYTES    = len(_PHASES) * _FLOATS_PER_PHASE * 4  # 96

    def __init__(self, port: str, baud: int = 9600):
        self.port = port
        self.baud = baud
        self._ser: Optional[serial.Serial] = None

    # ── Connection ──────────────────────────────────────────────────────────────

    def connect(self) -> bool:
        try:
            self._ser = serial.Serial(
                port=self.port, baudrate=self.baud,
                bytesize=8, parity="N", stopbits=1, timeout=2.0,
            )
            log.info("Opened %s @ %d baud", self.port, self.baud)
            return True
        except serial.SerialException as exc:
            log.error("connect failed: %s", exc)
            self._ser = None
            return False

    def disconnect(self):
        if self._ser and self._ser.is_open:
            self._ser.close()
            log.info("Port closed")
        self._ser = None

    def is_connected(self) -> bool:
        return self._ser is not None and self._ser.is_open

    # ── Frame I/O ───────────────────────────────────────────────────────────────

    @staticmethod
    def _xor(data: bytes) -> int:
        cs = 0
        for b in data:
            cs ^= b
        return cs

    def _build_frame(self, cmd: int, payload: bytes) -> bytes:
        n     = len(payload)
        inner = bytes([cmd, n & 0xFF, (n >> 8) & 0xFF]) + payload
        return bytes([self.SYNC]) + inner + bytes([self._xor(inner)])

    def _read_frame(self) -> Optional[bytes]:
        """Read one response frame and return the payload, or None on error."""
        # Scan for sync byte (skip stale/garbage bytes)
        for _ in range(256):
            b = self._ser.read(1)
            if not b:
                log.debug("Timeout waiting for sync byte")
                return None
            if b[0] == self.SYNC:
                break
        else:
            log.debug("No sync byte found in 256 bytes")
            return None

        # Read CMD + LEN_LOW + LEN_HIGH
        hdr = self._ser.read(3)
        if len(hdr) < 3:
            log.debug("Short header: %d byte(s)", len(hdr))
            return None
        cmd, len_lo, len_hi = hdr
        length = len_lo | (len_hi << 8)

        # Read payload + checksum byte
        rest = self._ser.read(length + 1)
        if len(rest) < length + 1:
            log.debug("Short payload: got %d, expected %d", len(rest), length + 1)
            return None

        payload, checksum = rest[:length], rest[length]

        # Verify checksum (XOR of CMD, LEN_LOW, LEN_HIGH, DATA…)
        if self._xor(bytes(hdr) + payload) != checksum:
            log.debug("Checksum mismatch")
            return None

        log.debug("CMD=0x%02X len=%d  %s", cmd, length, payload.hex())
        return payload

    # ── Public API ──────────────────────────────────────────────────────────────

    def read_instant_3phase(self) -> Optional[List[PhaseReading]]:
        """Request an instant snapshot and return three PhaseReading objects."""
        if not self.is_connected():
            return None
        try:
            self._ser.reset_input_buffer()
            self._ser.write(self._build_frame(self.CMD_GET_INSTANT, self._INSTANT_PAYLOAD))
            self._ser.flush()
        except serial.SerialException as exc:
            log.error("Write error: %s", exc)
            return None

        payload = self._read_frame()
        if payload is None:
            return None
        if len(payload) < self._PAYLOAD_BYTES:
            log.error("Payload too short: %d < %d", len(payload), self._PAYLOAD_BYTES)
            return None

        readings: List[PhaseReading] = []
        for i, label in enumerate(self._PHASES):
            base = i * self._FLOATS_PER_PHASE * 4
            vals = [
                _ti_float(payload[base + j * 4: base + j * 4 + 4])
                for j in range(self._FLOATS_PER_PHASE)
            ]
            readings.append(PhaseReading(
                phase=label,
                volt=vals[0], amp=vals[1], watt=vals[2], va=vals[3],
                var=vals[4], frequency=vals[5],
                phase_angle=vals[6], power_factor=vals[7],
            ))
        return readings
