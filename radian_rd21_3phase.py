"""
radian_rd21_3phase.py
=====================

Radian RD-21 / Dytronic serial driver.

This version fixes your issue by using the VB-style bulk read:
  [0Dh] Read Instantaneous Metrics (RD-3x ALL)

Why:
- Your per-metric [2Eh] reads can return 0.0 / nonsense for some indices (VOLTS/VAR),
  while [0Dh] returns the whole RD-3x instantaneous metric table reliably.
- We then auto-detect which metric index corresponds to:
    VOLTS, AMPS, WATTS, VA, VAR, FREQ, ANGLE, PF
  by matching your live conditions (~240V, 30A, 60Hz, ~7.2kW).

Frame format:
  Start(1) + PacketType(1) + Length(2, big-endian) + Data(N) + Checksum(2, big-endian)

Checksum:
  16-bit SUM of all bytes from Start through last Data byte (mod 0x10000), stored big-endian.

[0Dh] Read Instantaneous Metrics (RD-3x ALL):
  Request (data words):
    Control(0x0040) + StartIndex(0x0000) + Count(0x0014) + Terminator(0xFFFD)

  Example TX (without checksum shown here):
    A6 0D 00 08 00 40 00 00 00 14 FF FD  ....

Response:
  Typically returns Count metrics, each metric is 5 TI-C3x floats (20 bytes):
    metric_i: slot0 slot1 slot2 slot3 slot4
  Total payload len often = Count * 20 bytes (e.g., 0x14 * 20 = 400 bytes)

Slots are typically:
  slot0=PhaseA, slot1=PhaseB, slot2=PhaseC, slot3=Reserved, slot4=Net
Single-phase quirk:
  sometimes live value is in slot3 or slot4; we remap heuristically.

Also includes [2Eh] Read Instantaneous Metric (single index) as fallback.
"""

import logging
import os as _os
import struct
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import serial

log = logging.getLogger(__name__)

_DEBUG_FRAMES = _os.environ.get("RADIAN_DEBUG", "0") == "1"
if _DEBUG_FRAMES:
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s  %(levelname)-5s  %(message)s",
        datefmt="%H:%M:%S",
    )
    log.info("RADIAN_DEBUG enabled — raw frame hex will be printed")


def _hex(b: bytes) -> str:
    return " ".join(f"{x:02X}" for x in b)


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


# IEEE single-precision FLT_MAX
_FLT_MAX = 3.4028234663852886e38


def ti_c3x_float(be4: bytes) -> float:
    """
    Convert RD / TMS320C3x 32-bit float bytes -> Python float.

    On the wire it is BIG-ENDIAN:
      b0 = exponent (signed 8-bit, two's complement). 0x80 (-128) represents 0.
      b1 = sign(bit7) + mantissa[22:16] (7 bits)
      b2 = mantissa[15:8]
      b3 = mantissa[7:0]
    """
    if len(be4) != 4:
        raise ValueError("TI float requires exactly 4 bytes")

    b0, b1, b2, b3 = be4[0], be4[1], be4[2], be4[3]

    if (b0, b1, b2, b3) == (0x00, 0x00, 0x00, 0x00):
        return 0.0

    if b0 in (0x80, 0x81):
        return 0.0

    if (b0, b1, b2, b3) == (0x7F, 0x7F, 0xFF, 0xFF):
        return _FLT_MAX

    mantissa = ((b1 & 0x7F) << 16) | (b2 << 8) | b3
    neg = (b1 & 0x80) != 0
    exp = b0 if b0 < 0x80 else b0 - 0x100

    if exp == 0 and mantissa == 0 and not neg:
        return 0.0

    if (b0 == 0x7F) and neg and (mantissa == 0):
        return -_FLT_MAX

    if not neg:
        bits = (((exp + 0x7F) << 23) | mantissa) & 0x7FFFFFFF
    elif mantissa != 0:
        twos = ((~mantissa) + 1) & 0x7FFFFF
        bits = twos | 0x80000000 | ((exp + 0x7F) << 23)
    else:
        bits = 0x80000000 | ((exp + 0x80) << 23)

    return struct.unpack("<f", struct.pack("<I", bits & 0xFFFFFFFF))[0]


class RadianRD21:
    # Packet types
    CMD_IDENTIFY = 0x02
    CMD_INST_METRIC_RD3X = 0x2E
    CMD_INST_METRICS_BLOCK_RD3X = 0x0D

    # Start byte families
    _START_HINIBBLES = (0xA0, 0xB0, 0xC0, 0xD0)
    _START_LONIBBLES = (0x3, 0x6, 0x9, 0xC)

    def __init__(self, port: str, baud: int = 57600):
        self.port = port
        self.baud = baud
        self._ser: Optional[serial.Serial] = None

        # Auto map discovered at runtime
        self.metric_map: Optional[Dict[str, int]] = None

    # ── Connection ──────────────────────────────────────────────────────────────

    def connect(self) -> bool:
        try:
            self._ser = serial.Serial(
                port=self.port,
                baudrate=self.baud,
                bytesize=8,
                parity="N",
                stopbits=1,
                timeout=1.0,
                write_timeout=1.0,
            )
            try:
                self._ser.dtr = True
                self._ser.rts = True
            except Exception:
                pass

            try:
                self._ser.reset_input_buffer()
                self._ser.reset_output_buffer()
            except Exception:
                pass

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
    def _sum16(data: bytes) -> int:
        return sum(data) & 0xFFFF

    @classmethod
    def _is_start_byte(cls, v: int) -> bool:
        return (v & 0xF0) in cls._START_HINIBBLES and (v & 0x0F) in cls._START_LONIBBLES

    def _build_frame(self, start: int, packet_type: int, data: bytes) -> bytes:
        n = len(data)
        hdr = bytes([start, packet_type, (n >> 8) & 0xFF, n & 0xFF])
        body = hdr + data
        cs = self._sum16(body)
        return body + bytes([(cs >> 8) & 0xFF, cs & 0xFF])

    def _read_exact(self, n: int) -> bytes:
        chunks = []
        got = 0
        while got < n:
            b = self._ser.read(n - got)
            if not b:
                break
            chunks.append(b)
            got += len(b)
        return b"".join(chunks)

    def _read_frame(self) -> Optional[Tuple[int, int, bytes]]:
        if not self._ser:
            return None

        start = None
        for _ in range(4096):
            b = self._ser.read(1)
            if not b:
                return None
            v = b[0]
            if self._is_start_byte(v):
                start = v
                break
        if start is None:
            return None

        hdr = self._read_exact(3)  # type + len_hi + len_lo
        if len(hdr) != 3:
            return None

        packet_type = hdr[0]
        length = (hdr[1] << 8) | hdr[2]

        payload = self._read_exact(length)
        if len(payload) != length:
            return None

        cs_bytes = self._read_exact(2)
        if len(cs_bytes) != 2:
            return None
        cs_rx = (cs_bytes[0] << 8) | cs_bytes[1]

        body = bytes([start]) + hdr + payload
        cs_calc = self._sum16(body)
        if cs_calc != cs_rx:
            if _DEBUG_FRAMES:
                log.debug("Checksum mismatch: calc=%04X rx=%04X body=%s", cs_calc, cs_rx, _hex(body))
            return None

        return start, packet_type, payload

    # ── Slot mapping ────────────────────────────────────────────────────────────

    @staticmethod
    def _map_slots_to_phases(slots: List[float]) -> Tuple[float, float, float]:
        s0, s1, s2, s3, s4 = slots

        def nz(x: float) -> bool:
            return abs(x) > 1e-9

        if nz(s0) or nz(s1) or nz(s2):
            return s0, s1, s2

        if nz(s3) and not nz(s4):
            return s3, 0.0, 0.0

        if nz(s4) and not nz(s3):
            return s4, 0.0, 0.0

        if nz(s3) and nz(s4):
            return s3, 0.0, 0.0

        return 0.0, 0.0, 0.0

    # ── [2Eh] single-index read (fallback) ─────────────────────────────────────

    def _read_inst_metric_2e(self, metric_index: int) -> Optional[Tuple[float, float, float]]:
        if not self.is_connected():
            return None

        data = bytes([(metric_index >> 8) & 0xFF, metric_index & 0xFF]) + b"\xFF\xFD"
        frame = self._build_frame(0xA6, self.CMD_INST_METRIC_RD3X, data)

        try:
            self._ser.reset_input_buffer()
            self._ser.write(frame)
            self._ser.flush()
        except serial.SerialException as exc:
            log.error("Write error: %s", exc)
            return None

        if _DEBUG_FRAMES:
            log.debug("TX 2E idx=%d: %s", metric_index, _hex(frame))

        resp = self._read_frame()
        if resp is None:
            return None

        start, ptype, payload = resp

        if _DEBUG_FRAMES:
            log.debug("RX 2E idx=%d start=%02X type=%02X len=%d payload=%s", metric_index, start, ptype, len(payload), _hex(payload))

        if (start & 0x0F) == 0x9:
            if len(payload) >= 2:
                err = (payload[0] << 8) | payload[1]
                log.warning("Device error ACK for 2E idx=%d: code=0x%04X", metric_index, err)
            return None

        if ptype != self.CMD_INST_METRIC_RD3X:
            return None

        if len(payload) != 0x14:
            return None

        slots = [ti_c3x_float(payload[i:i + 4]) for i in range(0, 20, 4)]
        return self._map_slots_to_phases(slots)

    # ── [0Dh] bulk read (preferred) ────────────────────────────────────────────

    def _read_inst_block_0d(self, control: int = 0x0040, start_index: int = 0x0000, count: int = 0x0014) -> Optional[List[List[float]]]:
        """
        Returns metrics as a list:
          metrics[i] = [slot0, slot1, slot2, slot3, slot4]  (TI floats decoded)
        where i corresponds to metric index (start_index + i + 1) in the RD-3x table.

        If the device returns an error ACK (A9), returns None.
        """
        if not self.is_connected():
            return None

        data = struct.pack(">HHHH", control, start_index, count, 0xFFFD)  # 8 bytes
        frame = self._build_frame(0xA6, self.CMD_INST_METRICS_BLOCK_RD3X, data)

        try:
            self._ser.reset_input_buffer()
            self._ser.write(frame)
            self._ser.flush()
        except serial.SerialException as exc:
            log.error("Write error: %s", exc)
            return None

        if _DEBUG_FRAMES:
            log.debug("TX 0D: %s", _hex(frame))

        resp = self._read_frame()
        if resp is None:
            return None

        start, ptype, payload = resp

        if _DEBUG_FRAMES:
            log.debug("RX 0D start=%02X type=%02X len=%d", start, ptype, len(payload))
            if len(payload) <= 64:
                log.debug("RX 0D payload: %s", _hex(payload))

        # error ACK
        if (start & 0x0F) == 0x9:
            if len(payload) >= 2:
                err = (payload[0] << 8) | payload[1]
                log.warning("Device returned error ACK for 0D: code=0x%04X", err)
            return None

        if ptype != self.CMD_INST_METRICS_BLOCK_RD3X:
            return None

        # Most common format: count metrics * 20 bytes
        expected = count * 20
        if len(payload) < expected:
            # Some firmwares may return fewer; fail safely.
            if _DEBUG_FRAMES:
                log.debug("0D payload too short: got %d expected %d", len(payload), expected)
            return None

        metrics: List[List[float]] = []
        off = 0
        for _ in range(count):
            chunk = payload[off:off + 20]
            slots = [ti_c3x_float(chunk[i:i + 4]) for i in range(0, 20, 4)]
            metrics.append(slots)
            off += 20

        return metrics

    # ── Auto-detection of metric indices ───────────────────────────────────────

    @staticmethod
    def _pick_best(candidates: Dict[int, float], predicate, target: float, name: str) -> Optional[int]:
        best_idx = None
        best_err = None
        for idx, val in candidates.items():
            if not predicate(val):
                continue
            err = abs(val - target)
            if best_err is None or err < best_err:
                best_err = err
                best_idx = idx
        return best_idx

    def autodetect_metric_map(self,
                              expected_volts: float = 241.0,
                              expected_amps: float = 30.0,
                              expected_freq: float = 60.0,
                              expected_watts: float = 7200.0) -> Optional[Dict[str, int]]:
        """
        Build a map from label->metric_index using [0Dh] bulk read.
        Returns None if bulk read is unsupported.
        """
        metrics = self._read_inst_block_0d()
        if metrics is None:
            return None

        # Metric indices are 1-based (Table), we requested start_index=0, so:
        # metrics[0] == index 1, metrics[1] == index 2, ...
        phaseA_vals: Dict[int, float] = {}
        for i, slots in enumerate(metrics):
            a, _, _ = self._map_slots_to_phases(slots)
            phaseA_vals[i + 1] = a

        if _DEBUG_FRAMES:
            log.debug("autodetect probe (Phase-A): %s", phaseA_vals)

        # Identify the obvious ones
        freq_idx = self._pick_best(
            phaseA_vals,
            predicate=lambda v: 45.0 < v < 70.0,
            target=expected_freq,
            name="FREQ",
        )
        amps_idx = self._pick_best(
            phaseA_vals,
            predicate=lambda v: 0.5 < v < 200.0,
            target=expected_amps,
            name="AMPS",
        )
        volts_idx = self._pick_best(
            phaseA_vals,
            predicate=lambda v: 50.0 < abs(v) < 600.0,
            target=expected_volts,
            name="VOLTS",
        )
        watts_idx = self._pick_best(
            phaseA_vals,
            predicate=lambda v: 100.0 < abs(v) < 1_000_000.0,
            target=expected_watts,
            name="WATTS",
        )

        # PF near 1.0
        pf_idx = self._pick_best(
            phaseA_vals,
            predicate=lambda v: 0.0 < v <= 1.5,
            target=1.0,
            name="PF",
        )

        # Angle is usually small-ish degrees
        angle_idx = self._pick_best(
            phaseA_vals,
            predicate=lambda v: -180.0 <= v <= 180.0,
            target=0.0,
            name="ANGLE",
        )

        # VA tends to be near watts, slightly above
        va_idx = None
        if watts_idx is not None:
            wv = phaseA_vals[watts_idx]
            va_idx = self._pick_best(
                phaseA_vals,
                predicate=lambda v: 100.0 < abs(v) < 1_000_000.0,
                target=abs(wv) * 1.01,
                name="VA",
            )

        # VAR is what's left with magnitude reasonable (not freq/amps/volts/watts/va/pf/angle)
        used = {x for x in [freq_idx, amps_idx, volts_idx, watts_idx, va_idx, pf_idx, angle_idx] if x}
        var_idx = None
        var_candidates = {k: v for k, v in phaseA_vals.items() if k not in used}
        if var_candidates:
            # Prefer magnitude smaller than VA, and not insane sentinels
            def ok(v: float) -> bool:
                if abs(v) >= 1e20:
                    return False
                return abs(v) < 500_000.0

            # If power factor is near 1, VAR should be relatively small compared to VA.
            target_var = 0.0
            var_idx = self._pick_best(
                var_candidates,
                predicate=ok,
                target=target_var,
                name="VAR",
            )

        # Sanity: must find the key ones
        if None in (freq_idx, amps_idx, volts_idx, watts_idx):
            return None

        m = {
            "VOLTS": volts_idx,
            "AMPS": amps_idx,
            "WATTS": watts_idx,
            "VA": va_idx if va_idx is not None else 0,
            "VAR": var_idx if var_idx is not None else 0,
            "FREQ": freq_idx,
            "ANGLE": angle_idx if angle_idx is not None else 0,
            "PF": pf_idx if pf_idx is not None else 0,
        }

        # Remove zeros (unknown)
        m = {k: v for k, v in m.items() if v != 0}

        self.metric_map = m
        return m

    # ── Public API ──────────────────────────────────────────────────────────────

    def read_instant_3phase(self) -> Optional[List[PhaseReading]]:
        """
        Preferred:
          - if metric_map not known: autodetect via [0Dh]
          - read via [0Dh] again and pull indices
        Fallback:
          - use [2Eh] per-index reads (less reliable on your unit)
        """
        if not self.is_connected():
            return None

        # Try to build map with bulk read first
        if self.metric_map is None:
            m = self.autodetect_metric_map()
            if m is not None:
                log.info("RD-21 metric index map (auto): %s", m)

        # If we have a bulk-map, read bulk and extract
        if self.metric_map is not None:
            metrics = self._read_inst_block_0d()
            if metrics is None:
                # bulk read stopped working; fall back
                self.metric_map = None
            else:
                def get_metric(idx_1based: int) -> Tuple[float, float, float]:
                    slots = metrics[idx_1based - 1]
                    return self._map_slots_to_phases(slots)

                try:
                    v = get_metric(self.metric_map["VOLTS"])
                    a = get_metric(self.metric_map["AMPS"])
                    w = get_metric(self.metric_map["WATTS"])
                    va = get_metric(self.metric_map.get("VA", self.metric_map["WATTS"]))
                    var = get_metric(self.metric_map.get("VAR", self.metric_map["WATTS"]))
                    f = get_metric(self.metric_map["FREQ"])
                    ang = get_metric(self.metric_map.get("ANGLE", self.metric_map["FREQ"]))
                    pf = get_metric(self.metric_map.get("PF", self.metric_map["FREQ"]))
                except Exception:
                    return None

                phases: List[PhaseReading] = []
                for i, label in enumerate(("A", "B", "C")):
                    phases.append(PhaseReading(
                        phase=label,
                        volt=v[i],
                        amp=a[i],
                        watt=w[i],
                        va=va[i],
                        var=var[i],
                        frequency=f[i],
                        phase_angle=ang[i],
                        power_factor=pf[i],
                    ))
                return phases

        # Fallback: [2Eh] reads
        # NOTE: these are the ones that give you 0.0 volts on your unit sometimes.
        v = self._read_inst_metric_2e(1)
        a = self._read_inst_metric_2e(2)
        w = self._read_inst_metric_2e(3)
        va = self._read_inst_metric_2e(4)
        var = self._read_inst_metric_2e(5)
        f = self._read_inst_metric_2e(6)
        ang = self._read_inst_metric_2e(7)
        pf = self._read_inst_metric_2e(8)

        if not all([v, a, w, va, var, f, ang, pf]):
            return None

        phases: List[PhaseReading] = []
        for i, label in enumerate(("A", "B", "C")):
            phases.append(PhaseReading(
                phase=label,
                volt=v[i],
                amp=a[i],
                watt=w[i],
                va=va[i],
                var=var[i],
                frequency=f[i],
                phase_angle=ang[i],
                power_factor=pf[i],
            ))
        return phases