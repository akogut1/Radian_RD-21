"""
radian_rd21_3phase.py
=====================
Radian RD-21 / Dytronic serial driver (single-phase + 3-phase capable).
Exports for radian_rd21.py:
 - PhaseReading
 - RadianRD21
 - default_csv_name()
 - readings_to_row()
 - CSV_HEADER
 - POLL_INTERVAL_S
Key:
- Start byte: high nibble A/B/C/D channel, low nibble 3/6/9/C ack-type.
- Frame: Start + Type + Len(2 BE) + Payload + Checksum(2 BE)
- Checksum: SUM16 of Start..last payload byte (mod 65536), stored BE.
- [2Eh] Read Instantaneous Metrics response payload is 5 TI C3x floats (20 bytes):
   Slot0 Slot1 Slot2 Slot3(reserved) Slot4(net)
Single-phase quirk:
- Some units put the live value in Slot3 while Slot0..2 are 0. We remap Slot3 -> Phase A.
Auto index detection:
- We probe indices 1..8 and assign them to VOLTS/AMPS/WATTS/VA/VAR/FREQ/ANGLE/PF
 using *relationships*:
   V_est = W / (A * PF)
   VAR_est = VA * sqrt(1 - PF^2)
- We ignore FLT_MAX sentinel values as invalid candidates.
"""
import logging
import os as _os
import struct
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import serial
log = logging.getLogger("radian_rd21")
_DEBUG_FRAMES = _os.environ.get("RADIAN_DEBUG", "0") == "1"
if _DEBUG_FRAMES:
   logging.basicConfig(
       level=logging.DEBUG,
       format="%(asctime)s  %(levelname)-5s  %(message)s",
       datefmt="%H:%M:%S",
   )
   log.info("RADIAN_DEBUG enabled — raw frame hex will be printed")
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
           round(p.volt,         3),
           round(p.amp,          4),
           round(p.watt,         3),
           round(p.va,           2),
           round(p.var,          3),
           round(p.frequency,    3),
           round(p.phase_angle,  2),
           round(p.power_factor, 4),
       ])
   return row

def _hex(b: bytes) -> str:
   return " ".join(f"{x:02X}" for x in b)

_FLT_MAX = 3.4028234663852886e38
_FLT_MAX_THRESH = 1e37  # anything this large is effectively "sentinel/invalid"

def ti_c3x_float(be4: bytes) -> float:
   """
   Convert RD / TMS320C3x 32-bit float bytes -> Python float.
   Wire format (BIG-ENDIAN):
     b0 = exponent (signed 8-bit). 0x80(-128) is zero.
     b1 = sign(bit7) + mantissa[22:16](7 bits)
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
   CMD_IDENTIFY = 0x02
   CMD_INST_METRIC_RD3X = 0x2E
   _START_HINIBBLES = (0xA0, 0xB0, 0xC0, 0xD0)
   _START_LONIBBLES = (0x3, 0x6, 0x9, 0xC)
   TAIL_FFFD = b"\xFF\xFD"
   def __init__(self, port: str, baud: int = 57600):
       self.port = port
       self.baud = baud
       self._ser: Optional[serial.Serial] = None
       # Will be overwritten by autodetect_indices()
       self._idx: Dict[str, int] = {
           "VOLTS": 1,
           "AMPS": 2,
           "WATTS": 3,
           "VA": 4,
           "VAR": 5,
           "FREQ": 6,
           "ANGLE": 7,
           "PF": 8,
       }
       self._idx_locked: bool = False
   # ── Connection ──────────────────────────────────────────────────────────
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
           # Detect mapping once at connect
           self.autodetect_indices()
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
   # ── Frame I/O ───────────────────────────────────────────────────────────
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
   def _read_frame(self, expect_types: Optional[Tuple[int, ...]] = None) -> Optional[Tuple[int, int, bytes]]:
       if not self._ser:
           return None
       for _ in range(4096):
           b = self._ser.read(1)
           if not b:
               return None
           start = b[0]
           if not self._is_start_byte(start):
               continue
           hdr = self._read_exact(3)
           if len(hdr) != 3:
               return None
           ptype = hdr[0]
           length = (hdr[1] << 8) | hdr[2]
           if length > 512:
               continue
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
               continue
           if expect_types is not None and ptype not in expect_types:
               continue
           return start, ptype, payload
       return None
   # ── Slot mapping ─────────────────────────────────────────────────────────
   @staticmethod
   def _map_slots_to_phases(slots: List[float]) -> Tuple[float, float, float]:
       s0, s1, s2, s3, s4 = slots
       def nz(x: float) -> bool:
           return abs(x) > 1e-9 and abs(x) < _FLT_MAX_THRESH
       if nz(s0) or nz(s1) or nz(s2):
           return s0, s1, s2
       if nz(s3) and not nz(s4):
           return s3, 0.0, 0.0
       if nz(s4) and not nz(s3):
           return s4, 0.0, 0.0
       if nz(s3) and nz(s4):
           return s3, 0.0, 0.0
       return 0.0, 0.0, 0.0
   def _read_inst_metric_slots(self, metric_index: int) -> Optional[List[float]]:
       if not self.is_connected():
           return None
       data = bytes([(metric_index >> 8) & 0xFF, metric_index & 0xFF]) + self.TAIL_FFFD
       frame = self._build_frame(0xA6, self.CMD_INST_METRIC_RD3X, data)
       try:
           self._ser.reset_input_buffer()
           self._ser.write(frame)
           self._ser.flush()
       except serial.SerialException as exc:
           log.error("Write error: %s", exc)
           return None
       resp = self._read_frame(expect_types=(self.CMD_INST_METRIC_RD3X,))
       if resp is None:
           return None
       start, _, payload = resp
       # error ACK?
       if (start & 0x0F) == 0x9:
           return None
       if len(payload) != 0x14:
           return None
       slots = [ti_c3x_float(payload[i:i + 4]) for i in range(0, 20, 4)]
       return slots
   def _read_inst_metric_rd3x(self, metric_index: int) -> Optional[Tuple[float, float, float]]:
       slots = self._read_inst_metric_slots(metric_index)
       if slots is None:
           return None
       return self._map_slots_to_phases(slots)
   # ── Auto-detect indices (relationship-based) ─────────────────────────────
   @staticmethod
   def _valid(x: float) -> bool:
       return abs(x) < _FLT_MAX_THRESH and x == x  # not NaN, not FLT_MAX-ish
   def autodetect_indices(self) -> Dict[str, int]:
       """
       Probe indices 1..8 and solve mapping using relationships.
       This is designed to avoid the exact failure you showed:
         - VAR returning FLT_MAX sentinel
         - VOLTS being mis-picked and showing nonsense like -461 V
       """
       if not self.is_connected():
           return dict(self._idx)
       # Probe Phase-A values (with reserved-slot remap)
       vals: Dict[int, float] = {}
       for i in range(1, 9):
           v = self._read_inst_metric_rd3x(i)
           if v is None:
               log.debug("autodetect probe idx %d: no response", i)
               continue
           vals[i] = float(v[0])
           log.debug("autodetect probe idx %d: %.6g", i, vals[i])
       log.debug("autodetect probe summary (Phase-A): %s", {k: vals[k] for k in sorted(vals)})
       # Filter out FLT_MAX/sentinels from consideration for picking
       usable = {i: vals[i] for i in vals if self._valid(vals[i])}
       if len(usable) < 6:
           log.warning("autodetect_indices: not enough usable metrics (%d). Keeping defaults.", len(usable))
           return dict(self._idx)
       remaining = set(usable.keys())
       picked: Dict[str, int] = {}
       def pick_best(name: str, score_fn):
           nonlocal remaining, picked
           best_i, best_s = None, -1e18
           for i in remaining:
               s = score_fn(usable[i])
               if s > best_s:
                   best_s, best_i = s, i
           if best_i is not None and best_s > 0:
               picked[name] = best_i
               remaining.remove(best_i)
       # 1) FREQ: closest to 60 in a sane band
       pick_best("FREQ", lambda x: 20.0 - abs(abs(x) - 60.0) if 45.0 <= abs(x) <= 75.0 else 0.0)
       # 2) PF: magnitude around 1, but NOT 60
       pick_best("PF", lambda x: 12.0 - abs(abs(x) - 1.0) * 8.0 if abs(x) <= 1.2 else 0.0)
       # 3) AMPS: near 30 (broad band)
       pick_best("AMPS", lambda x: 12.0 - abs(abs(x) - 30.0) * 0.4 if 0.05 <= abs(x) <= 500.0 else 0.0)
       # 4) VOLTS: pick by line-voltage range (80–280 V) BEFORE power metrics can steal it.
       #    Score peaks at 120 V and 240 V (the two common line voltages).
       pick_best(
           "VOLTS",
           lambda x: (15.0 - min(abs(abs(x) - 120.0), abs(abs(x) - 240.0)) * 0.08)
                     if 80.0 <= abs(x) <= 280.0 else 0.0
       )
       # If the range heuristic missed (e.g. device off, unusual scale), fall back later.
       # 5) Power-like metrics: choose VA and W as the two biggest remaining magnitudes
       power_like = sorted(list(remaining), key=lambda i: abs(usable[i]), reverse=True)
       if power_like:
           picked["VA"] = power_like[0]
           remaining.remove(power_like[0])
       power_like = sorted(list(remaining), key=lambda i: abs(usable[i]), reverse=True)
       if power_like:
           picked["WATTS"] = power_like[0]
           remaining.remove(power_like[0])
       # 6) ANGLE: what's left that looks like degrees (not tiny like PF, not huge like power)
       pick_best(
           "ANGLE",
           lambda x: 6.0 if (2.0 <= abs(x) <= 360.0) else (2.0 if (0.1 <= abs(x) < 2.0) else 0.0)
       )
       # From here we can compute expected V and expected VAR using relationships.
       a = abs(usable[picked["AMPS"]]) if "AMPS" in picked else None
       pf = float(usable[picked["PF"]]) if "PF" in picked else None
       w = float(usable[picked["WATTS"]]) if "WATTS" in picked else None
       va = float(usable[picked["VA"]]) if "VA" in picked else None
       # 7) VAR: prefer closeness to VA*sqrt(1-PF^2), else pick the remaining moderate magnitude
       if va is not None and pf is not None and remaining:
           pf_clip = max(-1.0, min(1.0, pf))
           var_est = abs(va) * (max(0.0, 1.0 - pf_clip * pf_clip) ** 0.5)
           var_i = min(list(remaining), key=lambda i: abs(abs(usable[i]) - var_est))
           picked["VAR"] = var_i
           remaining.remove(var_i)
       else:
           # fallback: smallest of the remaining "not tiny" values
           if remaining:
               rem_sorted = sorted(list(remaining), key=lambda i: abs(usable[i]))
               picked["VAR"] = rem_sorted[0]
               remaining.remove(rem_sorted[0])
       # 8) VOLTS fallback: if step 4 range-heuristic didn't fire, use W/(A*PF) relationship
       if "VOLTS" not in picked:
           if a is not None and pf is not None and w is not None and remaining:
               denom = abs(a * pf)
               if denom > 1e-9:
                   v_est = abs(w) / denom
                   v_i = min(list(remaining), key=lambda i: abs(abs(usable[i]) - v_est))
                   picked["VOLTS"] = v_i
                   remaining.remove(v_i)
       if "VOLTS" not in picked:
           if remaining:
               v_i = min(list(remaining), key=lambda i: abs(abs(usable[i]) - 240.0))
               picked["VOLTS"] = v_i
               remaining.remove(v_i)
           else:
               picked["VOLTS"] = self._idx["VOLTS"]
       # Fill anything missing with defaults
       for name, default_i in self._idx.items():
           if name not in picked:
               picked[name] = default_i
       self._idx = dict(picked)
       self._idx_locked = True
       log.info("RD-21 metric index map (auto): %s", self._idx)
       dbg = {k: usable.get(v, None) for k, v in self._idx.items()}
       log.debug("RD-21 metric values at map (Phase-A): %s", dbg)
       return dict(self._idx)
   # ── Public API ───────────────────────────────────────────────────────────
   def read_instant_3phase(self) -> Optional[List[PhaseReading]]:
       if not self._idx_locked:
           self.autodetect_indices()
       v = self._read_inst_metric_rd3x(self._idx["VOLTS"])
       a = self._read_inst_metric_rd3x(self._idx["AMPS"])
       w = self._read_inst_metric_rd3x(self._idx["WATTS"])
       va = self._read_inst_metric_rd3x(self._idx["VA"])
       var = self._read_inst_metric_rd3x(self._idx["VAR"])
       f = self._read_inst_metric_rd3x(self._idx["FREQ"])
       ang = self._read_inst_metric_rd3x(self._idx["ANGLE"])
       pf = self._read_inst_metric_rd3x(self._idx["PF"])
       if not all([v, a, w, va, var, f, ang, pf]):
           return None
       phases: List[PhaseReading] = []
       for idx, label in enumerate(("A", "B", "C")):
           volt_val = abs(v[idx])
           # If the device returned 0 for voltage, derive it from W / (A × PF).
           if volt_val == 0.0:
               denom = abs(a[idx] * pf[idx])
               if denom > 1e-9:
                   volt_val = abs(w[idx]) / denom
                   log.debug("Ph%s: volt derived from W/(A*PF) = %.4f", label, volt_val)
           phases.append(PhaseReading(
               phase=label,
               volt=volt_val,
               amp=a[idx],
               watt=w[idx],
               va=abs(va[idx]),
               var=var[idx],
               frequency=abs(f[idx]),
               phase_angle=ang[idx],
               power_factor=pf[idx],
           ))
       return phases