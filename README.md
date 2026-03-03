# Radian Communication

A standalone script to communicate directly with a **Radian RD-21 Dytronic** power analyzer over serial (RS-232 / USB-serial adapter).

> **Note:** There is no backend, no REST API, and no existing project imports required.
> The only dependency beyond the standard library is `pyserial`.

---

## What It Reads

Per-phase instant metrics for **Phase A, B, and C**:

| Metric | Unit |
|--------|------|
| Volts | V |
| Current | A |
| Power | W |
| Reactive Power | VAR |

All readings are written to a **timestamped CSV file**.

---

## Setup

```bash
pip install pyserial
```

---

## How to Run the Program

### Arguments

| Argument | Short | Default | Description |
|----------|-------|---------|-------------|
| `--port` | `-p` | *(required)* | Serial port, e.g. `COM5` or `/dev/ttyUSB0` |
| `--baud` | `-b` | `9600` | Baud rate |
| `--count` | `-n` | `10` | Number of readings. Use `0` for unlimited |
| `--interval` | `-i` | `1.0` | Seconds between each reading |
| `--out` | `-o` | auto-named | Output CSV path. Defaults to `radian_rd21_YYYYMMDD_HHMMSS.csv` |
| `--list-ports` | | | List available serial ports and exit |
| `--debug` | | | Enable DEBUG logging (shows raw hex frames) |

### Examples

```bash
# Basic — 10 readings, 1 second apart, auto-named CSV
python radian_rd21.py --port COM5 --baud 9600

# Custom count and interval
python radian_rd21.py --port COM5 --baud 9600 --count 30 --interval 2.0

# Specific output file name
python radian_rd21.py --port COM5 --baud 9600 --out lab_test_01.csv

# Run until Ctrl-C (unlimited)
python radian_rd21.py --port COM5 --baud 9600 --count 0

# See what COM ports are available
python radian_rd21.py --list-ports

# Show raw hex frames for debugging
python radian_rd21.py --port COM5 --debug
```

---

## Radian Serial Protocol Overview

The RD-21 uses a simple framed packet format over RS-232.

### Packet Structure (sent and received)

| Byte(s) | Description |
|---------|-------------|
| Byte 0 | `0xA6` — sync / start-of-frame marker |
| Byte 1 | Command byte |
| Byte 2 | Data length low byte (little-endian 16-bit) |
| Byte 3 | Data length high byte |
| Byte 4..N-1 | Data payload |
| Byte N | Checksum — XOR of bytes 1 through N-1 |

### Commands

| Code | Name | Description |
|------|------|-------------|
| `0x02` | `IDENTIFY` | No data; device responds with firmware/model string |
| `0x0D` | `GET_INSTANT` | Request instant measurement packet |

### Instant-Metrics Command Payload

For "all channels, all phases":

```
Data = [0x00, 0x24, 0x00, 0x00, 0x00, 0x14, 0xFF, 0xFD]
  (8 bytes → length field = 0x0008)

Full frame before checksum:
  A6 0D 08 00  00 24 00 00 00 14 FF FD
  Checksum = XOR(0x0D, 0x08, 0x00, 0x00, 0x24, 0x00, 0x00, 0x00, 0x14, 0xFF, 0xFD)
```

### Response Payload

Contains **TI DSP 32-bit floats**. For a 3-phase meter the payload is 3 blocks of 8 floats (96 bytes total):

| Block | Phase | Fields (in order) |
|-------|-------|-------------------|
| 0 | A | volt, amp, watt, va, var, freq, phase_angle, power_factor |
| 1 | B | same order |
| 2 | C | same order |

### TI 32-bit Float Format

> **Note:** This is **NOT** standard IEEE-754.

| Byte | Description |
|------|-------------|
| Byte 0 | Exponent (biased by 128) |
| Byte 1 | Sign (MSB) + mantissa bits 22..16 |
| Byte 2 | Mantissa bits 15..8 |
| Byte 3 | Mantissa bits 7..0 |
