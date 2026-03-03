"""
radian_rd21_gui.py
==================
PyQt6 GUI front-end for the Radian RD-21 3-phase reader.

Imports the serial driver and data types from radian_rd21_3phase.py —
both files must be in the same directory.

Install dependencies:
    pip install pyserial PyQt6

Run:
    python radian_rd21_gui.py
"""

import csv
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import serial.tools.list_ports

from PyQt6.QtCore import (
    Qt, QThread, pyqtSignal, QTimer, QObject
)
from PyQt6.QtGui import QFont, QColor, QPalette, QFontDatabase
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget,
    QVBoxLayout, QHBoxLayout, QGridLayout,
    QLabel, QPushButton, QComboBox, QLineEdit,
    QTextEdit, QGroupBox, QFileDialog,
    QFrame, QSizePolicy, QStatusBar,
)

# Import driver classes from the CLI script (same directory)
from radian_rd21_3phase import (
    RadianRD21, PhaseReading,
    default_csv_name, readings_to_row, CSV_HEADER,
    POLL_INTERVAL_S,
)


# ── Colour palette ─────────────────────────────────────────────────────────────
#
#  Industrial / instrument dark theme — monochrome base with amber accents.
#  Feels like real test equipment firmware.

BG_DEEP    = "#0D0D0D"   # window background
BG_PANEL   = "#141414"   # group box / card background
BG_FIELD   = "#1C1C1C"   # input field background
BORDER     = "#2A2A2A"   # subtle borders
AMBER      = "#F5A623"   # primary accent — readings, highlights
AMBER_DIM  = "#7A5010"   # dimmed amber for inactive values
GREEN_ON   = "#39D353"   # connected / running indicator
RED_OFF    = "#E84040"   # error / stopped indicator
TEXT_PRI   = "#F0F0F0"   # primary text
TEXT_SEC   = "#707070"   # secondary / label text
TEXT_LOG   = "#909090"   # log pane text


# ── Stylesheet ─────────────────────────────────────────────────────────────────

STYLESHEET = f"""
QMainWindow, QWidget {{
    background-color: {BG_DEEP};
    color: {TEXT_PRI};
    font-family: "Consolas", "Courier New", monospace;
    font-size: 12px;
}}

QGroupBox {{
    background-color: {BG_PANEL};
    border: 1px solid {BORDER};
    border-radius: 4px;
    margin-top: 18px;
    padding: 10px 8px 8px 8px;
    font-size: 11px;
    color: {TEXT_SEC};
    letter-spacing: 1px;
    text-transform: uppercase;
}}

QGroupBox::title {{
    subcontrol-origin: margin;
    subcontrol-position: top left;
    padding: 0 6px;
    color: {TEXT_SEC};
    font-size: 10px;
    letter-spacing: 2px;
}}

QLabel {{
    color: {TEXT_PRI};
    background: transparent;
}}

QLabel.dim {{
    color: {TEXT_SEC};
}}

QComboBox, QLineEdit {{
    background-color: {BG_FIELD};
    border: 1px solid {BORDER};
    border-radius: 3px;
    color: {TEXT_PRI};
    padding: 4px 8px;
    font-family: "Consolas", monospace;
    font-size: 12px;
}}

QComboBox:hover, QLineEdit:hover {{
    border-color: {AMBER_DIM};
}}

QComboBox:focus, QLineEdit:focus {{
    border-color: {AMBER};
}}

QComboBox QAbstractItemView {{
    background-color: {BG_FIELD};
    color: {TEXT_PRI};
    selection-background-color: {AMBER_DIM};
    border: 1px solid {BORDER};
}}

QPushButton {{
    background-color: {BG_FIELD};
    border: 1px solid {BORDER};
    border-radius: 3px;
    color: {TEXT_PRI};
    padding: 6px 18px;
    font-family: "Consolas", monospace;
    font-size: 12px;
    letter-spacing: 1px;
}}

QPushButton:hover {{
    border-color: {AMBER};
    color: {AMBER};
}}

QPushButton:pressed {{
    background-color: {AMBER_DIM};
}}

QPushButton:disabled {{
    color: {TEXT_SEC};
    border-color: {BORDER};
}}

QPushButton#startBtn {{
    border-color: {GREEN_ON};
    color: {GREEN_ON};
    font-weight: bold;
    letter-spacing: 2px;
}}

QPushButton#startBtn:hover {{
    background-color: #0F2A14;
}}

QPushButton#stopBtn {{
    border-color: {RED_OFF};
    color: {RED_OFF};
    font-weight: bold;
    letter-spacing: 2px;
}}

QPushButton#stopBtn:hover {{
    background-color: #2A0F0F;
}}

QTextEdit {{
    background-color: {BG_FIELD};
    border: 1px solid {BORDER};
    border-radius: 3px;
    color: {TEXT_LOG};
    font-family: "Consolas", "Courier New", monospace;
    font-size: 11px;
    padding: 4px;
}}

QStatusBar {{
    background-color: {BG_PANEL};
    color: {TEXT_SEC};
    border-top: 1px solid {BORDER};
    font-size: 11px;
}}

QFrame#divider {{
    background-color: {BORDER};
}}
"""


# ── Value display label ─────────────────────────────────────────────────────────

class ValueLabel(QLabel):
    """Large amber numeric readout used inside phase cards."""

    def __init__(self, parent=None):
        super().__init__("---", parent)
        self.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        font = QFont("Consolas", 20, QFont.Weight.Bold)
        self.setFont(font)
        self.setStyleSheet(f"color: {AMBER_DIM}; background: transparent;")
        self._active = False

    def set_value(self, value: float, fmt: str = ".3f"):
        self.setText(format(value, fmt))
        if not self._active:
            self._active = True
            self.setStyleSheet(f"color: {AMBER}; background: transparent;")

    def reset(self):
        self.setText("---")
        self._active = False
        self.setStyleSheet(f"color: {AMBER_DIM}; background: transparent;")


# ── Phase card ──────────────────────────────────────────────────────────────────

class PhaseCard(QGroupBox):
    """
    One card per phase (A / B / C) showing Volts, Amps, Watts, VARs.
    Also shows VA, Freq, Angle, PF in a smaller secondary row.
    """

    LABEL_STYLE = f"color: {TEXT_SEC}; font-size: 10px; letter-spacing: 1px; background: transparent;"
    UNIT_STYLE  = f"color: {TEXT_SEC}; font-size: 11px; background: transparent;"

    def __init__(self, phase_label: str, parent=None):
        super().__init__(f"PHASE  {phase_label}", parent)
        self.phase_label = phase_label
        self._build()

    def _build(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(4)

        # Primary metrics — large readouts
        for attr, unit, fmt in [
            ("volt",  "V",   ".3f"),
            ("amp",   "A",   ".4f"),
            ("watt",  "W",   ".3f"),
            ("var",   "VAR", ".3f"),
        ]:
            row = QHBoxLayout()
            lbl = QLabel(attr.upper())
            lbl.setStyleSheet(self.LABEL_STYLE)
            lbl.setFixedWidth(38)

            val = ValueLabel()
            setattr(self, f"_{attr}_val", val)
            setattr(self, f"_{attr}_fmt", fmt)

            unit_lbl = QLabel(unit)
            unit_lbl.setStyleSheet(self.UNIT_STYLE)
            unit_lbl.setFixedWidth(32)

            row.addWidget(lbl)
            row.addWidget(val, 1)
            row.addWidget(unit_lbl)
            layout.addLayout(row)

        # Divider
        div = QFrame()
        div.setObjectName("divider")
        div.setFixedHeight(1)
        layout.addWidget(div)

        # Secondary metrics — smaller
        sec_layout = QGridLayout()
        sec_layout.setSpacing(4)
        for col, (attr, unit) in enumerate([
            ("va",    "VA"),
            ("freq",  "Hz"),
            ("angle", "°"),
            ("pf",    "PF"),
        ]):
            lbl = QLabel(attr.upper())
            lbl.setStyleSheet(self.LABEL_STYLE)
            lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)

            val = QLabel("---")
            val.setAlignment(Qt.AlignmentFlag.AlignCenter)
            val.setStyleSheet(f"color: {TEXT_SEC}; font-size: 11px; background: transparent;")
            setattr(self, f"_{attr}_sec", val)

            u = QLabel(unit)
            u.setAlignment(Qt.AlignmentFlag.AlignCenter)
            u.setStyleSheet(self.UNIT_STYLE)

            sec_layout.addWidget(lbl, 0, col)
            sec_layout.addWidget(val, 1, col)
            sec_layout.addWidget(u,   2, col)

        layout.addLayout(sec_layout)

    def update(self, reading: PhaseReading):
        self._volt_val.set_value(reading.volt,  ".3f")
        self._amp_val.set_value(reading.amp,    ".4f")
        self._watt_val.set_value(reading.watt,  ".3f")
        self._var_val.set_value(reading.var,    ".3f")

        self._va_sec.setText(f"{reading.va:.2f}")
        self._va_sec.setStyleSheet(f"color: {TEXT_PRI}; font-size: 11px; background: transparent;")
        self._freq_sec.setText(f"{reading.frequency:.3f}")
        self._freq_sec.setStyleSheet(f"color: {TEXT_PRI}; font-size: 11px; background: transparent;")
        self._angle_sec.setText(f"{reading.phase_angle:.2f}")
        self._angle_sec.setStyleSheet(f"color: {TEXT_PRI}; font-size: 11px; background: transparent;")
        self._pf_sec.setText(f"{reading.power_factor:.4f}")
        self._pf_sec.setStyleSheet(f"color: {TEXT_PRI}; font-size: 11px; background: transparent;")

    def reset(self):
        for attr in ("volt", "amp", "watt", "var"):
            getattr(self, f"_{attr}_val").reset()
        for attr in ("va", "freq", "angle", "pf"):
            w = getattr(self, f"_{attr}_sec")
            w.setText("---")
            w.setStyleSheet(f"color: {TEXT_SEC}; font-size: 11px; background: transparent;")


# ── Poll worker (runs in a QThread) ────────────────────────────────────────────

class PollWorker(QObject):
    """
    Runs the 1-second Radian poll loop in a background QThread.
    Emits signals back to the main thread for UI updates.
    """
    reading_ready  = pyqtSignal(list)   # list[PhaseReading]
    error          = pyqtSignal(str)
    stopped        = pyqtSignal()

    def __init__(self, radian: RadianRD21):
        super().__init__()
        self._radian   = radian
        self._running  = False
        self._paused   = False

    def start_poll(self):
        self._running = True
        self._paused  = False
        consec_errs   = 0

        while self._running:
            if not self._paused:
                phases = self._radian.read_instant_3phase()
                if phases:
                    consec_errs = 0
                    self.reading_ready.emit(phases)
                else:
                    consec_errs += 1
                    self.error.emit(f"Read failed ({consec_errs} consecutive)")
                    if consec_errs >= 5:
                        self.error.emit("5 consecutive failures — stopping.")
                        self._running = False
                        break

            # Interruptible 1-second sleep
            deadline = time.monotonic() + POLL_INTERVAL_S
            while time.monotonic() < deadline and self._running:
                time.sleep(0.05)

        self.stopped.emit()

    def pause(self):
        self._paused = True

    def resume(self):
        self._paused = False

    def stop(self):
        self._running = False


# ── Status indicator ────────────────────────────────────────────────────────────

class StatusDot(QLabel):
    """Small coloured circle indicating connected/running state."""

    def __init__(self, parent=None):
        super().__init__("●", parent)
        self.setFont(QFont("Arial", 14))
        self.set_off()

    def set_off(self):
        self.setStyleSheet(f"color: {TEXT_SEC}; background: transparent;")
        self.setToolTip("Disconnected")

    def set_connected(self):
        self.setStyleSheet(f"color: {AMBER}; background: transparent;")
        self.setToolTip("Connected")

    def set_running(self):
        self.setStyleSheet(f"color: {GREEN_ON}; background: transparent;")
        self.setToolTip("Running")

    def set_error(self):
        self.setStyleSheet(f"color: {RED_OFF}; background: transparent;")
        self.setToolTip("Error")


# ── Main window ─────────────────────────────────────────────────────────────────

class MainWindow(QMainWindow):

    def __init__(self):
        super().__init__()
        self.setWindowTitle("Radian RD-21  ·  3-Phase Monitor")
        self.setMinimumSize(980, 640)

        self._radian:      Optional[RadianRD21] = None
        self._worker:      Optional[PollWorker] = None
        self._thread:      Optional[QThread]    = None
        self._csv_file     = None
        self._csv_writer   = None
        self._csv_path:    str  = ""
        self._reading_num: int  = 0
        self._start_time:  Optional[float] = None
        self._paused:      bool = False

        self._build_ui()
        self.setStyleSheet(STYLESHEET)
        self._refresh_ports()

        # Clock timer — updates elapsed time display every second
        self._clock = QTimer()
        self._clock.timeout.connect(self._tick_clock)
        self._clock.start(1000)

    # ── UI construction ────────────────────────────────────────────────────────

    def _build_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        root = QVBoxLayout(central)
        root.setContentsMargins(12, 12, 12, 8)
        root.setSpacing(10)

        # ── Top bar: title + status dot ───────────────────────────────────────
        top_bar = QHBoxLayout()
        title = QLabel("RD-21  DYTRONIC")
        title.setFont(QFont("Consolas", 16, QFont.Weight.Bold))
        title.setStyleSheet(f"color: {AMBER}; letter-spacing: 3px; background: transparent;")
        subtitle = QLabel("3-PHASE POWER MONITOR")
        subtitle.setStyleSheet(f"color: {TEXT_SEC}; font-size: 10px; letter-spacing: 4px; background: transparent;")
        sub_col = QVBoxLayout()
        sub_col.setSpacing(0)
        sub_col.addWidget(title)
        sub_col.addWidget(subtitle)
        top_bar.addLayout(sub_col)
        top_bar.addStretch()
        self._status_dot = StatusDot()
        self._status_label = QLabel("OFFLINE")
        self._status_label.setStyleSheet(f"color: {TEXT_SEC}; font-size: 11px; letter-spacing: 2px; background: transparent;")
        top_bar.addWidget(self._status_dot)
        top_bar.addSpacing(6)
        top_bar.addWidget(self._status_label)
        root.addLayout(top_bar)

        # ── Connection panel ──────────────────────────────────────────────────
        conn_group = QGroupBox("Connection")
        conn_layout = QHBoxLayout(conn_group)
        conn_layout.setSpacing(8)

        conn_layout.addWidget(QLabel("Port"))
        self._port_combo = QComboBox()
        self._port_combo.setMinimumWidth(120)
        conn_layout.addWidget(self._port_combo)

        self._refresh_btn = QPushButton("↺")
        self._refresh_btn.setFixedWidth(32)
        self._refresh_btn.setToolTip("Refresh port list")
        self._refresh_btn.clicked.connect(self._refresh_ports)
        conn_layout.addWidget(self._refresh_btn)

        conn_layout.addSpacing(12)
        conn_layout.addWidget(QLabel("Baud"))
        self._baud_combo = QComboBox()
        self._baud_combo.addItems(["9600", "19200", "38400", "57600", "115200"])
        self._baud_combo.setCurrentText("9600")
        self._baud_combo.setMinimumWidth(90)
        conn_layout.addWidget(self._baud_combo)

        conn_layout.addSpacing(12)
        conn_layout.addWidget(QLabel("CSV Output"))
        self._csv_edit = QLineEdit()
        self._csv_edit.setPlaceholderText("auto-named  (click ⋯ to choose)")
        self._csv_edit.setMinimumWidth(240)
        conn_layout.addWidget(self._csv_edit, 1)
        self._browse_btn = QPushButton("⋯")
        self._browse_btn.setFixedWidth(32)
        self._browse_btn.setToolTip("Choose output file")
        self._browse_btn.clicked.connect(self._browse_csv)
        conn_layout.addWidget(self._browse_btn)

        conn_layout.addSpacing(16)
        self._connect_btn = QPushButton("CONNECT")
        self._connect_btn.setMinimumWidth(100)
        self._connect_btn.clicked.connect(self._on_connect)
        conn_layout.addWidget(self._connect_btn)

        root.addWidget(conn_group)

        # ── Phase cards ───────────────────────────────────────────────────────
        cards_layout = QHBoxLayout()
        cards_layout.setSpacing(10)
        self._phase_cards: dict[str, PhaseCard] = {}
        for label in ("A", "B", "C"):
            card = PhaseCard(label)
            card.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
            self._phase_cards[label] = card
            cards_layout.addWidget(card)
        root.addLayout(cards_layout)

        # ── Control + stats row ───────────────────────────────────────────────
        ctrl_row = QHBoxLayout()
        ctrl_row.setSpacing(10)

        # Start / Stop / Pause buttons
        self._start_btn = QPushButton("▶  START")
        self._start_btn.setObjectName("startBtn")
        self._start_btn.setFixedHeight(36)
        self._start_btn.setEnabled(False)
        self._start_btn.clicked.connect(self._on_start)
        ctrl_row.addWidget(self._start_btn)

        self._pause_btn = QPushButton("⏸  PAUSE")
        self._pause_btn.setFixedHeight(36)
        self._pause_btn.setEnabled(False)
        self._pause_btn.clicked.connect(self._on_pause)
        ctrl_row.addWidget(self._pause_btn)

        self._stop_btn = QPushButton("■  STOP")
        self._stop_btn.setObjectName("stopBtn")
        self._stop_btn.setFixedHeight(36)
        self._stop_btn.setEnabled(False)
        self._stop_btn.clicked.connect(self._on_stop)
        ctrl_row.addWidget(self._stop_btn)

        ctrl_row.addSpacing(16)

        # Stats display
        stats_group = QGroupBox("Session")
        stats_layout = QHBoxLayout(stats_group)
        stats_layout.setSpacing(24)

        for attr, caption in [
            ("_lbl_readings", "READINGS"),
            ("_lbl_elapsed",  "ELAPSED"),
            ("_lbl_csv",      "CSV FILE"),
        ]:
            col = QVBoxLayout()
            col.setSpacing(2)
            cap = QLabel(caption)
            cap.setStyleSheet(f"color: {TEXT_SEC}; font-size: 9px; letter-spacing: 2px; background: transparent;")
            val = QLabel("—")
            val.setStyleSheet(f"color: {TEXT_PRI}; font-size: 13px; background: transparent;")
            if attr == "_lbl_csv":
                val.setMaximumWidth(300)
            col.addWidget(cap)
            col.addWidget(val)
            setattr(self, attr, val)
            stats_layout.addLayout(col)

        stats_layout.addStretch()
        ctrl_row.addWidget(stats_group, 1)

        root.addLayout(ctrl_row)

        # ── Log pane ──────────────────────────────────────────────────────────
        log_group = QGroupBox("Log")
        log_layout = QVBoxLayout(log_group)
        log_layout.setContentsMargins(6, 6, 6, 6)
        self._log_pane = QTextEdit()
        self._log_pane.setReadOnly(True)
        self._log_pane.setMaximumHeight(130)
        log_layout.addWidget(self._log_pane)
        root.addWidget(log_group)

        # ── Status bar ────────────────────────────────────────────────────────
        self._status_bar = QStatusBar()
        self.setStatusBar(self._status_bar)
        self._status_bar.showMessage("Select a port and click CONNECT to begin.")

    # ── Port helpers ───────────────────────────────────────────────────────────

    def _refresh_ports(self):
        ports = [p.device for p in serial.tools.list_ports.comports()]
        self._port_combo.clear()
        if ports:
            self._port_combo.addItems(ports)
            self._log(f"Found {len(ports)} port(s): {', '.join(ports)}")
        else:
            self._log("No serial ports detected.")

    def _browse_csv(self):
        path, _ = QFileDialog.getSaveFileName(
            self, "Choose CSV output file",
            default_csv_name(),
            "CSV Files (*.csv);;All Files (*)",
        )
        if path:
            self._csv_edit.setText(path)

    # ── Connect / disconnect ───────────────────────────────────────────────────

    def _on_connect(self):
        if self._radian and self._radian.is_connected():
            self._disconnect()
        else:
            self._connect()

    def _connect(self):
        port = self._port_combo.currentText()
        if not port:
            self._log("ERROR: No port selected.")
            return
        baud = int(self._baud_combo.currentText())

        self._log(f"Connecting to {port} @ {baud} baud…")
        self._status_bar.showMessage(f"Connecting to {port}…")

        self._radian = RadianRD21(port, baud)
        if self._radian.connect():
            self._log(f"Connected to Radian on {port}.")
            self._status_dot.set_connected()
            self._status_label.setText("CONNECTED")
            self._connect_btn.setText("DISCONNECT")
            self._start_btn.setEnabled(True)
            self._port_combo.setEnabled(False)
            self._baud_combo.setEnabled(False)
            self._status_bar.showMessage(f"Connected — {port} @ {baud}. Ready to start.")
        else:
            self._log(f"ERROR: Could not connect to {port}. Check device and baud rate.")
            self._status_dot.set_error()
            self._status_label.setText("ERROR")
            self._radian = None
            self._status_bar.showMessage("Connection failed.")

    def _disconnect(self):
        if self._worker:
            self._stop_worker_immediate()
        if self._radian:
            self._radian.disconnect()
            self._radian = None
        self._status_dot.set_off()
        self._status_label.setText("OFFLINE")
        self._connect_btn.setText("CONNECT")
        self._start_btn.setEnabled(False)
        self._port_combo.setEnabled(True)
        self._baud_combo.setEnabled(True)
        self._log("Disconnected.")
        self._status_bar.showMessage("Disconnected.")

    # ── Start / pause / stop ───────────────────────────────────────────────────

    def _on_start(self):
        if not self._radian or not self._radian.is_connected():
            self._log("ERROR: Not connected.")
            return

        # Determine CSV path
        path = self._csv_edit.text().strip() or default_csv_name()
        self._csv_path = path
        self._lbl_csv.setText(Path(path).name)

        # Open CSV
        try:
            self._csv_file   = open(path, "w", newline="", encoding="utf-8")
            self._csv_writer = csv.writer(self._csv_file)
            self._csv_writer.writerow(CSV_HEADER)
            self._csv_file.flush()
        except OSError as e:
            self._log(f"ERROR: Cannot open CSV file: {e}")
            return

        self._reading_num = 0
        self._start_time  = time.monotonic()
        self._paused      = False
        self._lbl_readings.setText("0")
        self._lbl_elapsed.setText("00:00:00")

        # Start worker thread
        self._worker = PollWorker(self._radian)
        self._thread = QThread()
        self._worker.moveToThread(self._thread)
        self._thread.started.connect(self._worker.start_poll)
        self._worker.reading_ready.connect(self._on_reading)
        self._worker.error.connect(self._on_worker_error)
        self._worker.stopped.connect(self._on_worker_stopped)
        self._thread.start()

        self._start_btn.setEnabled(False)
        self._pause_btn.setEnabled(True)
        self._stop_btn.setEnabled(True)
        self._connect_btn.setEnabled(False)
        self._status_dot.set_running()
        self._status_label.setText("RUNNING")
        self._log(f"Started. Writing to: {path}")
        self._status_bar.showMessage(f"Running — {path}")

    def _on_pause(self):
        if not self._worker:
            return
        if not self._paused:
            self._worker.pause()
            self._paused = True
            self._pause_btn.setText("▶  RESUME")
            self._status_dot.set_connected()
            self._status_label.setText("PAUSED")
            self._log("Paused.")
            self._status_bar.showMessage("Paused — click RESUME to continue.")
        else:
            self._worker.resume()
            self._paused = False
            self._pause_btn.setText("⏸  PAUSE")
            self._status_dot.set_running()
            self._status_label.setText("RUNNING")
            self._log("Resumed.")
            self._status_bar.showMessage("Running.")

    def _on_stop(self):
        """Stop button handler — asks for confirmation before stopping."""
        if not self._worker:
            return

        from PyQt6.QtWidgets import QMessageBox
        dlg = QMessageBox(self)
        dlg.setWindowTitle("Stop Session")
        dlg.setText("Stop the current reading session?")
        dlg.setInformativeText(
            f"{self._reading_num} reading(s) recorded so far.\n"
            "The CSV will be saved and closed."
        )
        dlg.setStandardButtons(
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.Cancel
        )
        dlg.setDefaultButton(QMessageBox.StandardButton.Cancel)
        dlg.setStyleSheet(STYLESHEET)

        if dlg.exec() == QMessageBox.StandardButton.Yes:
            self._stop_worker_immediate()

    def _stop_worker_immediate(self):
        """Stop the poll worker without confirmation (used by closeEvent)."""
        if self._worker:
            self._worker.stop()
            # _on_worker_stopped() handles cleanup once the thread finishes

    def _on_worker_stopped(self):
        if self._thread:
            self._thread.quit()
            self._thread.wait()
            self._thread = None
        self._worker = None

        if self._csv_file:
            self._csv_file.flush()
            self._csv_file.close()
            self._csv_file   = None
            self._csv_writer = None
            self._log(f"CSV saved: {self._csv_path} ({self._reading_num} readings)")

        self._start_btn.setEnabled(True)
        self._pause_btn.setEnabled(False)
        self._pause_btn.setText("⏸  PAUSE")
        self._stop_btn.setEnabled(False)
        self._connect_btn.setEnabled(True)
        self._paused = False

        if self._radian and self._radian.is_connected():
            self._status_dot.set_connected()
            self._status_label.setText("CONNECTED")
        else:
            self._status_dot.set_off()
            self._status_label.setText("OFFLINE")

        self._status_bar.showMessage(
            f"Stopped. {self._reading_num} reading(s) written to {self._csv_path}"
        )

    # ── Data handlers ──────────────────────────────────────────────────────────

    def _on_reading(self, phases: list):
        self._reading_num += 1
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        # Update phase cards
        for p in phases:
            card = self._phase_cards.get(p.phase)
            if card:
                card.update(p)

        # Write CSV
        if self._csv_writer and self._csv_file:
            self._csv_writer.writerow(readings_to_row(ts, phases))
            self._csv_file.flush()

        # Update stats
        self._lbl_readings.setText(str(self._reading_num))

        # Log line
        parts = [f"#{self._reading_num:>5}  {ts}"]
        for p in phases:
            parts.append(
                f"  Ph{p.phase}: {p.volt:8.3f}V  {p.amp:8.4f}A"
                f"  {p.watt:9.3f}W  {p.var:9.3f}VAR"
            )
        self._log("  ".join(parts))

    def _on_worker_error(self, msg: str):
        self._log(f"WARN  {msg}")
        self._status_bar.showMessage(f"Warning: {msg}")

    # ── Clock ──────────────────────────────────────────────────────────────────

    def _tick_clock(self):
        if self._start_time and not self._paused:
            elapsed = time.monotonic() - self._start_time
            m, s = divmod(int(elapsed), 60)
            h, m = divmod(m, 60)
            self._lbl_elapsed.setText(f"{h:02d}:{m:02d}:{s:02d}")

    # ── Log helper ─────────────────────────────────────────────────────────────

    def _log(self, msg: str):
        ts  = datetime.now().strftime("%H:%M:%S")
        self._log_pane.append(f"<span style='color:{TEXT_SEC}'>{ts}</span>  {msg}")
        sb = self._log_pane.verticalScrollBar()
        sb.setValue(sb.maximum())

    # ── Close event ────────────────────────────────────────────────────────────

    def closeEvent(self, event):
        if self._worker:
            self._worker.stop()
            if self._thread:
                self._thread.quit()
                self._thread.wait()
        if self._csv_file:
            self._csv_file.flush()
            self._csv_file.close()
        if self._radian:
            self._radian.disconnect()
        event.accept()


# ── Entry point ─────────────────────────────────────────────────────────────────

def main():
    app = QApplication(sys.argv)
    app.setStyle("Fusion")

    win = MainWindow()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()