"""System tray icon for FIS watcher status."""

import sys

from PySide6.QtCore import QTimer
from PySide6.QtGui import QAction, QIcon
from PySide6.QtWidgets import QApplication, QMenu, QSystemTrayIcon

from fis.db.models import get_pending_files
from fis.log import get_logger

log = get_logger("tray")


class FISTray:
    """System tray icon showing watcher status and pending count."""

    def __init__(self, app: QApplication):
        self.app = app

        self.tray = QSystemTrayIcon()
        self.tray.setToolTip("FIS — File Intelligence System")

        # Menu
        menu = QMenu()

        self.status_action = QAction("Status: Running")
        self.status_action.setEnabled(False)
        menu.addAction(self.status_action)

        self.pending_action = QAction("Pending: 0")
        self.pending_action.setEnabled(False)
        menu.addAction(self.pending_action)

        menu.addSeparator()

        open_queue = QAction("Open Rename Queue (Ctrl+Alt+F)")
        open_queue.triggered.connect(self._open_popup)
        menu.addAction(open_queue)

        export_action = QAction("Export Kickouts")
        export_action.triggered.connect(self._export_kickouts)
        menu.addAction(export_action)

        menu.addSeparator()

        quit_action = QAction("Quit FIS")
        quit_action.triggered.connect(self.app.quit)
        menu.addAction(quit_action)

        self.tray.setContextMenu(menu)
        self.tray.activated.connect(self._on_activate)
        self.tray.show()

        # Periodic pending count updates (every 30 seconds)
        self._timer = QTimer()
        self._timer.timeout.connect(self.update_pending_count)
        self._timer.start(30_000)

        # Initial count
        self.update_pending_count()

    def update_pending_count(self):
        try:
            files = get_pending_files(limit=1000)
            count = len(files)
            self.pending_action.setText(f"Pending: {count}")
            if count > 0:
                self.tray.setToolTip(f"FIS — {count} files pending review")
            else:
                self.tray.setToolTip("FIS — File Intelligence System")
        except Exception:
            pass

    def _on_activate(self, reason):
        if reason == QSystemTrayIcon.ActivationReason.DoubleClick:
            self._open_popup()

    def _open_popup(self):
        from fis.ui.popup import FISPopup
        self._popup = FISPopup()
        self._popup.show()

    def _export_kickouts(self):
        from fis.export_kickouts import export_kickouts
        export_kickouts()


def launch_tray():
    """Launch the system tray icon standalone."""
    app = QApplication.instance() or QApplication(sys.argv)
    tray = FISTray(app)
    log.info("System tray active.")
    app.exec()
