"""Clipboard monitor — watches clipboard and pipes into BIL automatically.

Runs in background. Every time you copy something:
1. Extracts keywords with YAKE
2. Records what app you copied from
3. Sends to BIL clipboard model
4. When you paste, marks it as "used" (signal=1)

Start: python -m fis clipboard
"""

import time
import threading

import win32clipboard
import win32gui
import win32process
import psutil


class ClipboardMonitor:
    def __init__(self, api_url="http://localhost:8420"):
        self.api_url = api_url
        self.last_text = ""
        self.last_copy_app = ""
        self.running = False

    def start(self):
        from fis.log import get_logger
        self._log = get_logger("clipboard")
        self.running = True
        self._log.info("Clipboard monitor running.")
        while self.running:
            try:
                text = self._get_clipboard_text()
                if text and text != self.last_text:
                    app = self._get_foreground_app()
                    self.last_text = text
                    self.last_copy_app = app
                    self._send_to_bil(text, app)
                    self._log.info("%d chars from %s", len(text), app)
            except Exception:
                pass
            time.sleep(0.5)

    def stop(self):
        self.running = False

    def _get_clipboard_text(self) -> str:
        try:
            win32clipboard.OpenClipboard()
            if win32clipboard.IsClipboardFormatAvailable(win32clipboard.CF_UNICODETEXT):
                text = win32clipboard.GetClipboardData(win32clipboard.CF_UNICODETEXT)
                win32clipboard.CloseClipboard()
                return text
            win32clipboard.CloseClipboard()
        except Exception:
            pass
        return ""

    def _get_foreground_app(self) -> str:
        try:
            hwnd = win32gui.GetForegroundWindow()
            _, pid = win32process.GetWindowThreadProcessId(hwnd)
            process = psutil.Process(pid)
            return process.name()
        except Exception:
            return "unknown"

    def _send_to_bil(self, text: str, app: str):
        import json
        import urllib.request

        data = json.dumps({
            "text": text[:2000],
            "app": app,
            "used": False,
        }).encode()

        try:
            req = urllib.request.Request(
                f"{self.api_url}/bil/clipboard",
                data=data,
                headers={"Content-Type": "application/json"},
            )
            urllib.request.urlopen(req, timeout=2)
        except Exception:
            pass  # API not running — that's fine


def start_clipboard_monitor():
    monitor = ClipboardMonitor()
    try:
        monitor.start()
    except KeyboardInterrupt:
        monitor.stop()


if __name__ == "__main__":
    start_clipboard_monitor()
