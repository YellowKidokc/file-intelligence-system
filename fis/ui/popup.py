"""FIS Popup — Ctrl+Alt+F rename queue and code search interface."""

import sys

from PySide6.QtCore import Qt, QTimer
from PySide6.QtGui import QColor, QFont, QKeySequence, QShortcut
from PySide6.QtWidgets import (
    QApplication,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMainWindow,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from fis.db.models import get_pending_files, get_subject_codes, search_files, update_file_status
from fis.renamer import rename_file


class RenameQueueTab(QWidget):
    """Tab showing pending rename proposals."""

    def __init__(self):
        super().__init__()
        layout = QVBoxLayout(self)

        # Header
        header = QHBoxLayout()
        self.count_label = QLabel("Pending: 0")
        self.count_label.setFont(QFont("Segoe UI", 12, QFont.Weight.Bold))
        header.addWidget(self.count_label)

        refresh_btn = QPushButton("Refresh")
        refresh_btn.clicked.connect(self.load_pending)
        header.addWidget(refresh_btn)

        approve_all_btn = QPushButton("Approve All")
        approve_all_btn.clicked.connect(self.approve_all)
        header.addWidget(approve_all_btn)

        layout.addLayout(header)

        # Table
        self.table = QTableWidget()
        self.table.setColumnCount(6)
        self.table.setHorizontalHeaderLabels([
            "Original", "Proposed", "Domain", "Subjects", "Confidence", "Action"
        ])
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.table.setAlternatingRowColors(True)
        layout.addWidget(self.table)

        self.load_pending()

    def load_pending(self):
        files = get_pending_files(limit=100)
        self.table.setRowCount(len(files))
        self.count_label.setText(f"Pending: {len(files)}")

        for row, f in enumerate(files):
            self.table.setItem(row, 0, QTableWidgetItem(f["original_name"]))
            self.table.setItem(row, 1, QTableWidgetItem(f["proposed_name"] or ""))
            self.table.setItem(row, 2, QTableWidgetItem(f["domain"] or ""))
            self.table.setItem(row, 3, QTableWidgetItem(
                ", ".join(f["subject_codes"]) if f["subject_codes"] else ""
            ))

            conf = f["confidence"] or 0
            conf_item = QTableWidgetItem(f"{conf:.0f}%")
            if conf >= 85:
                conf_item.setForeground(QColor("#2E7D32"))
            elif conf >= 50:
                conf_item.setForeground(QColor("#F57F17"))
            else:
                conf_item.setForeground(QColor("#C62828"))
            self.table.setItem(row, 4, conf_item)

            # Approve button
            approve_btn = QPushButton("Approve")
            approve_btn.setProperty("file_id", f["file_id"])
            approve_btn.setProperty("file_path", f["file_path"])
            approve_btn.setProperty("proposed_name", f["proposed_name"])
            approve_btn.clicked.connect(self._approve_single)
            self.table.setCellWidget(row, 5, approve_btn)

    def _approve_single(self):
        btn = self.sender()
        file_id = btn.property("file_id")
        file_path = btn.property("file_path")
        proposed = btn.property("proposed_name")
        if proposed:
            rename_file(file_path, proposed, file_id)
        self.load_pending()

    def approve_all(self):
        files = get_pending_files(limit=100)
        for f in files:
            if f["proposed_name"] and f["confidence"] and f["confidence"] >= 50:
                rename_file(f["file_path"], f["proposed_name"], f["file_id"])
        self.load_pending()


class CodeSearchTab(QWidget):
    """Tab for searching subject codes and files by concept."""

    def __init__(self):
        super().__init__()
        layout = QVBoxLayout(self)

        # Search bar
        search_layout = QHBoxLayout()
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Type a concept... (consciousness, master equation, entropy)")
        self.search_input.setFont(QFont("Segoe UI", 12))
        self.search_input.textChanged.connect(self._on_search)
        search_layout.addWidget(self.search_input)
        layout.addLayout(search_layout)

        # Results: codes
        self.code_label = QLabel("Matching Codes:")
        self.code_label.setFont(QFont("Segoe UI", 10, QFont.Weight.Bold))
        layout.addWidget(self.code_label)

        self.code_table = QTableWidget()
        self.code_table.setColumnCount(4)
        self.code_table.setHorizontalHeaderLabels(["Code", "Label", "Domain", "Aliases"])
        self.code_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.code_table.setMaximumHeight(200)
        layout.addWidget(self.code_table)

        # Results: files
        self.file_label = QLabel("Matching Files:")
        self.file_label.setFont(QFont("Segoe UI", 10, QFont.Weight.Bold))
        layout.addWidget(self.file_label)

        self.file_table = QTableWidget()
        self.file_table.setColumnCount(5)
        self.file_table.setHorizontalHeaderLabels(["Name", "Domain", "Subjects", "Tags", "Path"])
        self.file_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        layout.addWidget(self.file_table)

    def _on_search(self, text: str):
        if len(text) < 2:
            return

        # Search codes
        try:
            codes = get_subject_codes()
            matching = []
            for code in codes:
                searchable = (
                    (code["label"] or "").lower() + " " +
                    " ".join(code.get("aliases") or []).lower() + " " +
                    (code.get("description") or "").lower()
                )
                if text.lower() in searchable:
                    matching.append(code)

            self.code_table.setRowCount(len(matching))
            for row, c in enumerate(matching):
                self.code_table.setItem(row, 0, QTableWidgetItem(c["code"]))
                self.code_table.setItem(row, 1, QTableWidgetItem(c["label"]))
                self.code_table.setItem(row, 2, QTableWidgetItem(c["domain"]))
                self.code_table.setItem(row, 3, QTableWidgetItem(
                    ", ".join(c.get("aliases") or [])
                ))
        except Exception:
            pass

        # Search files
        try:
            files = search_files(text, limit=20)
            self.file_table.setRowCount(len(files))
            for row, f in enumerate(files):
                self.file_table.setItem(row, 0, QTableWidgetItem(
                    f.get("final_name") or f.get("proposed_name") or f["original_name"]
                ))
                self.file_table.setItem(row, 1, QTableWidgetItem(f.get("domain") or ""))
                self.file_table.setItem(row, 2, QTableWidgetItem(
                    ", ".join(f.get("subject_codes") or [])
                ))
                self.file_table.setItem(row, 3, QTableWidgetItem(
                    ", ".join(f.get("tags") or [])
                ))
                self.file_table.setItem(row, 4, QTableWidgetItem(f.get("file_path") or ""))
        except Exception:
            pass


class FISPopup(QMainWindow):
    """Main FIS popup window — triggered by Ctrl+Alt+F."""

    def __init__(self):
        super().__init__()
        self.setWindowTitle("FIS — File Intelligence System")
        self.setMinimumSize(900, 600)
        self.setWindowFlags(Qt.WindowType.WindowStaysOnTopHint)

        # Dark theme
        self.setStyleSheet("""
            QMainWindow { background-color: #1E1E2E; }
            QWidget { background-color: #1E1E2E; color: #CDD6F4; }
            QTableWidget { background-color: #313244; alternate-background-color: #45475A;
                          gridline-color: #585B70; border: none; }
            QHeaderView::section { background-color: #585B70; color: #CDD6F4;
                                  padding: 6px; border: none; font-weight: bold; }
            QLineEdit { background-color: #313244; border: 2px solid #585B70;
                       border-radius: 8px; padding: 8px; color: #CDD6F4; font-size: 14px; }
            QLineEdit:focus { border-color: #89B4FA; }
            QPushButton { background-color: #89B4FA; color: #1E1E2E; border: none;
                         border-radius: 6px; padding: 6px 16px; font-weight: bold; }
            QPushButton:hover { background-color: #B4D0FB; }
            QTabWidget::pane { border: none; }
            QTabBar::tab { background-color: #313244; color: #CDD6F4; padding: 8px 20px;
                          border: none; margin-right: 2px; }
            QTabBar::tab:selected { background-color: #585B70; }
        """)

        # Tabs
        tabs = QTabWidget()
        tabs.addTab(RenameQueueTab(), "Rename Queue")
        tabs.addTab(CodeSearchTab(), "Code Search")
        self.setCentralWidget(tabs)

        # Escape to close
        QShortcut(QKeySequence(Qt.Key.Key_Escape), self, self.close)


def launch_popup():
    app = QApplication.instance() or QApplication(sys.argv)
    window = FISPopup()
    window.show()
    app.exec()


if __name__ == "__main__":
    launch_popup()
