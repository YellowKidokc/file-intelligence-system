; FIS Hotkeys — AutoHotkey v2
; Place in shell:startup for auto-launch

#Requires AutoHotkey v2.0

; Ctrl+Alt+F — Open rename queue popup
^!f:: {
    Run "python -m fis popup"
}

; Ctrl+Alt+S — Force scan current folder
^!s:: {
    path := Explorer_GetActivePath()
    if (path) {
        Run 'python -m fis backfill --path "' path '" --dry-run'
    }
}

; Ctrl+Alt+K — Export kickouts to Excel
^!k:: {
    Run "python -m fis export"
    MsgBox "Kickouts exported to kickouts.xlsx", "FIS", 64
}

; Ctrl+Alt+B — Run backfill on selected folder
^!b:: {
    path := Explorer_GetActivePath()
    if (path) {
        result := MsgBox("Run FIS backfill on:`n" path "?", "FIS Backfill", 4)
        if (result = "Yes") {
            Run 'python -m fis backfill --path "' path '"'
        }
    }
}

; Helper: get the active Explorer window's path
Explorer_GetActivePath() {
    hwnd := WinGetID("A")
    for window in ComObject("Shell.Application").Windows {
        try {
            if (window.HWND = hwnd) {
                return window.Document.Folder.Self.Path
            }
        }
    }
    return ""
}
