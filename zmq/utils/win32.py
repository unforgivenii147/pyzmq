"""Win32 compatibility utilities."""

from __future__ import annotations
import os
from typing import Any, Callable


class allow_interrupt:
    def __init__(self, action: Callable[[], Any] | None = None) -> None:
        if os.name != "nt":
            return
        self._init_action(action)

    def _init_action(self, action):
        from ctypes import WINFUNCTYPE, windll
        from ctypes.wintypes import BOOL, DWORD

        kernel32 = windll.LoadLibrary("kernel32")
        PHANDLER_ROUTINE = WINFUNCTYPE(BOOL, DWORD)
        SetConsoleCtrlHandler = self._SetConsoleCtrlHandler = (
            kernel32.SetConsoleCtrlHandler
        )
        SetConsoleCtrlHandler.argtypes = (PHANDLER_ROUTINE, BOOL)
        SetConsoleCtrlHandler.restype = BOOL
        if action is None:

            def action():
                return None

        self.action = action

        @PHANDLER_ROUTINE
        def handle(event):
            if event == 0:
                action()
            return 0

        self.handle = handle

    def __enter__(self):
        if os.name != "nt":
            return
        result = self._SetConsoleCtrlHandler(self.handle, 1)
        if result == 0:
            raise OSError()

    def __exit__(self, *args):
        if os.name != "nt":
            return
        result = self._SetConsoleCtrlHandler(self.handle, 0)
        if result == 0:
            raise OSError()
