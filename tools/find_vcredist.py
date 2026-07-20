"""
Locate the latest MSVC redist dir

and add it to $GITHUB_PATH so delvewheel can find the DLLs

finds 'C:/Program Files/Microsoft Visual Studio/2022/Enterprise/VC/Redist/MSVC/14.38.33135/arm64/Microsoft.VC143.CRT'
as of writing (2024-02-27)
"""

import os
import sys
from pathlib import Path


def log(msg):
    print(msg, file=sys.stderr)


vs_version = "2022"
arch = os.environ.get("CIBW_ARCHS", "arm64")
vc_redist_path = (
    Path("C:/Program Files/Microsoft Visual Studio")
    / vs_version
    / "Enterprise/VC/Redist/MSVC"
)
log("Found VC redist versions:")
for v in vc_redist_path.glob("*"):
    log(v)


def _sort_key(dll_path):
    version_dir = dll_path.relative_to(vc_redist_path).parents[-2]
    version_str = version_dir.name
    try:
        return tuple((int(part) for part in version_str.split(".")))
    except ValueError:
        log(f"Not an apparent version: {version_str}")
        return (0, 0, 0, version_str)


log(f"Found msvcp for {arch}:")
found_arm_msvcp = sorted(
    vc_redist_path.glob(f"*/{arch}/**/msvcp140.dll"), key=_sort_key
)
for dll in found_arm_msvcp:
    log(dll)
selected_path = found_arm_msvcp[-1].parent
log(f"Selecting {selected_path}")
if os.environ.get("GITHUB_PATH"):
    log(f"Adding {selected_path} to $GITHUB_PATH")
    with open(os.environ["GITHUB_PATH"], "a") as f:
        f.write(str(selected_path) + "\n")
