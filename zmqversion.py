"""A simply script to scrape zmq.h for the zeromq version.
This is similar to the version.sh script in a zeromq source dir, but
it searches for an installed header, rather than in the current dir.
"""

import os
import re
import sys
import traceback
from configparser import ConfigParser
from warnings import warn

pjoin = os.path.join
MAJOR_PAT = "^#define +ZMQ_VERSION_MAJOR +[0-9]+$"
MINOR_PAT = "^#define +ZMQ_VERSION_MINOR +[0-9]+$"
PATCH_PAT = "^#define +ZMQ_VERSION_PATCH +[0-9]+$"


def include_dirs_from_path():
    include_dirs = []
    for p in os.environ["PATH"].split(os.path.pathsep):
        if p.endswith("/"):
            p = p[:-1]
        if p.endswith("bin"):
            include_dirs.append(p[:-3] + "include")
    return include_dirs


def default_include_dirs():
    return ["/usr/local/include", "/usr/include"]


def find_zmq_version():
    include_dirs = []
    if os.path.exists("setup.cfg"):
        cfg = ConfigParser()
        cfg.read("setup.cfg")
        if "build_ext" in cfg.sections():
            items = cfg.items("build_ext")
            for name, val in items:
                if name == "include_dirs":
                    include_dirs = val.split(os.path.pathsep)
    if not include_dirs:
        include_dirs = default_include_dirs()
    for include in include_dirs:
        zmq_h = pjoin(include, "zmq.h")
        if os.path.isfile(zmq_h):
            with open(zmq_h) as f:
                contents = f.read()
        else:
            continue
        line = re.findall(MAJOR_PAT, contents, re.MULTILINE)[0]
        major = int(re.findall("[0-9]+", line)[0])
        line = re.findall(MINOR_PAT, contents, re.MULTILINE)[0]
        minor = int(re.findall("[0-9]+", line)[0])
        line = re.findall(PATCH_PAT, contents, re.MULTILINE)[0]
        patch = int(re.findall("[0-9]+", line)[0])
        return ((major, minor, patch), zmq_h)
    raise OSError("Couldn't find zmq.h")


def ver_str(version):
    return ".".join(map(str, version))


def check_zmq_version(min_version):
    sv = ver_str(min_version)
    try:
        found, zmq_h = find_zmq_version()
        sf = ver_str(found)
        if found < min_version:
            print(f"This pyzmq requires zeromq >= {sv}")
            print(f"but it appears you are building against {zmq_h}")
            print(f"which has zeromq {sf}")
            sys.exit(1)
    except OSError:
        msg = "\n".join(
            [
                "Couldn't find zmq.h to check for version compatibility.",
                "If you see 'undeclared identifier' errors, your ZeroMQ is likely too old.",
                f"This pyzmq requires zeromq >= {sv}",
            ]
        )
        warn(msg)
    except IndexError:
        msg = "\n".join(
            [
                "Couldn't find ZMQ_VERSION macros in zmq.h to check for version compatibility.",
                "This probably means that you have ZeroMQ <= 2.0.9",
                "If you see 'undeclared identifier' errors, your ZeroMQ is likely too old.",
                f"This pyzmq requires zeromq >= {sv}",
            ]
        )
        warn(msg)
    except Exception:
        traceback.print_exc()
        msg = "\n".join(
            [
                "Unexpected Error checking for zmq version.",
                "If you see 'undeclared identifier' errors, your ZeroMQ is likely too old.",
                f"This pyzmq requires zeromq >= {sv}",
            ]
        )
        warn(msg)


if __name__ == "__main__":
    v, h = find_zmq_version()
    print(h)
    print(ver_str(v))
