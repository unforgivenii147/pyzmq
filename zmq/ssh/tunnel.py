"""Basic ssh tunnel utilities, and convenience functions for tunneling
zeromq connections.
"""

import atexit
import os
import re
import signal
import socket
import sys
import warnings
from getpass import getpass, getuser
from multiprocessing import Process

try:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        import paramiko

        SSHException = paramiko.ssh_exception.SSHException
except ImportError:
    paramiko = None

    class SSHException(Exception):
        pass
else:
    from .forward import forward_tunnel
try:
    import pexpect
except ImportError:
    pexpect = None


class MaxRetryExceeded(Exception):
    pass


def select_random_ports(n):
    ports = []
    sockets = []
    for i in range(n):
        sock = socket.socket()
        sock.bind(("", 0))
        ports.append(sock.getsockname()[1])
        sockets.append(sock)
    for sock in sockets:
        sock.close()
    return ports


_password_pat = re.compile(b"pass(word|phrase)", re.IGNORECASE)


def try_passwordless_ssh(server, keyfile, paramiko=None):
    if paramiko is None:
        paramiko = sys.platform == "win32"
    if not paramiko:
        f = _try_passwordless_openssh
    else:
        f = _try_passwordless_paramiko
    return f(server, keyfile)


def _try_passwordless_openssh(server, keyfile):
    if pexpect is None:
        raise ImportError("pexpect unavailable, use paramiko")
    cmd = "ssh -f " + server
    if keyfile:
        cmd += " -i " + keyfile
    cmd += " exit"
    env = os.environ.copy()
    env.pop("SSH_ASKPASS", None)
    ssh_newkey = "Are you sure you want to continue connecting"
    p = pexpect.spawn(cmd, env=env)
    MAX_RETRY = 10
    for _ in range(MAX_RETRY):
        try:
            i = p.expect([ssh_newkey, _password_pat], timeout=0.1)
            if i == 0:
                raise SSHException("The authenticity of the host can't be established.")
        except pexpect.TIMEOUT:
            continue
        except pexpect.EOF:
            return True
        else:
            return False
    raise MaxRetryExceeded(f"Failed after {MAX_RETRY} attempts")


def _try_passwordless_paramiko(server, keyfile):
    if paramiko is None:
        msg = "Paramiko unavailable, "
        if sys.platform == "win32":
            msg += "Paramiko is required for ssh tunneled connections on Windows."
        else:
            msg += "use OpenSSH."
        raise ImportError(msg)
    username, server, port = _split_server(server)
    client = paramiko.SSHClient()
    known_hosts = os.path.expanduser("~/.ssh/known_hosts")
    try:
        client.load_host_keys(known_hosts)
    except FileNotFoundError:
        pass
    policy_name = os.environ.get("PYZMQ_PARAMIKO_HOST_KEY_POLICY", None)
    if policy_name:
        policy = getattr(paramiko, f"{policy_name}Policy")
        client.set_missing_host_key_policy(policy())
    try:
        client.connect(
            server, port, username=username, key_filename=keyfile, look_for_keys=True
        )
    except paramiko.AuthenticationException:
        return False
    else:
        client.close()
        return True


def tunnel_connection(
    socket, addr, server, keyfile=None, password=None, paramiko=None, timeout=60
):
    new_url, tunnel = open_tunnel(
        addr,
        server,
        keyfile=keyfile,
        password=password,
        paramiko=paramiko,
        timeout=timeout,
    )
    socket.connect(new_url)
    return tunnel


def open_tunnel(addr, server, keyfile=None, password=None, paramiko=None, timeout=60):
    lport = select_random_ports(1)[0]
    transport, addr = addr.split("://")
    ip, rport = addr.split(":")
    rport = int(rport)
    if paramiko is None:
        paramiko = sys.platform == "win32"
    if paramiko:
        tunnelf = paramiko_tunnel
    else:
        tunnelf = openssh_tunnel
    tunnel = tunnelf(
        lport,
        rport,
        server,
        remoteip=ip,
        keyfile=keyfile,
        password=password,
        timeout=timeout,
    )
    return (f"tcp://127.0.0.1:{lport}", tunnel)


def openssh_tunnel(
    lport, rport, server, remoteip="127.0.0.1", keyfile=None, password=None, timeout=60
):
    if pexpect is None:
        raise ImportError("pexpect unavailable, use paramiko_tunnel")
    ssh = "ssh "
    if keyfile:
        ssh += "-i " + keyfile
    if ":" in server:
        server, port = server.split(":")
        ssh += f" -p {port}"
    cmd = f"{ssh} -O check {server}"
    output, exitstatus = pexpect.run(cmd, withexitstatus=True)
    if not exitstatus:
        pid = int(output[output.find(b"(pid=") + 5 : output.find(b")")])
        cmd = f"{ssh} -O forward -L 127.0.0.1:{lport}:{remoteip}:{rport} {server}"
        output, exitstatus = pexpect.run(cmd, withexitstatus=True)
        if not exitstatus:
            atexit.register(_stop_tunnel, cmd.replace("-O forward", "-O cancel", 1))
            return pid
    cmd = f"{ssh} -f -S none -L 127.0.0.1:{lport}:{remoteip}:{rport} {server} sleep {timeout}"
    env = os.environ.copy()
    env.pop("SSH_ASKPASS", None)
    ssh_newkey = "Are you sure you want to continue connecting"
    tunnel = pexpect.spawn(cmd, env=env)
    failed = False
    MAX_RETRY = 10
    for _ in range(MAX_RETRY):
        try:
            i = tunnel.expect([ssh_newkey, _password_pat], timeout=0.1)
            if i == 0:
                raise SSHException("The authenticity of the host can't be established.")
        except pexpect.TIMEOUT:
            continue
        except pexpect.EOF:
            if tunnel.exitstatus:
                print(tunnel.exitstatus)
                print(tunnel.before)
                print(tunnel.after)
                raise RuntimeError(f"tunnel '{cmd}' failed to start")
            else:
                return tunnel.pid
        else:
            if failed:
                print("Password rejected, try again")
                password = None
            if password is None:
                password = getpass(f"{server}'s password: ")
            tunnel.sendline(password)
            failed = True
    raise MaxRetryExceeded(f"Failed after {MAX_RETRY} attempts")


def _stop_tunnel(cmd):
    pexpect.run(cmd)


def _split_server(server):
    if "@" in server:
        username, server = server.split("@", 1)
    else:
        username = getuser()
    if ":" in server:
        server, port = server.split(":")
        port = int(port)
    else:
        port = 22
    return (username, server, port)


def paramiko_tunnel(
    lport, rport, server, remoteip="127.0.0.1", keyfile=None, password=None, timeout=60
):
    if paramiko is None:
        raise ImportError("Paramiko not available")
    if password is None:
        if not _try_passwordless_paramiko(server, keyfile):
            password = getpass(f"{server}'s password: ")
    p = Process(
        target=_paramiko_tunnel,
        args=(lport, rport, server, remoteip),
        kwargs=dict(keyfile=keyfile, password=password),
    )
    p.daemon = True
    p.start()
    return p


def _paramiko_tunnel(lport, rport, server, remoteip, keyfile=None, password=None):
    username, server, port = _split_server(server)
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.WarningPolicy())
    try:
        client.connect(
            server,
            port,
            username=username,
            key_filename=keyfile,
            look_for_keys=True,
            password=password,
        )
    except Exception as e:
        print(f"*** Failed to connect to {server}:{port}: {e!r}")
        sys.exit(1)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    try:
        forward_tunnel(lport, remoteip, rport, client.get_transport())
    except KeyboardInterrupt:
        print("SIGINT: Port forwarding stopped cleanly")
        sys.exit(0)
    except Exception as e:
        print(f"Port forwarding stopped uncleanly: {e}")
        sys.exit(255)


if sys.platform == "win32":
    ssh_tunnel = paramiko_tunnel
else:
    ssh_tunnel = openssh_tunnel
__all__ = [
    "tunnel_connection",
    "ssh_tunnel",
    "openssh_tunnel",
    "paramiko_tunnel",
    "try_passwordless_ssh",
]
