"""Outbound URL policy: stop subscribers from aiming the relay at internal systems (SSRF).

Limitation: the host is resolved here and again by the HTTP client when it connects, so a
DNS-rebinding attacker could still race the check. Pinning the resolved address is the
stricter fix and is listed in the README roadmap.
"""

from __future__ import annotations

import ipaddress
import socket
from urllib.parse import urlsplit


class UnsafeTarget(ValueError):
    """The URL is not an acceptable webhook destination."""


def _blocked(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped is not None:
        ip = ip.ipv4_mapped
    return (not ip.is_global) or ip.is_multicast


def validate_target(url: str, allow_private: bool = False) -> None:
    try:
        parts = urlsplit(url)
        port = parts.port
    except ValueError as exc:
        raise UnsafeTarget(f"invalid URL: {exc}") from None
    if parts.scheme not in ("http", "https"):
        raise UnsafeTarget("only http and https URLs are allowed")
    if not parts.hostname:
        raise UnsafeTarget("URL has no host")
    if parts.username or parts.password:
        raise UnsafeTarget("credentials in the URL are not allowed")
    if allow_private:
        return
    port = port or (443 if parts.scheme == "https" else 80)
    try:
        infos = socket.getaddrinfo(parts.hostname, port, type=socket.SOCK_STREAM)
    except socket.gaierror as exc:
        raise UnsafeTarget(f"cannot resolve host: {exc}") from None
    for info in infos:
        address = ipaddress.ip_address(info[4][0].split("%")[0])
        if _blocked(address):
            raise UnsafeTarget(f"destination {address} is not a public address")
