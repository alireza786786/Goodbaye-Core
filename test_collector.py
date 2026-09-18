#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""آزمون‌های واحد v4.0 — پارس، rename، الگو، SSRF، نام/هدر."""
import base64
import json

import pytest


def _vmess(host="1.2.3.4", port=443, ps="old"):
    obj = {"v": 2, "ps": ps, "add": host, "port": port, "id": "abc", "aid": 0}
    return f"vmess://{base64.b64encode(json.dumps(obj).encode()).decode()}"


@pytest.fixture
def parser():
    from collector import ConfigParser
    return ConfigParser


def test_parse_vless(parser):
    pc = parser.parse("vless://abc@1.2.3.4:443?type=tcp#old")
    assert pc and pc.host == "1.2.3.4" and pc.port == 443 and pc.scheme == "vless"


def test_parse_vmess_included(parser):
    pc = parser.parse(_vmess())
    assert pc and pc.scheme == "vmess" and pc.host == "1.2.3.4"


def test_parse_invalid(parser):
    assert parser.parse("garbage") is None


def test_ssrf_blocks_private():
    from collector import resolve_safe_ip
    assert resolve_safe_ip("127.0.0.1") is None
    assert resolve_safe_ip("10.0.0.5") is None
    assert resolve_safe_ip("1.1.1.1") == "1.1.1.1"


def test_rename_vmess_ps(parser):
    renamed = parser.rename(_vmess(), "vmess", "NEW_NAME")
    body = renamed[len("vmess://"):]
    obj = json.loads(base64.b64decode(body + "==").decode())
    assert obj["ps"] == "NEW_NAME"


def test_rename_fragment(parser):
    raw = "vless://abc@1.2.3.4:443#OLD"
    renamed = parser.rename(raw, "vless", "نام جدید")
    assert "#OLD" not in renamed and "#" in renamed


@pytest.mark.parametrize("link,expected", [
    # Reality-Vision بدون sid (نمونهٔ واقعی منابع)
    ("vless://abc@2.59.134.226:443?security=reality&encryption=none&pbk=1sSmdw90uTiGuitDvp_q8zcXTcHXKQS3IVjA7T1UNiY&fp=qq&type=tcp&flow=xtls-rprx-vision&sni=rich-falcon.cdn.cachefleet.com",
     "reality-vision"),
    # Reality-gRPC
    ("vless://abc@1.2.3.4:8443?security=reality&encryption=none&pbk=bzbe7QdD633ew_Er1_YWEhuCEHWurmDpxpz3fRzXr0A&fp=ios&type=grpc&serviceName=shiforgrpc&sni=cloudflare.com",
     "reality-grpc"),
    # CF Worker (workers.dev در پارامتر host، آدرس اتصال IP است)
    ("vless://abc@1.2.3.4:443?path=%2Fx&security=tls&host=w1z2qhevu1ps.workers.dev&type=ws&sni=w1z2qhevu1ps.workers.dev",
     "cf-workers"),
    # CF CleanIP (alpn URL-encoded)
    ("vless://abc@104.25.30.10:443?path=%2Fapi&security=tls&alpn=http%2F1.1&host=de14.69lover.my&type=ws&sni=de14.69lover.my",
     "cf-cleanip"),
    # Plain TCP بدون TLS
    ("vless://abc@157.137.226.173:62145?security=&type=tcp",
     "plain"),
])
def test_detect_pattern(link, expected):
    from collector import detect_pattern
    assert detect_pattern(link, "vless") == expected


def test_name_pattern_matches_channel_template():
    from collector import SmartScorer, CFG
    name = SmartScorer.build_name("🇩🇪", "Germany", "Frankfurt am Main", 32, "Reality-Vision")
    assert CFG.CHANNEL_TAG in name
    assert "🇩🇪" in name and "Germany" in name
    assert "ping:32.00ms" in name and "Reality-Vision" in name


def test_header_has_required_lines():
    from collector import build_header, CFG
    h = build_header(10, 3, 5)
    assert "گفتگو و تبادل" in h
    assert CFG.CHAT_GROUP_LINK in h
    assert "آپدیت" in h and "منبع" in h
    assert CFG.TELEGRAM_LINK in h


def test_sources_loads_deduplicated():
    from collector import CFG
    urls = list(CFG.SOURCES)
    assert len(urls) == len(set(urls)), "منابع تکراری نباید باشند"
    assert len(urls) >= 30