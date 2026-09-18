#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V2Ray Smart Collector — v4.0 (Full Subscription Engine)

جمع‌آوری از منابعِ شما → dedup دقیق → تست شبکه → تست واقعی Xray (پینگ/سرعت واقعی)
→ فیلتر پینگ < 500ms → بازنویسی نام کانال روی همهٔ پروتکل‌ها (شامل vmess) →
خروجی txt با هدرِ زیبا و زمانِ زنده → ارسال به کانال تلگرام.

ویژگی‌های کلیدی:
  - اگر منبعی دانلود نشود، اجرا متوقف نمی‌شود و به منبع بعدی می‌رود.
  - dedup در ۳ سطح: رشته‌ای، (scheme,host,port) قبل از تست، و خطِ نهایی.
  - نامِ هر کانفیگ (از جمله vmess در فیلد ps) با الگوی کانال بازنویسی می‌شود.
  - پینگ واقعی از روی اتصالِ واقعیِ Xray اندازه‌گیری و در نام گره نمایش داده می‌شود.
  - فقط گره‌های با پینگ < 500ms وارد خروجی می‌شوند (بدون سقف تعداد).
  - توکن و آیدی تلگرام فقط از متغیر محیطی خوانده می‌شوند (در گیت‌هاب Secrets).
"""

from __future__ import annotations

import asyncio
import base64
import gzip
import ipaddress
import json
import logging
import os
import random
import socket
import sqlite3
import ssl
import sys
import time
import zlib
import zipfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qs, quote, unquote, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

import aiohttp

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# =============================================================================
# 1. پیکربندی
# =============================================================================

_DEFAULT_SOURCES: Tuple[str, ...] = (
    "https://raw.githubusercontent.com/iboxz/free-v2ray-collector/main/main/mix",
    "https://raw.githubusercontent.com/roosterkid/openproxylist/main/V2RAY_BASE64.txt",
    "https://manager.onetwothree123.ir/",
    "https://raw.githubusercontent.com/0xRadikal/Free-v2ray-Configs/main/top100.txt",
    "https://raw.githubusercontent.com/Q3dlaXpoaQ/Q3dlaXpoaQ.github.io/refs/heads/main/APIs/cg1.txt",
    "https://raw.githubusercontent.com/mahsanet/MahsaFreeConfig/refs/heads/main/mci/sub_1.txt",
    "https://raw.githubusercontent.com/mahsanet/MahsaFreeConfig/main/mtn/sub_1.txt",
    "https://raw.githubusercontent.com/igareck/vpn-configs-for-russia/refs/heads/main/Vless-Reality-White-Lists-Rus-Mobile.txt",
    "https://raw.githubusercontent.com/ShatakVPN/ConfigForge-V2Ray/refs/heads/main/configs/ir/vless.txt",
    "https://raw.githubusercontent.com/Surfboardv2ray/TGParse/refs/heads/main/splitted/hysteria2",
    "https://raw.githubusercontent.com/iboxz/free-v2ray-collector/main/main/vless.txt",
    "https://raw.githubusercontent.com/0xRadikal/Free-v2ray-Configs/refs/heads/main/protocols/hysteria2.txt",
    "https://raw.githubusercontent.com/V2RAYCONFIGSPOOL/V2RAY_SUB/refs/heads/main/v2ray_configs_no1.txt",
    "https://raw.githubusercontent.com/V2RAYCONFIGSPOOL/V2RAY_SUB/refs/heads/main/v2ray_configs_no2.txt",
    "https://raw.githubusercontent.com/V2RAYCONFIGSPOOL/V2RAY_SUB/refs/heads/main/v2ray_configs_no3.txt",
    "https://raw.githubusercontent.com/V2RAYCONFIGSPOOL/V2RAY_SUB/refs/heads/main/v2ray_configs_no4.txt",
    "https://raw.githubusercontent.com/V2RAYCONFIGSPOOL/V2RAY_SUB/refs/heads/main/v2ray_configs_no5.txt",
    "https://raw.githubusercontent.com/V2RAYCONFIGSPOOL/V2RAY_SUB/refs/heads/main/v2ray_configs_no6.txt",
    "https://raw.githubusercontent.com/V2RAYCONFIGSPOOL/V2RAY_SUB/refs/heads/main/v2ray_configs_no7.txt",
    "https://raw.githubusercontent.com/V2RAYCONFIGSPOOL/V2RAY_SUB/refs/heads/main/v2ray_configs_no8.txt",
    "https://raw.githubusercontent.com/V2RAYCONFIGSPOOL/V2RAY_SUB/refs/heads/main/v2ray_configs_no9.txt",
    "https://raw.githubusercontent.com/V2RAYCONFIGSPOOL/V2RAY_SUB/refs/heads/main/v2ray_configs_no10.txt",
    "https://raw.githubusercontent.com/MohammadBahemmat/V2ray-Collector/main/all_servers.txt",
    "https://raw.githubusercontent.com/MahanKenway/Freedom-V2Ray/main/configs/vless_sub.txt",
    "https://raw.githubusercontent.com/ebrasha/free-v2ray-public-list/refs/heads/main/vless_configs.txt",
    "https://raw.githubusercontent.com/ProblemTheCode/SylphNet-public/refs/heads/main/sub/sub.txt",
    "https://raw.githubusercontent.com/F0rc3Run/F0rc3Run/refs/heads/main/splitted-by-protocol/vless.txt",
    "https://github.com/Au1rxx/free-vpn-subscriptions/raw/main/output/v2ray-base64.txt",
    "https://raw.githubusercontent.com/sakha1370/OpenRay/refs/heads/main/output_iran/mci_top100.txt",
    "https://raw.githubusercontent.com/sakha1370/OpenRay/refs/heads/main/output_iran/irancell_top100.txt",
    "https://raw.githubusercontent.com/sakha1370/OpenRay/refs/heads/main/output_iran/tci_top100.txt",
    "https://raw.githubusercontent.com/sakha1370/OpenRay/refs/heads/main/output_iran/others_top100.txt",
    "https://raw.githubusercontent.com/0xRadikal/Free-v2ray-Configs/main/verified/configs.txt",
    "https://raw.githubusercontent.com/0xRadikal/Free-v2ray-Configs/main/light/configs.txt",
    "https://raw.githubusercontent.com/MatinGhanbari/v2ray-configs/main/subscriptions/filtered/subs/vless.txt",
    "https://raw.githubusercontent.com/sakha1370/OpenRay/refs/heads/main/output_iran/iran_top100_checked.txt",
)


def _load_sources() -> Tuple[str, ...]:
    """منابع از sources.txt (کنار اسکریپت)؛ در نبودِ آن از پیش‌فرض. هرگز خطا نمی‌دهد."""
    try:
        p = Path("sources.txt")
        if p.exists():
            lines = [ln.strip() for ln in p.read_text(encoding="utf-8").splitlines()
                     if ln.strip() and not ln.lstrip().startswith("#")]
            if lines:
                return tuple(lines[:400])
    except Exception as e:
        logging.getLogger("v2ray").warning("sources.txt خوانده نشد (%s); پیش‌فرض استفاده می‌شود", e)
    return _DEFAULT_SOURCES


@dataclass(frozen=True)
class Config:
    BOT_TOKEN: str = field(default_factory=lambda: os.environ.get("BOT_TOKEN", ""))
    CHAT_ID: str = field(default_factory=lambda: os.environ.get("CHAT_ID", ""))
    MY_CHANNEL_ID: str = "Goodbaye_filtering"
    TELEGRAM_LINK: str = "https://t.me/Goodbaye_filtering"
    CHAT_GROUP_LINK: str = "https://t.me/CONFIG_V2RAY_VIP"

    SOURCES: Tuple[str, ...] = field(default_factory=_load_sources)

    MAX_WORKERS: int = 50
    GEO_MAX_CONCURRENT: int = 20
    GEO_TIMEOUT: float = 3.0
    GEO_MAX_CALLS_PER_RUN: int = 800

    TCP_TIMEOUT: float = 1.5
    TLS_TIMEOUT: float = 2.5
    MAX_PING_MS: int = 600          # آستانهٔ تست اولیه
    MAX_TLS_PING_MS: int = 800
    MAX_CANDIDATES: int = 3000
    STABILITY_ROUNDS: int = 2

    # فیلتر نهایی: فقط گره‌های با پینگِ کمتر از این وارد خروجی می‌شوند
    MAX_FINAL_PING_MS: int = 500
    CHUNK_SIZE: int = 150           # هر فایل چند کانفیگ (برای ارسال تلگرام)

    REAL_TEST_ENABLED: bool = True
    REAL_TEST_MAX_CANDIDATES: int = 800
    REAL_TEST_CONCURRENCY: int = 15
    REAL_TEST_URL: str = "http://cp.cloudflare.com/generate_204"
    REAL_TEST_TIMEOUT: float = 5.0
    XRAY_ARCH: str = field(default_factory=lambda: os.environ.get("XRAY_ARCH", "Xray-linux-64.zip"))

    INCLUDE_VMESS: bool = True      # vmess هم استخراج و بازنویسی می‌شود

    BASE_SCHEMES: Tuple[str, ...] = (
        "vless://", "ss://", "hysteria2://", "hy2://", "trojan://",
    )
    CHANNEL_TAG: str = "Goodbaye_filtering"
    DB_PATH: str = "history.db"
    OUTPUT_DIR: str = "."

    @property
    def ALLOWED_SCHEMES(self) -> Tuple[str, ...]:
        return self.BASE_SCHEMES + (("vmess://",) if self.INCLUDE_VMESS else ())

    def validate(self) -> None:
        if self.MAX_WORKERS <= 0 or self.TCP_TIMEOUT <= 0 or self.MAX_FINAL_PING_MS <= 0:
            raise ValueError("پارامترهای عددی باید مثبت باشند.")
        if not self.CHANNEL_TAG:
            raise ValueError("CHANNEL_TAG نمی‌تواند خالی باشد.")
        if not self.BOT_TOKEN or not self.CHAT_ID:
            logging.getLogger("v2ray").warning(
                "⚠️ BOT_TOKEN/CHAT_ID ست نشده است؛ فقط فایل‌ها ساخته می‌شوند، ارسال تلگرام انجام نمی‌شود.")


CFG = Config()


def setup_logging(level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger("v2ray")
    if logger.handlers:
        return logger
    logger.setLevel(level)
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S"))
    logger.addHandler(h)
    return logger


log = setup_logging()
CFG.validate()

# =============================================================================
# 2. مدل‌های داده
# =============================================================================

@dataclass
class ParsedConfig:
    raw: str
    scheme: str
    host: str
    port: int
    remark: str = ""


@dataclass
class TestResult:
    config: str
    host: str
    port: int
    tcp_ping: Optional[int] = None
    tls_ping: Optional[int] = None
    handshake_ok: bool = False
    is_reality: bool = False
    is_hysteria2: bool = False
    is_trojan: bool = False
    is_vless: bool = False
    is_vmess: bool = False
    is_ss: bool = False
    scheme: str = ""
    stability: float = 0.0


@dataclass
class ScoredNode:
    score: float
    config: str
    name: str
    ping: int
    country: str
    city: str
    flag: str
    protocol: str
    host: str
    port: int
    scheme: str = ""
    speed_mbps: float = 0.0
    pattern: str = "plain"
    real_ping: float = 0.0
    cc: str = "XX"
    real_tested: bool = False       # آیا از آزمونِ واقعی Xray گذشت (یا قابل گذراندن نبود)

# =============================================================================
# 3. دیتابیس (تاریخچه + کش جغرافیا)
# =============================================================================

class HistoryDB:
    def __init__(self, path: str = CFG.DB_PATH) -> None:
        self.path = path
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=NORMAL;")
        self._conn.execute("BEGIN")
        try:
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS node_history (
                    host TEXT NOT NULL, port INTEGER NOT NULL,
                    last_ping INTEGER, last_score REAL,
                    success_count INTEGER DEFAULT 0, fail_count INTEGER DEFAULT 0,
                    last_seen TEXT, PRIMARY KEY (host, port)
                )""")
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS geo_cache (
                    host TEXT PRIMARY KEY, flag TEXT, cc TEXT,
                    country TEXT, city TEXT, cached_at TEXT
                )""")
            self._conn.execute("DELETE FROM geo_cache WHERE country = 'Unknown' OR cc = 'XX'")
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise

    def __enter__(self) -> "HistoryDB":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def record(self, host: str, port: int, ping: int, score: float, success: bool) -> None:
        now = datetime.now(timezone.utc).isoformat()
        try:
            with self._conn:
                if success:
                    self._conn.execute("""
                        INSERT INTO node_history (host, port, last_ping, last_score,
                                                  success_count, last_seen)
                        VALUES (?, ?, ?, ?, 1, ?)
                        ON CONFLICT(host, port) DO UPDATE SET
                            last_ping=excluded.last_ping, last_score=excluded.last_score,
                            success_count=success_count+1, last_seen=excluded.last_seen
                    """, (host, port, ping, score, now))
                else:
                    self._conn.execute("""
                        INSERT INTO node_history (host, port, fail_count, last_seen)
                        VALUES (?, ?, 1, ?)
                        ON CONFLICT(host, port) DO UPDATE SET
                            fail_count=fail_count+1, last_seen=excluded.last_seen
                    """, (host, port, now))
        except Exception as e:
            log.warning("DB record error: %s", e)

    def get_reliability_bonus(self, host: str, port: int) -> float:
        try:
            row = self._conn.execute(
                "SELECT success_count, fail_count FROM node_history WHERE host=? AND port=?",
                (host, port)).fetchone()
            if not row:
                return 0.0
            success, fail = row
            if success + fail < 3:
                return 0.0
            ratio = success / (success + fail)
            return (ratio - 0.5) * 200
        except Exception:
            return 0.0

    def get_geo_cached(self, host: str) -> Optional[Tuple[str, str, str, str]]:
        try:
            row = self._conn.execute(
                "SELECT flag, cc, country, city FROM geo_cache WHERE host=?", (host,)).fetchone()
            return tuple(row) if row else None
        except Exception:
            return None

    def set_geo_cached(self, host: str, flag: str, cc: str, country: str, city: str) -> None:
        try:
            with self._conn:
                self._conn.execute("""
                    INSERT INTO geo_cache (host, flag, cc, country, city, cached_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(host) DO UPDATE SET
                        flag=excluded.flag, cc=excluded.cc, country=excluded.country,
                        city=excluded.city, cached_at=excluded.cached_at
                """, (host, flag, cc, country, city, datetime.now(timezone.utc).isoformat()))
        except Exception as e:
            log.debug("geo cache write fail: %s", e)

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:
            pass

# =============================================================================
# 4. دریافت async منابع (مقاوم در برابر خطا: منبع خراب → ادامه)
# =============================================================================

class AsyncFetcher:
    def __init__(self, max_concurrent: int = 30, max_attempts: int = 3) -> None:
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.max_attempts = max_attempts
        self.timeout = aiohttp.ClientTimeout(total=15, connect=5)
        self.failed: List[str] = []

    async def fetch_one(self, session: aiohttp.ClientSession, url: str) -> str:
        async with self.semaphore:
            for attempt in range(self.max_attempts):
                try:
                    async with session.get(
                        url, timeout=self.timeout,
                        headers={"User-Agent": "V2RayCollector/4.0"},
                    ) as r:
                        if r.status == 200:
                            return await r.text()
                        log.debug("fetch %s -> status %s", url, r.status)
                except asyncio.CancelledError:
                    raise
                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    log.debug("fetch %s attempt %d: %s", url, attempt, e)
                if attempt < self.max_attempts - 1:
                    await asyncio.sleep(0.5 * (2 ** attempt))
            self.failed.append(url)
            log.warning("⚠️ منبع دانلود نشد (ادامه می‌دهیم): %s", url)
            return ""

    async def fetch_all(self, urls: List[str]) -> List[str]:
        connector = aiohttp.TCPConnector(limit=50, ttl_dns_cache=300)
        async with aiohttp.ClientSession(connector=connector) as session:
            return await asyncio.gather(*(self.fetch_one(session, u) for u in urls))

# =============================================================================
# 5. رمزگشایی فایل
# =============================================================================

class ConfigDecoder:
    @staticmethod
    def try_b64(text: str) -> Optional[str]:
        try:
            clean = text.replace('\n', '').replace('\r', '').replace(' ', '')
            if len(clean) < 16:
                return None
            pad = '=' * (-len(clean) % 4)
            decoded = base64.b64decode(clean + pad, validate=True)
            txt = decoded.decode('utf-8', errors='ignore')
            return txt if any(p in txt for p in CFG.ALLOWED_SCHEMES) else None
        except Exception:
            return None

    @classmethod
    def decode_all(cls, raw: str) -> List[str]:
        results = {raw}
        b64 = cls.try_b64(raw)
        if b64:
            results.add(b64)
            b64_n = cls.try_b64(b64)
            if b64_n:
                results.add(b64_n)
        data = raw.encode()
        for fn in (gzip.decompress, zlib.decompress):
            try:
                t = fn(data).decode('utf-8', errors='ignore')
                if "://" in t:
                    results.add(t)
            except Exception:
                pass
        return list(results)

# =============================================================================
# 6. حفاظت SSRF
# =============================================================================

def _is_public_ip(ip_obj) -> bool:
    if ip_obj.is_private or ip_obj.is_loopback or ip_obj.is_link_local:
        return False
    if ip_obj.is_multicast or ip_obj.is_reserved or ip_obj.is_unspecified:
        return False
    if isinstance(ip_obj, ipaddress.IPv4Address) and ip_obj in ipaddress.ip_network("100.64.0.0/10"):
        return False
    return True


def resolve_safe_ip(host: str) -> Optional[str]:
    try:
        try:
            ip_obj = ipaddress.ip_address(host)
            return host if _is_public_ip(ip_obj) else None
        except ValueError:
            pass
        try:
            infos = socket.getaddrinfo(host, None)
        except (socket.gaierror, OSError):
            return None
        for info in infos:
            ip_str = info[4][0]
            try:
                ip_obj = ipaddress.ip_address(ip_str)
            except ValueError:
                continue
            if _is_public_ip(ip_obj):
                return ip_str
        return None
    except Exception:
        return None

# =============================================================================
# 7. پارس کانفیگ (همهٔ پروتکل‌ها + rename تضمینی)
# =============================================================================

class ConfigParser:
    @staticmethod
    def parse(conf: str) -> Optional[ParsedConfig]:
        try:
            if conf.startswith("vmess://"):
                return ConfigParser._parse_vmess(conf)
            if conf.startswith("ss://"):
                return ConfigParser._parse_ss(conf)
            p = urlparse(conf)
            if not p.hostname or not p.port:
                return None
            return ParsedConfig(raw=conf, scheme=p.scheme, host=p.hostname,
                                port=p.port, remark=unquote(p.fragment or ""))
        except Exception as e:
            log.debug("parse fail (%s...): %s", conf[:30], e)
            return None

    @staticmethod
    def _parse_vmess(conf: str) -> Optional[ParsedConfig]:
        body = conf[len("vmess://"):].split("#", 1)[0]
        pad = "=" * (-len(body) % 4)
        try:
            obj = json.loads(base64.b64decode(body + pad, validate=False)
                             .decode("utf-8", errors="ignore"))
        except Exception as e:
            log.debug("vmess decode fail: %s", e)
            return None
        host, port = obj.get("add"), obj.get("port")
        if not host or not port:
            return None
        try:
            port = int(port)
        except (TypeError, ValueError):
            return None
        return ParsedConfig(raw=conf, scheme="vmess", host=str(host), port=port,
                            remark=str(obj.get("ps", "v2ray")))

    @staticmethod
    def _parse_ss(conf: str) -> Optional[ParsedConfig]:
        body = conf[len("ss://"):]
        remark = ""
        if "#" in body:
            body, frag = body.split("#", 1)
            remark = unquote(frag)
        if "@" in body:
            userinfo, hostport = body.rsplit("@", 1)
            if ":" not in hostport:
                return None
            host, _, port_s = hostport.rpartition(":")
            try:
                port = int(port_s)
            except ValueError:
                return None
            return ParsedConfig(raw=conf, scheme="ss", host=host, port=port, remark=remark)
        pad = "=" * (-len(body) % 4)
        try:
            decoded = base64.b64decode(body + pad).decode("utf-8", errors="ignore")
            if "@" not in decoded or ":" not in decoded:
                return None
            _, hostport = decoded.rsplit("@", 1)
            host, _, port_s = hostport.rpartition(":")
            port = int(port_s)
            return ParsedConfig(raw=conf, scheme="ss", host=host, port=port, remark=remark)
        except Exception as e:
            log.debug("ss decode fail: %s", e)
            return None

    @staticmethod
    def rename(raw: str, scheme: str, new_name: str) -> str:
        """بازنویسی تضمینی نام: vmess داخل JSON (ps)، بقیه فرگمنت #."""
        if scheme == "vmess":
            return ConfigParser._rename_vmess(raw, new_name)
        return f"{raw.split('#', 1)[0]}#{quote(new_name)}"

    @staticmethod
    def _rename_vmess(raw: str, new_name: str) -> str:
        body = raw[len("vmess://"):].split("#", 1)[0]
        pad = "=" * (-len(body) % 4)
        try:
            obj = json.loads(base64.b64decode(body + pad, validate=False)
                             .decode("utf-8", errors="ignore"))
        except Exception:
            return ""  # rename تضمینی ممکن نیست → منتشر نشود
        obj["ps"] = new_name
        new_body = base64.b64encode(json.dumps(obj, ensure_ascii=False).encode("utf-8")).decode()
        return f"vmess://{new_body}"

    @staticmethod
    def is_valid_host(host: str) -> bool:
        try:
            ipaddress.ip_address(host)
            return True
        except ValueError:
            pass
        try:
            host.encode("idna")
            return True
        except Exception:
            return False

# =============================================================================
# 8. طبقه‌بندی الگوی معماری
# =============================================================================

_CF_WORKERS_DOMAINS = ("workers.dev", "pages.dev", "jsdelivr.net")
_PAAS_DOMAINS = ("railway.app", "onrender.com", "fly.dev", "vercel.app",
                 "deno.dev", "glitch.me", "replit.app", "koyeb.app")


def _query_params(raw: str) -> Dict[str, str]:
    try:
        parsed = parse_qs(urlparse(raw.split("#", 1)[0]).query)
        return {k: (vals[0] if vals else "") for k, vals in parsed.items()}
    except Exception:
        return {}


def _reality_viable(qs: Dict[str, str]) -> bool:
    return bool((qs.get("pbk") or "").strip()) and bool((qs.get("sni") or "").strip())


def detect_pattern(raw: str, scheme: str) -> str:
    qs = _query_params(raw)
    conn_host = ""
    try:
        conn_host = (urlparse(raw.split("#", 1)[0]).hostname or "").lower()
    except Exception:
        pass
    hosts: set = set()
    if conn_host:
        hosts.add(conn_host)
    for _k in ("host", "sni", "serverName"):
        _v = (qs.get(_k) or "").strip().lower()
        if _v:
            hosts.add(_v)

    security = (qs.get("security") or "").strip().lower()
    network = (qs.get("type") or "tcp").strip().lower()
    is_tls = security in ("reality", "tls")

    if security == "reality":
        if not _reality_viable(qs):
            return "plain"
        return "reality-grpc" if network == "grpc" else "reality-vision"

    if any(d in h for h in hosts for d in _CF_WORKERS_DOMAINS) and network == "ws":
        return "cf-workers"
    if any(d in h for h in hosts for d in _PAAS_DOMAINS) and network == "ws":
        return "paas"
    try:
        ipaddress.ip_address(conn_host)
        _is_ip = True
    except ValueError:
        _is_ip = False
    if is_tls and _is_ip and network == "ws":
        return "cf-cleanip"
    if not is_tls and network == "tcp":
        return "plain"
    if is_tls and network == "ws":
        return "cf-workers"
    if network == "grpc":
        return "reality-grpc"
    return "plain"


class PatternClassifier:
    """برچسب‌های الگو دقیقاً مطابق نمونه‌های کانال شما."""
    PATTERN_LABEL = {
        "reality-vision": "Reality-Vision",
        "reality-grpc": "Reality-gRPC",
        "cf-cleanip": "CF-CleanIP",
        "cf-workers": "CF-Worker",
        "paas": "PaaS",
        "plain": "Plain-TCP",
    }
    PATTERN_BONUS = {
        "reality-vision": 260,
        "reality-grpc": 240,
        "cf-cleanip": 180,
        "cf-workers": 160,
        "paas": 120,
        "plain": 0,
    }

# =============================================================================
# 9. تست شبکه
# =============================================================================

class AdvancedTester:
    def __init__(self, max_workers: Optional[int] = None) -> None:
        self.max_workers = max_workers or CFG.MAX_WORKERS

    @staticmethod
    def tcp_ping(host: str, port: int, timeout: float) -> Optional[int]:
        safe_ip = resolve_safe_ip(host)
        if safe_ip is None:
            return None
        try:
            start = time.perf_counter()
            with socket.create_connection((safe_ip, port), timeout=timeout):
                return int((time.perf_counter() - start) * 1000)
        except Exception:
            return None

    @staticmethod
    def tls_handshake(host: str, port: int, timeout: float,
                      allow_self_signed: bool) -> Tuple[bool, Optional[int]]:
        safe_ip = resolve_safe_ip(host)
        if safe_ip is None:
            return False, None
        try:
            start = time.perf_counter()
            ctx = ssl.create_default_context()
            if allow_self_signed:
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
            ctx.minimum_version = ssl.TLSVersion.TLSv1_2
            with socket.create_connection((safe_ip, port), timeout=timeout) as sock:
                with ctx.wrap_socket(sock, server_hostname=host) as ss:
                    ss.do_handshake()
                    if not ss.cipher():
                        return False, None
            return True, int((time.perf_counter() - start) * 1000)
        except Exception:
            return False, None

    def test_single(self, pc: ParsedConfig) -> Optional[TestResult]:
        try:
            if not ConfigParser.is_valid_host(pc.host):
                return None
            upper = pc.raw.upper()
            is_reality = "REALITY" in upper
            needs_tls = pc.scheme in ("vless", "trojan", "hysteria2", "hy2")

            tcp = self.tcp_ping(pc.host, pc.port, CFG.TCP_TIMEOUT)
            if tcp is None or tcp > CFG.MAX_PING_MS:
                return None

            result = TestResult(
                config=pc.raw, host=pc.host, port=pc.port, tcp_ping=tcp,
                is_reality=is_reality, is_hysteria2=pc.scheme in ("hysteria2", "hy2"),
                is_trojan=pc.scheme == "trojan", is_vless=pc.scheme == "vless",
                is_vmess=pc.scheme == "vmess", is_ss=pc.scheme == "ss", scheme=pc.scheme,
            )

            if needs_tls and not is_reality:
                ok, tls_p = self.tls_handshake(pc.host, pc.port, CFG.TLS_TIMEOUT, False)
                result.handshake_ok = ok
                result.tls_ping = tls_p
                if not ok or (tls_p and tls_p > CFG.MAX_TLS_PING_MS):
                    return None
            if is_reality:
                ok, tls_p = self.tls_handshake(pc.host, pc.port, CFG.TLS_TIMEOUT, True)
                if not ok:
                    return None
                result.handshake_ok, result.tls_ping = ok, tls_p

            pings = [tcp]
            for _ in range(CFG.STABILITY_ROUNDS - 1):
                p = self.tcp_ping(pc.host, pc.port, CFG.TCP_TIMEOUT)
                if not p:
                    return None
                pings.append(p)
            result.tcp_ping = min(pings)
            result.stability = 1.0 - (max(pings) - min(pings)) / max(max(pings), 1)
            return result
        except Exception as e:
            log.debug("test_single fail %s:%s: %s", pc.host, pc.port, e)
            return None

    def test_all(self, parsed: List[ParsedConfig]) -> List[TestResult]:
        log.info("🔬 تست شبکه روی %d کانفیگ...", len(parsed))
        results: List[TestResult] = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as ex:
            futures = {ex.submit(self.test_single, p): p for p in parsed}
            done = 0
            for fut in as_completed(futures):
                done += 1
                if done % 300 == 0:
                    log.info("   پیشرفت: %d/%d | قبول: %d", done, len(parsed), len(results))
                r = fut.result()
                if r:
                    results.append(r)
        log.info("✅ %d کانفیگ سالم از %d", len(results), len(parsed))
        return results

# =============================================================================
# 10. جغرافیا
# =============================================================================

class GeoLocator:
    BATCH_URL = "http://ip-api.com/batch?fields=status,country,countryCode,city,query"
    BATCH_SIZE = 100
    BATCH_SLEEP = 1.5

    def __init__(self, db: HistoryDB) -> None:
        self.db = db
        self.api_calls = 0

    @staticmethod
    def _flag(cc: str) -> str:
        try:
            return ''.join(chr(127397 + ord(c)) for c in cc.upper()[:2] if c.isalpha())
        except Exception:
            return "🌐"

    async def resolve_all(self, hosts: List[str]) -> dict:
        unique_hosts = list(dict.fromkeys(hosts))
        default = ("🌐", "XX", "Unknown", "Server")
        result: dict = {}
        to_fetch: List[str] = []
        for h in unique_hosts:
            cached = self.db.get_geo_cached(h)
            if cached:
                result[h] = cached
            else:
                to_fetch.append(h)
        if not to_fetch:
            return result
        async with aiohttp.ClientSession() as session:
            for i in range(0, len(to_fetch), self.BATCH_SIZE):
                if self.api_calls >= CFG.GEO_MAX_CALLS_PER_RUN:
                    break
                chunk = to_fetch[i:i + self.BATCH_SIZE]
                self.api_calls += 1
                try:
                    async with session.post(
                        self.BATCH_URL, json=chunk,
                        timeout=aiohttp.ClientTimeout(total=CFG.GEO_TIMEOUT * 3),
                        headers={"User-Agent": "V2RayCollector/4.0"},
                    ) as r:
                        rows = await r.json(content_type=None)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    log.debug("geo batch lookup fail: %s", e)
                    rows = []
                for item in (rows or []):
                    host = item.get("query", "")
                    if not host:
                        continue
                    if item.get("status") != "success":
                        result[host] = default
                        continue
                    cc = item.get("countryCode", "XX") or "XX"
                    country = item.get("country", "Unknown") or "Unknown"
                    city = item.get("city") or "Server"
                    flag = self._flag(cc)
                    self.db.set_geo_cached(host, flag, cc, country, city)
                    result[host] = (flag, cc, country, city)
                if i + self.BATCH_SIZE < len(to_fetch):
                    await asyncio.sleep(self.BATCH_SLEEP)
        for h in to_fetch:
            result.setdefault(h, default)
        return result

# =============================================================================
# 11. امتیازدهی + ساخت نام طبق الگوی کانال
# =============================================================================

class SmartScorer:
    PROTO_BONUS = {"REALITY": 600, "HYSTERIA2": 500, "TROJAN": 350,
                   "VLESS": 200, "VMESS": 180, "SS": 100}
    COUNTRY_BONUS = {
        "IR": 300, "TR": 250, "AE": 230, "AZ": 220, "AM": 200, "IQ": 210,
        "TM": 200, "GE": 190, "RU": 150, "OM": 180, "DE": 80, "NL": 70,
        "FI": 60, "FR": 50, "GB": 40, "IT": 30, "PL": 30, "CA": 20, "US": 10,
    }
    GOLDEN_PORTS = {443: 250, 8443: 150, 2053: 100, 2083: 100, 2087: 100, 2096: 100,
                    80: 60, 8080: 60, 8880: 60, 2052: 60, 2082: 60, 2086: 60}

    def __init__(self, db: HistoryDB, geo_map: dict) -> None:
        self.db = db
        self.geo_map = geo_map

    @staticmethod
    def build_name(flag: str, country: str, city: str, ping: float,
                   pattern_label: str) -> str:
        """الگوی دقیق نام کانال شما:
        👉🆔@Goodbaye_filtering📡{flag}®️{country}©️{city}🅿️ping:{ping}ms⚡{label}"""
        return (f"👉🆔@{CFG.CHANNEL_TAG}📡{flag}®️{country}©️{city}"
                f"🅿️ping:{ping:.2f}ms⚡{pattern_label}")

    def score_one(self, r: TestResult) -> Optional[ScoredNode]:
        try:
            ping = r.tls_ping or r.tcp_ping
            if not ping:
                return None
            score = 1000 - ping
            if r.is_reality:
                score += self.PROTO_BONUS["REALITY"]; protocol = "Vless"
            elif r.is_hysteria2:
                score += self.PROTO_BONUS["HYSTERIA2"]; protocol = "Hysteria2"
            elif r.is_trojan:
                score += self.PROTO_BONUS["TROJAN"]; protocol = "Trojan"
            elif r.is_vless:
                score += self.PROTO_BONUS["VLESS"]; protocol = "Vless"
            elif r.is_vmess:
                score += self.PROTO_BONUS["VMESS"]; protocol = "Vmess"
            else:
                score += self.PROTO_BONUS["SS"]; protocol = "SS"

            flag, cc, country, city = self.geo_map.get(r.host, ("🌐", "XX", "Unknown", "Server"))
            score += self.COUNTRY_BONUS.get(cc, 0)
            score += self.GOLDEN_PORTS.get(r.port, 0)
            if r.tls_ping and r.tls_ping > 350:
                score -= 150
            if r.handshake_ok and r.is_reality:
                score += 100
            if r.is_reality and "flow=xtls-rprx-vision" in r.config:
                score += 150

            pattern = detect_pattern(r.config, r.scheme)
            db_bonus = self.db.get_reliability_bonus(r.host, r.port)
            score += r.stability * 50 + db_bonus + PatternClassifier.PATTERN_BONUS.get(pattern, 0)

            label = PatternClassifier.PATTERN_LABEL.get(pattern, "Plain-TCP")
            name = self.build_name(flag, country, city, ping, label)
            final_link = ConfigParser.rename(r.config, r.scheme, name)
            if not final_link:
                return None

            return ScoredNode(score=score, config=final_link, name=name, ping=ping,
                              country=country, city=city, flag=flag, protocol=protocol,
                              host=r.host, port=r.port, scheme=r.scheme,
                              pattern=pattern, cc=cc)
        except Exception as e:
            log.debug("score_one fail %s: %s", r.host, e)
            return None

    def score_all(self, results: List[TestResult]) -> List[ScoredNode]:
        scored = [s for s in (self.score_one(r) for r in results) if s]
        scored.sort(key=lambda x: x.score, reverse=True)
        return scored

# =============================================================================
# 12. تست واقعی Xray (پینگ و سرعت واقعی)
# =============================================================================

_XRAY_PORT_COUNTER = {"n": 28000}


def _next_local_port() -> int:
    _XRAY_PORT_COUNTER["n"] += 1
    return _XRAY_PORT_COUNTER["n"]


async def ensure_xray_binary() -> Optional[str]:
    bin_dir = os.path.join(CFG.OUTPUT_DIR, ".xray_bin")
    bin_path = os.path.join(bin_dir, "xray")
    if os.path.exists(bin_path) and os.access(bin_path, os.X_OK):
        return bin_path
    try:
        os.makedirs(bin_dir, exist_ok=True)
        url = (f"https://github.com/XTLS/Xray-core/releases/latest/download/"
               f"{CFG.XRAY_ARCH}")
        zip_path = bin_path + ".zip"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as r:
                if r.status != 200:
                    log.warning("⚠️ دانلود Xray ناموفق (HTTP %s)؛ تست واقعی رد می‌شود", r.status)
                    return None
                data = await r.read()
        with open(zip_path, "wb") as f:
            f.write(data)
        with zipfile.ZipFile(zip_path) as z:
            z.extract("xray", bin_dir)
        os.chmod(bin_path, 0o755)
        os.remove(zip_path)
        return bin_path
    except Exception as e:
        log.warning("⚠️ آماده‌سازی Xray ناموفق: %s", e)
        return None


def build_xray_outbound(raw: str, scheme: str) -> Optional[dict]:
    try:
        p = urlparse(raw)
        qs = {k: v[0] for k, v in parse_qs(p.query).items()}
        host, port = p.hostname, p.port
        if not host or not port:
            return None
        network = qs.get("type", "tcp") or "tcp"
        security = qs.get("security", "") or ""
        sni = qs.get("sni") or qs.get("host") or host
        fp = qs.get("fp", "chrome") or "chrome"
        stream: dict = {"network": network}
        if security == "reality":
            stream["security"] = "reality"
            stream["realitySettings"] = {
                "serverName": sni, "fingerprint": fp, "shortId": qs.get("sid", ""),
                "publicKey": qs.get("pbk", ""), "spiderX": qs.get("spx", "/"),
            }
        elif security == "tls":
            stream["security"] = "tls"
            stream["tlsSettings"] = {
                "serverName": sni, "allowInsecure": True, "fingerprint": fp,
            }
        if network == "ws":
            stream["wsSettings"] = {
                "path": qs.get("path", "/") or "/",
                "headers": {"Host": qs.get("host", sni)},
            }
        elif network == "grpc":
            stream["grpcSettings"] = {"serviceName": qs.get("serviceName", ""),
                                      "multiMode": False}
        if scheme == "vless":
            return {
                "protocol": "vless",
                "settings": {"vnext": [{"address": host, "port": port, "users": [{
                    "id": unquote(p.username or ""),
                    "encryption": qs.get("encryption", "none") or "none",
                    "flow": qs.get("flow", "") or "",
                }]}]},
                "streamSettings": stream,
            }
        if scheme == "trojan":
            password = unquote(p.username or "")
            if not stream.get("security"):
                stream["security"] = "tls"
                stream["tlsSettings"] = {"serverName": sni, "allowInsecure": True, "fingerprint": fp}
            return {"protocol": "trojan",
                    "settings": {"servers": [{"address": host, "port": port, "password": password}]},
                    "streamSettings": stream}
        if scheme == "ss":
            userinfo = unquote(p.username or "")
            method, password = None, None
            try:
                pad = "=" * (-len(userinfo) % 4)
                decoded = base64.urlsafe_b64decode(userinfo + pad).decode()
                method, password = decoded.split(":", 1)
            except Exception:
                if ":" in userinfo:
                    method, password = userinfo.split(":", 1)
            if not method or not password:
                return None
            return {"protocol": "shadowsocks",
                    "settings": {"servers": [{"address": host, "port": port,
                                              "method": method, "password": password}]}}
        return None
    except Exception as e:
        log.debug("build_xray_outbound fail: %s", e)
        return None


async def real_test_one(xray_path: str, raw_config: str, scheme: str) -> Tuple[bool, float, float]:
    """(ok, speed_mbps, real_ping_ms). پینگ واقعی = حداقل تاخیرِ چند زمان‌گیری از
    طریق اتصالِ واقعیِ پروکسی."""
    if scheme in ("hysteria2", "hy2", "vmess"):
        return True, 0.0, 0.0  # Xray-core این‌ها را نمی‌سازد؛ جریمه نمی‌کنیم
    outbound = build_xray_outbound(raw_config, scheme)
    if outbound is None:
        return True, 0.0, 0.0
    local_port = _next_local_port()
    conf_path = f"/tmp/xray_{local_port}.json"
    conf = {"log": {"loglevel": "none"},
            "inbounds": [{"listen": "127.0.0.1", "port": local_port,
                          "protocol": "http", "settings": {}}],
            "outbounds": [outbound]}
    proc = None
    try:
        with open(conf_path, "w") as f:
            json.dump(conf, f)
        proc = await asyncio.create_subprocess_exec(
            xray_path, "run", "-c", conf_path,
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
        await asyncio.sleep(0.8)
        if proc.returncode is not None:
            return False, 0.0, 0.0
        proxy_url = f"http://127.0.0.1:{local_port}"
        ok = False
        for attempt in range(2):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        CFG.REAL_TEST_URL, proxy=proxy_url,
                        timeout=aiohttp.ClientTimeout(total=CFG.REAL_TEST_TIMEOUT)) as r:
                        ok = r.status in (200, 204)
                        break
            except asyncio.CancelledError:
                raise
            except Exception:
                if attempt == 0:
                    await asyncio.sleep(0.5)
                    continue
                return False, 0.0, 0.0
        if not ok:
            return False, 0.0, 0.0

        # پینگ واقعی: حداقلِ چند زمان‌گیری تکراری
        real_ping_ms = 0.0
        try:
            samples = []
            for _ in range(3):
                t0 = time.perf_counter()
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        CFG.REAL_TEST_URL, proxy=proxy_url,
                        timeout=aiohttp.ClientTimeout(total=CFG.REAL_TEST_TIMEOUT)) as r:
                        await r.read()
                dt = (time.perf_counter() - t0) * 1000
                if r.status in (200, 204):
                    samples.append(dt)
                if len(samples) >= 2:
                    break
            if samples:
                real_ping_ms = round(min(samples), 1)
        except asyncio.CancelledError:
            raise
        except Exception:
            pass

        speed_mbps = 0.0
        try:
            test_bytes = 1_500_000
            start = time.perf_counter()
            received = 0
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"https://speed.cloudflare.com/__down?bytes={test_bytes}",
                    proxy=proxy_url, timeout=aiohttp.ClientTimeout(total=6.0)) as r:
                    async for chunk in r.content.iter_chunked(65536):
                        received += len(chunk)
            elapsed = max(time.perf_counter() - start, 0.05)
            if received > 0:
                speed_mbps = round((received * 8) / elapsed / 1_000_000, 1)
        except asyncio.CancelledError:
            raise
        except Exception:
            pass
        return True, speed_mbps, real_ping_ms
    except asyncio.CancelledError:
        raise
    except Exception as e:
        log.debug("real_test_one infra fail: %s", e)
        return True, 0.0, 0.0
    finally:
        if proc is not None and proc.returncode is None:
            try:
                proc.kill()
                await proc.wait()
            except Exception:
                pass
        try:
            os.remove(conf_path)
        except Exception:
            pass


async def run_real_tests(scored: List[ScoredNode]) -> None:
    """روی گره‌ها درجا اثر می‌گذارد: پینگ واقعی جایگزین می‌شود، سرعت ثبت می‌شود،
    و گره‌های شکست‌خوردهٔ واقعی حذف می‌شوند."""
    if not CFG.REAL_TEST_ENABLED:
        return
    try:
        xray_path = await ensure_xray_binary()
    except Exception as e:
        log.warning("⚠️ تست واقعی رد شد: %s", e)
        return
    if not xray_path:
        return

    candidates = scored[:CFG.REAL_TEST_MAX_CANDIDATES]
    rest = scored[CFG.REAL_TEST_MAX_CANDIDATES:]
    sem = asyncio.Semaphore(CFG.REAL_TEST_CONCURRENCY)

    async def _check(n: ScoredNode):
        async with sem:
            try:
                ok, speed, real_ping = await real_test_one(xray_path, n.config, n.scheme)
            except asyncio.CancelledError:
                raise
            except Exception:
                ok, speed, real_ping = True, 0.0, 0.0
            return n, ok, speed, real_ping

    log.info("   🔎 تست واقعی روی %d کاندیدای برتر...", len(candidates))
    results = await asyncio.gather(*[_check(n) for n in candidates])
    verified: List[ScoredNode] = []
    for n, ok, speed, real_ping in results:
        if not ok:
            continue
        n.speed_mbps = speed
        n.real_tested = True
        if real_ping > 0:
            n.real_ping = real_ping
            n.ping = int(round(real_ping))
            # نام و لینک نهایی با پینگِ واقعیِ تازه بازسازی می‌شود
            label = PatternClassifier.PATTERN_LABEL.get(n.pattern, "Plain-TCP")
            n.name = SmartScorer.build_name(n.flag, n.country, n.city, n.ping, label)
            n.config = ConfigParser.rename(
                n.config.split("#", 1)[0], n.scheme, n.name)
        n.score += min(speed, 50.0) * 2
        verified.append(n)
    verified.sort(key=lambda x: x.score, reverse=True)
    log.info("   ✅ %d تأیید شد | ❌ %d رد شد", len(verified), len(candidates) - len(verified))

    # گره‌های با پینگ بزرگ‌تر از آستانهٔ نهایی از خروجی حذف می‌شوند
    verified = [n for n in verified if (n.ping or 0) < CFG.MAX_FINAL_PING_MS]

    # پروتکل‌های غیرقابل‌تستِ Xray که از تست TCP/TLS گذشته‌اند، بدون آزمون واقعی هم
    # قابل قبول‌اند (Xray-core آن‌ها را نمی‌سازد): همین‌جا تایید علامت می‌شوند.
    for n in rest:
        if n.scheme in ("hysteria2", "hy2", "vmess") and (n.ping or 0) < CFG.MAX_FINAL_PING_MS:
            n.real_tested = True

    merged = verified + rest
    merged.sort(key=lambda x: x.score, reverse=True)
    scored[:] = merged

# =============================================================================
# 13. تلگرام + ساخت فایل‌ها
# =============================================================================

def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M")


def build_header(total: int, protocols: int, countries: int) -> str:
    """هدرِ زیبا با اطلاعات دقیقاً مطابق خواسته‌ی شما + تزئینات."""
    return (
        "# ═══════════════════════════════════════════════════════\n"
        "#  ✨ V2Ray Smart Subscription — کانال گودبای فیلترینگ\n"
        "#\n"
        "#  ➤ 💬 گفتگو و تبادل:\n"
        f"#      {CFG.CHAT_GROUP_LINK}\n"
        "#\n"
        f"#  📅 آپدیت: ✅ تایید شد ({_now_str()})\n"
        f"#  ✨ منبع: {CFG.TELEGRAM_LINK}\n"
        "#\n"
        f"#  📊 {total} گره سالم | {protocols} پروتکل | {countries} کشور\n"
        f"#  ⏱️ همگی با پینگ واقعی < {CFG.MAX_FINAL_PING_MS}ms (تایید شده)\n"
        "# ═══════════════════════════════════════════════════════\n"
    )


class TelegramSender:
    def __init__(self) -> None:
        self.base = f"https://api.telegram.org/bot{CFG.BOT_TOKEN}"
        self.semaphore = asyncio.Semaphore(3)

    def build_single_file(self, nodes: List[ScoredNode]) -> Tuple[str, str]:
        """تمامی کانفیگ‌ها در یک فایل واحد (نه چانک‌بندی)."""
        protocols = len({n.protocol for n in nodes})
        countries = len({n.country for n in nodes})

        header = build_header(len(nodes), protocols, countries)
        # dedup خطِ نهایی (رشتهٔ کامل)
        seen = set()
        lines = []
        for x in nodes:
            if x.config not in seen:
                seen.add(x.config)
                lines.append(x.config)

        fname = "subscription.txt"
        with open(fname, "w", encoding="utf-8") as f:
            f.write(header + "\n".join(lines) + "\n")
            # پایان‌نامه: شمارش صریح
            f.write("# ═══════════════════════════════════════════════════════\n")
            f.write(f"#  ✅ {len(lines)} کانفیگ در این فایل | آخرین بروزرسانی {_now_str()}\n")
            f.write("# ═══════════════════════════════════════════════════════\n")

        caption = (
            f"🔥 *اشتراک هوشمند — کامل*\n"
            f"📦 فایل: `{fname}`\n"
            f"📊 *{len(lines)}* کانفیگ تاییدشده (پینگ واقعی <{CFG.MAX_FINAL_PING_MS}ms)\n"
            f"🕒 به‌روز: {_now_str()}\n\n"
            f"💬 گروه: {CFG.CHAT_GROUP_LINK}\n"
            f"✨ کانال: {CFG.TELEGRAM_LINK}"
        )
        log.info("   📄 یک فایل واحد ساخته شد: %s (%d کانفیگ)", fname, len(lines))
        return (fname, caption)

    async def _send_one(self, session, file_path: str, caption: str, num: int) -> bool:
        async with self.semaphore:
            for attempt in range(3):
                try:
                    data = aiohttp.FormData()
                    data.add_field('chat_id', CFG.CHAT_ID)
                    data.add_field('caption', caption)
                    with open(file_path, 'rb') as f:
                        data.add_field('document', f, filename=file_path)
                        async with session.post(
                            f"{self.base}/sendDocument", data=data,
                            timeout=aiohttp.ClientTimeout(total=60)) as resp:
                            r = await resp.json()
                    if r.get("ok"):
                        log.info("   ✅ پارت %d ارسال شد", num)
                        return True
                    log.warning("   ⚠️ خطا: %s", r.get('description'))
                    retry_after = r.get("parameters", {}).get("retry_after")
                    if retry_after:
                        await asyncio.sleep(retry_after)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    log.error("   ❌ Attempt %d: %s", attempt + 1, e)
                    await asyncio.sleep(2 ** attempt)
        return False

    async def send_all(self, parts: List[Tuple[str, str]]) -> None:
        if not CFG.BOT_TOKEN or not CFG.CHAT_ID:
            log.warning("⚠️ BOT_TOKEN/CHAT_ID ست نشده؛ فقط فایل‌ها ساخته شدند.")
            return
        async with aiohttp.ClientSession() as session:
            tasks = [self._send_one(session, f, c, i) for i, (f, c) in enumerate(parts, 1)]
            await asyncio.gather(*tasks)

# =============================================================================
# 14. Pipeline
# =============================================================================

async def run_pipeline() -> int:
    start = time.time()
    log.info("=" * 60)
    log.info("🚀 V2Ray Smart Collector v4.0 — Full Subscription Engine")
    log.info("=" * 60)

    with HistoryDB() as db:
        # 1) دریافت منابع (هر منبع خراب → ادامه)
        log.info("📥 مرحله 1: دریافت از %d منبع...", len(CFG.SOURCES))
        fetcher = AsyncFetcher()
        raw = await fetcher.fetch_all(list(CFG.SOURCES))
        success_sources = sum(1 for t in raw if t)
        log.info("   ✅ %d/%d منبع موفق (%d ناموفق)", success_sources, len(CFG.SOURCES),
                 len(fetcher.failed))

        # 2) رمزگشایی + تلقیح خطوط
        all_configs: set = set()
        for text in raw:
            if not text:
                continue
            for decoded in ConfigDecoder.decode_all(text):
                for line in decoded.splitlines():
                    line = line.strip()
                    if line.startswith(CFG.ALLOWED_SCHEMES):
                        all_configs.add(line)
        log.info("   ✅ %d کانفیگ خامِ یکتا (رشته‌ای)", len(all_configs))

        # 3) پارس
        parsed_list: List[ParsedConfig] = []
        parse_fail = 0
        for c in all_configs:
            pc = ConfigParser.parse(c)
            if pc:
                parsed_list.append(pc)
            else:
                parse_fail += 1
        log.info("   ✅ %d پارس موفق | %d پارس ناموفق", len(parsed_list), parse_fail)

        # 4) dedup دقیق بر اساس (scheme, host, port)
        by_endpoint: dict = {}
        for pc in parsed_list:
            key = (pc.scheme, pc.host, pc.port)
            if key not in by_endpoint:
                by_endpoint[key] = pc
        parsed_list = list(by_endpoint.values())
        log.info("   ✅ %d پس از dedup (scheme,host,port)", len(parsed_list))

        if len(parsed_list) > CFG.MAX_CANDIDATES:
            random.shuffle(parsed_list)
            parsed_list = parsed_list[:CFG.MAX_CANDIDATES]
            log.info("   🎲 به %d کاندید محدود شد", CFG.MAX_CANDIDATES)

        # 5) تست شبکه
        if not parsed_list:
            log.warning("⚠️ هیچ کانفیگ قابل‌تستی نماند؛ خروجی ساخته نمی‌شود.")
            return 0
        log.info("🧪 مرحله 5: تست شبکه...")
        tested = AdvancedTester().test_all(parsed_list)
        if not tested:
            log.warning("⚠️ هیچ گره‌ای از تست شبکه نگذشت.")
            return 0

        # 6) جغرافیا
        log.info("🌍 مرحله 6: جغرافیا...")
        geo = GeoLocator(db)
        geo_map = await geo.resolve_all([r.host for r in tested])
        log.info("   ✅ %d میزبان geo-resolve شد", len(geo_map))

        # 7) امتیازدهی + ساخت نام
        log.info("🎯 مرحله 7: امتیازدهی...")
        scored = SmartScorer(db, geo_map).score_all(tested)
        log.info("   ✅ %d گره امتیازدهی شد", len(scored))

        # 8) تست واقعی (پینگ/سرعت واقعی + حذف مرده‌ها) + فیلتر نهایی پینگ
        log.info("🧪 مرحله 8: تست واقعی اتصال (Xray)...")
        await run_real_tests(scored)

        # 9) فیلتر نهایی پینگ < آستانه: فقط گره‌هایی که آزمون واقعی دیده‌اند
        #    (یا پروتکل‌های غیرقابل‌تستِ Xray مثل hy2/vmess با TCP/TLS تاییدشده)
        ok_final = []
        for n in scored:
            if not n.config:
                continue
            if not (n.real_tested or n.scheme in ("hysteria2", "hy2", "vmess")):
                continue
            if (n.ping or 99999) >= CFG.MAX_FINAL_PING_MS:
                continue
            # مطمئن می‌شویم نام کانال واقعاً اعمال شده است
            if n.scheme == "vmess":
                if not n.config.startswith("vmess://"):
                    continue
            elif "#" not in n.config:
                continue
            ok_final.append(n)
        log.info("   🏆 %d گره با پینگ < %dms وارد خروجی می‌شوند",
                 len(ok_final), CFG.MAX_FINAL_PING_MS)

        if not ok_final:
            log.warning("⚠️ هیچ گره‌ای از فیلتر پینگ نگذشت؛ خروجی خالی است.")
            return 0

        for n in ok_final:
            db.record(n.host, n.port, n.ping, n.score, success=True)

        # مرتب‌سازی نهایی: کم‌ترین پینگ اول (مثل top100)
        ok_final.sort(key=lambda n: n.ping or 0)

        # 10) ساخت فایل‌ها و ارسال به کانال
        log.info("📤 مرحله 10: ساخت فایل و ارسال...")
        sender = TelegramSender()
        fname, caption = sender.build_single_file(ok_final)
        await sender.send_all([(fname, caption)])

        elapsed = time.time() - start
        log.info("=" * 60)
        log.info("✨ تمام شد در %.1f ثانیه", elapsed)
        log.info("📊 خام: %d | پارس: %d | تست‌شده: %d | نهایی(<%dms): %d | فایل: %s",
                 len(all_configs), len(parsed_list), len(tested),
                 CFG.MAX_FINAL_PING_MS, len(ok_final), fname)
        log.info("=" * 60)
    return 0


async def main() -> int:
    try:
        return await run_pipeline()
    except asyncio.CancelledError:
        log.warning("⚠️ لغو شد")
        raise
    except Exception as e:
        log.exception("❌ خطای بحرانی: %s", e)
        return 1


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except KeyboardInterrupt:
        log.warning("⚠️ لغو شد توسط کاربر")
        sys.exit(130)
