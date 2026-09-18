#!/data/data/com.termux/files/usr/bin/env bash
# ============================================================
#  V2Ray Collector — میان‌بر اجرا در Termux (arm64) و PC
#  استفاده:  run.sh
#  به‌صورت خودکار معماری (arm64/x86_64) را تشخیص می‌دهد و باینری
#  درست Xray را برای تست واقعی دانلود می‌کند؛ اگر ارسال تلگرام
#  خواستید، قبلش BOT_TOKEN و CHAT_ID را ست کنید.
# ============================================================
set -euo pipefail

cd "$(dirname "$0")"

# تشخیص معماری: Termux روی گوشی arm64 -> باینری arm؛ بقیه amd64
case "$(uname -m)" in
  aarch64|arm64)  XRAY_ARCH="${XRAY_ARCH:-Xray-linux-arm64-v8a.zip}" ;;
  *)              XRAY_ARCH="${XRAY_ARCH:-Xray-linux-64.zip}" ;;
esac
export XRAY_ARCH

# نصب خودکار وابستگی‌ها در اولین اجرا
command -v python3 >/dev/null 2>&1 || { echo "❌ python نصب نیست: pkg install python"; exit 1; }
python3 -c "import aiohttp, dotenv" 2>/dev/null || pip install -q aiohttp python-dotenv

echo "🌐 معماری باینری Xray: $XRAY_ARCH"
exec python3 collector.py "$@"