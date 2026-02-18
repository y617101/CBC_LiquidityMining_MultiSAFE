import os
import json
import requests

CONFIG_PATH = "config.json"

def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def send_telegram(text: str, chat_id: str) -> None:
    token = os.getenv("TG_BOT_TOKEN")
    if not token:
        raise RuntimeError("TG_BOT_TOKEN is not set")

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    r = requests.post(url, json=payload, timeout=30)
    r.raise_for_status()

def main():
    cfg = load_config(CONFIG_PATH)
    safes = cfg.get("safes", [])
    if not safes:
        raise RuntimeError("No safes found in config.json")

    ok = 0
    for s in safes:
        name = s.get("name", "UNKNOWN")
        safe = s.get("safe_address", "")
        chat_id = s.get("telegram_chat_id", "")
        if not safe or not chat_id:
            continue

        msg = (
            "CBC Liquidity Mining — Daily (MULTI TEST)\n"
            "────────────────\n"
            f"NAME: {name}\n"
            f"SAFE: {safe}\n"
            "STATUS: OK"
        )
        send_telegram(msg, chat_id)
        ok += 1

    print(f"sent={ok}")

if __name__ == "__main__":
    main()

print("BOT START TEST")
