import os
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# ============================================================
# SETTINGS
# ============================================================
TIMEOUT = 5
MAX_THREADS = 50
OUTPUT_FOLDER = "active_m3u"
EXTRA_FILE = "extra_myanmar.m3u" # အစ်ကို ကိုယ်တိုင်ထည့်မည့် လိုင်းများ

# iptv-org မှ Myanmar လိုင်းများ ယူမည့် လင့်ခ်အမှန်
TARGET_URLS = [
    "https://raw.githubusercontent.com/iptv-org/iptv/master/streams/mm.m3u"
]

def extract_channel_name(line):
    if "," in line:
        name = line.split(",", 1)[1].strip()
        return name if name else "Unknown Channel"
    return "Unknown Channel"

def fetch_m3u_online(url):
    channels = []
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            lines = resp.text.splitlines()
            current_name = None
            for line in lines:
                line = line.strip()
                if not line: continue
                if line.startswith("#EXTINF"):
                    current_name = extract_channel_name(line)
                elif line.startswith("http"):
                    channels.append({"name": current_name or "Unknown Channel", "url": line})
                    current_name = None
    except Exception as e:
        print(f"Error fetching {url}: {e}")
    return channels

def fetch_m3u_local(filepath):
    channels = []
    if not os.path.exists(filepath):
        return channels
    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        lines = f.readlines()
        current_name = None
        for line in lines:
            line = line.strip()
            if not line: continue
            if line.startswith("#EXTINF"):
                current_name = extract_channel_name(line)
            elif line.startswith("http"):
                channels.append({"name": current_name or "Unknown Channel", "url": line})
                current_name = None
    return channels

def check_link(channel):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"}
    try:
        resp = requests.get(channel["url"], headers=headers, timeout=TIMEOUT, stream=True, allow_redirects=True)
        if resp.status_code == 200:
            return True
    except:
        pass
    return False

def main():
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    all_channels = []

    print("=" * 70)
    print("      SAHARA IPTV - AUTO MERGE & SCANNER")
    print("=" * 70)

    # 1. iptv-org မှ M3U ကို ယူခြင်း
    for url in TARGET_URLS:
        print(f"Fetching from iptv-org: {url}")
        all_channels.extend(fetch_m3u_online(url))

    # 2. Local မှ ကိုယ်ပိုင်လိုင်းများ (extra_myanmar.m3u) ထပ်ပေါင်းခြင်း
    if os.path.exists(EXTRA_FILE):
        print(f"Fetching local extra: {EXTRA_FILE}")
        all_channels.extend(fetch_m3u_local(EXTRA_FILE))
    else:
        print(f"No local extra file found: {EXTRA_FILE}")

    # 3. Duplicate ဖြစ်နေသော URL များ ဖယ်ထုတ်ခြင်း
    unique_channels = []
    seen = set()
    for ch in all_channels:
        if ch["url"] not in seen:
            seen.add(ch["url"])
            unique_channels.append(ch)

    total_channels = len(unique_channels)
    print(f"\nTotal Unique Channels to Check: {total_channels}")

    # 4. အသေ/အရှင် စစ်ဆေးခြင်း
    active_channels = []
    completed = 0

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        future_to_ch = {executor.submit(check_link, ch): ch for ch in unique_channels}
        for future in as_completed(future_to_ch):
            ch = future_to_ch[future]
            try:
                is_active = future.result()
            except:
                is_active = False

            completed += 1
            if is_active:
                active_channels.append(ch)
                print(f"[ACTIVE] {ch['name']}")
            else:
                print(f"[DEAD]   {ch['name']}")

            print(f"Progress: {completed}/{total_channels}", end="\r")

    print("\n")

    # 5. အရှင်များကိုသာ သိမ်းဆည်းခြင်း
    output_path = os.path.join(OUTPUT_FOLDER, "Myanmar.m3u")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("#EXTM3U\n")
        for ch in active_channels:
            f.write(f"#EXTINF:-1,{ch['name']}\n{ch['url']}\n")

    active_count = len(active_channels)
    print("=" * 70)
    print(f"Total Scanned : {total_channels}")
    print(f"Total Active  : {active_count}")
    print(f"Total Dead    : {total_channels - active_count}")
    print(f"Saved to      : {output_path}")
    print("=" * 70)

if __name__ == '__main__':
    main()
