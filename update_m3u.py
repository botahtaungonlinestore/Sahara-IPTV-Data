import os
import requests
import pycountry
from concurrent.futures import ThreadPoolExecutor, as_completed

TIMEOUT = 5
MAX_THREADS = 50
OUTPUT_FOLDER = "active_m3u"
EXTRA_FILE = "extra_myanmar.m3u"
IPTV_ORG_API = "https://api.github.com/repos/iptv-org/iptv/contents/streams"

def get_country_name(filename):
    name = os.path.splitext(filename)[0]
    lower_name = name.lower()
    parts = lower_name.split("_", 1)
    country_code = parts[0]
    country = pycountry.countries.get(alpha_2=country_code.upper())
    
    if country:
        country_name = country.name.replace(" ", "_")
        if len(parts) > 1:
            suffix = parts[1]
            return f"{country_name}_{suffix}.m3u"
        else:
            return f"{country_name}.m3u"
    return f"{name}.m3u"

def extract_channel_name(extinf_line):
    if "," in extinf_line:
        channel_name = extinf_line.split(",", 1)[1].strip()
        if channel_name:
            return channel_name
    return "Unknown Channel"

def parse_m3u_text(text):
    channels = []
    lines = text.splitlines()
    current_channel_name = None
    for raw_line in lines:
        line = raw_line.strip()
        if not line: continue
        if line.startswith("#EXTINF"):
            current_channel_name = extract_channel_name(line)
        elif line.startswith("http"):
            channels.append({
                "name": current_channel_name or "Unknown Channel",
                "url": line
            })
            current_channel_name = None
    return channels

def check_link(channel):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"}
    try:
        response = requests.get(channel["url"], headers=headers, timeout=TIMEOUT, stream=True, allow_redirects=True)
        if response.status_code == 200:
            return True
    except:
        pass
    return False

def process_and_save(filename, channels):
    print(f"\n=== Processing: {filename} ===")
    
    unique_channels = []
    seen_urls = set()
    for channel in channels:
        url = channel["url"]
        if url not in seen_urls:
            seen_urls.add(url)
            unique_channels.append(channel)

    active_channels = []
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        future_map = {executor.submit(check_link, ch): ch for ch in unique_channels}
        for future in as_completed(future_map):
            ch = future_map[future]
            try:
                if future.result():
                    active_channels.append(ch)
            except:
                pass

    if not active_channels:
        print(f"No active channels for {filename}. Skipping.")
        return

    output_filename = get_country_name(filename)
    output_path = os.path.join(OUTPUT_FOLDER, output_filename)
    
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("#EXTM3U\n")
        for ch in active_channels:
            f.write(f"#EXTINF:-1,{ch['name']}\n{ch['url']}\n")
    
    print(f"Saved: {output_path} (Active: {len(active_channels)}/{len(unique_channels)})")

def main():
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    
    print("Fetching files from iptv-org...")
    try:
        response = requests.get(IPTV_ORG_API)
        files = response.json()
    except Exception as e:
        print(f"Failed to fetch API: {e}")
        return

    for file_info in files:
        if file_info['name'].endswith('.m3u'):
            filename = file_info['name']
            download_url = file_info['download_url']
            
            try:
                res = requests.get(download_url)
                channels = parse_m3u_text(res.text)
                
                # 🌟 Myanmar ဖိုင်ဆိုရင် အစ်ကို့ extra_myanmar ကိုပါ ပေါင်းထည့်မည်
                if filename.lower() == "mm.m3u" and os.path.exists(EXTRA_FILE):
                    print("Adding extra_myanmar.m3u to Myanmar list...")
                    with open(EXTRA_FILE, 'r', encoding='utf-8', errors='ignore') as ef:
                        extra_channels = parse_m3u_text(ef.read())
                        channels.extend(extra_channels)
                
                process_and_save(filename, channels)
                
            except Exception as e:
                print(f"Failed to process {filename}: {e}")

if __name__ == '__main__':
    main()
