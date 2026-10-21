import os
import requests
import pycountry
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ============================================================
# SETTINGS
# ============================================================
TIMEOUT = 5
MAX_THREADS = 50
OUTPUT_FOLDER = "active_m3u"
EXTRA_FILE_MYANMAR = "extra_myanmar.m3u"

# ✨ 1. GitHub API များ (အစ်ကိုပေးသော လင့်ခ်များကို API အဖြစ် ပြောင်းထားပါသည်)
GITHUB_APIS = [
    "https://api.github.com/repos/iptv-org/iptv/contents/streams",
    "https://api.github.com/repos/YueChan/Live/contents",
    "https://api.github.com/repos/qiangi/m3u/contents",
    "https://api.github.com/repos/youshandefeiyang/IPTV-URL-1/contents",
    "https://api.github.com/repos/youshandefeiyang/IPTV-URL/contents"
]

# ✨ 2. တိုက်ရိုက် Direct M3U လင့်ခ်များ
CUSTOM_ONLINE_PLAYLISTS = {
    "Migu_Playlist.m3u": "https://migu.yang-1989.xyz/playlist.m3u"
}

# ✨ Expert ၏ အဆင့်မြင့် Network Connection စနစ်
_THREAD_LOCAL = threading.local()

def get_session():
    session = getattr(_THREAD_LOCAL, "session", None)
    if session is None:
        session = requests.Session()
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
            "Accept": "*/*",
            "Connection": "keep-alive",
        })
        retries = Retry(
            total=2,
            backoff_factor=0.35,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"]
        )
        adapter = HTTPAdapter(max_retries=retries, pool_connections=8, pool_maxsize=8)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        _THREAD_LOCAL.session = session
    return session

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
    return f"{name.title()}.m3u"

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
    try:
        session = get_session()
        response = session.get(channel["url"], timeout=TIMEOUT, stream=True, allow_redirects=True)
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
    
    # 1. GitHub APIs များမှ M3U များကို အကုန်ဆွဲမည်
    for api_url in GITHUB_APIS:
        print(f"\nFetching files from GitHub API: {api_url}")
        try:
            session = get_session()
            response = session.get(api_url, timeout=15)
            files = response.json()
            
            if isinstance(files, list):
                for file_info in files:
                    if isinstance(file_info, dict) and (file_info.get('name', '').lower().endswith('.m3u') or file_info.get('name', '').lower().endswith('.m3u8')):
                        filename = file_info['name']
                        download_url = file_info['download_url']
                        
                        try:
                            res = get_session().get(download_url, timeout=15)
                            channels = parse_m3u_text(res.text)
                            
                            # Myanmar ဆိုလျှင် extra_myanmar ကိုပါ ပေါင်းထည့်မည်
                            if ("mm" in filename.lower() or "myanmar" in filename.lower()) and os.path.exists(EXTRA_FILE_MYANMAR):
                                print(f"Merging {EXTRA_FILE_MYANMAR} into Myanmar list...")
                                with open(EXTRA_FILE_MYANMAR, 'r', encoding='utf-8', errors='ignore') as ef:
                                    extra_channels = parse_m3u_text(ef.read())
                                    channels.extend(extra_channels)
                            
                            process_and_save(filename, channels)
                        except Exception as e:
                            print(f"Failed to process {filename}: {e}")
            else:
                print(f"API Response is not a list for {api_url}")
                
        except Exception as e:
            print(f"Failed to fetch API {api_url}: {e}")

    # 2. Local မှ အခြား extra_*.m3u ဖိုင်များကို စီမံခြင်း
    for local_file in os.listdir('.'):
        if local_file.startswith('extra_') and local_file.endswith('.m3u') and local_file != EXTRA_FILE_MYANMAR:
            cat_filename = local_file.replace('extra_', '') 
            print(f"Processing local custom category: {local_file}")
            with open(local_file, 'r', encoding='utf-8', errors='ignore') as ef:
                channels = parse_m3u_text(ef.read())
                process_and_save(cat_filename, channels)

    # 3. တိုက်ရိုက်လင့်ခ် (CUSTOM_ONLINE_PLAYLISTS) များကို စီမံခြင်း
    for filename, download_url in CUSTOM_ONLINE_PLAYLISTS.items():
        print(f"\nFetching custom online playlist: {filename}")
        try:
            res = get_session().get(download_url, timeout=15)
            if res.status_code == 200:
                channels = parse_m3u_text(res.text)
                process_and_save(filename, channels)
            else:
                print(f"Failed to fetch {download_url} (HTTP {res.status_code})")
        except Exception as e:
            print(f"Failed to process custom playlist {filename}: {e}")

if __name__ == '__main__':
    main()
