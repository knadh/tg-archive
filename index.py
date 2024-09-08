import os
import json
import re
import shutil
import time
import subprocess
import asyncio
from telethon import TelegramClient
from telethon.tl.types import Channel, Chat

# Load API credentials from .env file
API_ID = os.getenv('API_ID')
API_HASH = os.getenv('API_HASH')
SESSION_ID = os.getenv('SESSION_ID', 'session.session')
SESSION_PATH = f'/session/{SESSION_ID}'
# Path for the cache file
CACHE_FILE = '/data/index.json'

def parse_group_name(name):
    # Remove non-ASCII characters and replace spaces with underscores
    parsed_name = re.sub(r'[^\x00-\x7F]+', '', name)
    parsed_name = re.sub(r'\s+', '_', parsed_name)
    parsed_name = re.sub(r'[^a-zA-Z0-9\-_\.]', '', parsed_name)
    parsed_name = parsed_name.lower()
    return parsed_name.strip('_')

def create_group_directory(group_name, group_id):
    parsed_name = parse_group_name(group_name)
    dir_path = os.path.join('/data', parsed_name)
    os.makedirs(dir_path, exist_ok=True)
    
    # Copy and modify config.yaml
    config_src = '/app/mysite/config.yaml'
    config_dest = os.path.join(dir_path, 'config.yaml')
    with open(config_src, 'r') as src_file, open(config_dest, 'w') as dest_file:
        config_content = src_file.read()
        config_content = config_content.replace('--GROUP-ID--', str(group_id))
        config_content = config_content.replace('--ID--', str(API_ID))
        config_content = config_content.replace('--HASH--', str(API_HASH))
        dest_file.write(config_content)
    
    # Create static directory and copy template.html
    static_dir = os.path.join(dir_path, 'static')
    os.makedirs(static_dir, exist_ok=True)
    template_src = '/app/mysite/template.html'
    template_dest = os.path.join(dir_path, 'template.html')
    shutil.copy(template_src, template_dest)
    
    return dir_path

async def get_groups():
    async with TelegramClient(SESSION_ID, API_ID, API_HASH) as client:
        dialogs = await client.get_dialogs(archived=False)
        groups = []
        print(f"Total dialogs fetched: {len(dialogs)}")
        for d in dialogs:
            if not isinstance(d.entity, (Channel, Chat)):
                continue
            # Check if the group is archived, if so, skip it
            if d.entity.left:
                print(f"Skipping archived group: {d.name}")
                continue
            print(d.name)
            group_dir = create_group_directory(d.name, d.entity.id)
            groups.append({
                'id': d.entity.id,
                'name': d.name,
                'type': 'channel' if isinstance(d.entity, Channel) else 'group',
                'directory': group_dir
            })
        
        return groups

def cache_groups(groups):
    with open(CACHE_FILE, 'w') as f:
        json.dump(groups, f, indent=2)

import time

def load_cached_groups():
    if os.path.exists(CACHE_FILE):
        cache_age = time.time() - os.path.getmtime(CACHE_FILE)
        if cache_age < 86400:  # 86400 seconds = 1 day
            with open(CACHE_FILE, 'r') as f:
                return json.load(f)
    return None

import subprocess

import colorama
import time
import humanize

def bytes_to_human(size):
    return humanize.naturalsize(size, binary=True)

colorama.init(strip=False, autoreset=True)

def run_tg_archive(group_id, group_dir):
    config_path = os.path.join(group_dir, 'config.yaml')
    data_path = os.path.join(group_dir, 'data.sqlite')
    template = os.path.join(group_dir, 'template.html')
    sync_log = os.path.join(group_dir, 'sync.log')
    build_log = os.path.join(group_dir, 'build.log')
    base_command = ['/usr/local/bin/tg-archive', 
                    '--symlink', 
                    '--config', config_path, 
                    '--data', data_path, 
                    '--path', group_dir, 
                    '--session', SESSION_ID,
                    '--rss-template', template
                    ]
    
    sync_command = base_command + ['--sync']
    build_command = base_command + ['--build']
    
    try:
        group_size = os.path.getsize(data_path) if os.path.exists(data_path) else 0
        print(colorama.Fore.CYAN + f"#Processing group {group_id} (Current size: {bytes_to_human(group_size)})" + colorama.Fore.RESET)
        
        print(colorama.Fore.GREEN + f"- Running [sync] for group {group_id}, saving in {group_dir}" + colorama.Fore.RESET)
        print(colorama.Fore.GREEN + sync_command + colorama.Fore.RESET)
        start_time = time.time()
        with open(sync_log, 'w') as log_file:
            process = subprocess.Popen(sync_command, cwd=group_dir, stdout=log_file, stderr=subprocess.STDOUT)
            while process.poll() is None:
                time.sleep(60)  # Wait for 1 minute
                elapsed_time = time.time() - start_time
                print(colorama.Fore.YELLOW + f" - time elapsed: {time.strftime('%H:%M:%S', time.gmtime(elapsed_time))}" + colorama.Fore.RESET)
                dir_size = get_directory_size(group_dir)
                print(colorama.Fore.YELLOW + f" - size: {bytes_to_human(dir_size)}" + colorama.Fore.RESET)
            process.wait()
        if process.returncode == 0:
            print(colorama.Fore.GREEN + f"- Successfully ran tg-archive sync for group {group_id}" + colorama.Fore.RESET)
        else:
            print(colorama.Fore.RED + f"- Error running tg-archive sync for group {group_id}" + colorama.Fore.RESET)
            with open(sync_log, 'r') as log_file:
                log_lines = log_file.readlines()
                last_10_lines = log_lines[-10:]
                print(colorama.Fore.RED + "- Last 10 lines of the sync log:" + colorama.Fore.RESET)
                for line in last_10_lines:
                    print(line.strip())
            return
        
        print(colorama.Fore.GREEN + f"- Running [build] for group {group_id}" + colorama.Fore.RESET)
        print(colorama.Fore.GREEN + build_command + colorama.Fore.RESET)
        start_time = time.time()
        with open(build_log, 'w') as log_file:
            process = subprocess.Popen(build_command, cwd=group_dir, stdout=log_file, stderr=subprocess.STDOUT)
            while process.poll() is None:
                time.sleep(60)  # Wait for 1 minute
                elapsed_time = time.time() - start_time
                print(colorama.Fore.YELLOW + f"Build in progress... Time elapsed: {time.strftime('%H:%M:%S', time.gmtime(elapsed_time))}" + colorama.Fore.RESET)
            process.wait()
        if process.returncode == 0:
            print(colorama.Fore.GREEN + f"- Successfully ran tg-archive build for group {group_id}" + colorama.Fore.RESET)
        else:
            print(colorama.Fore.RED + f"- Error running tg-archive build for group {group_id}" + colorama.Fore.RESET)
            with open(build_log, 'r') as log_file:
                log_lines = log_file.readlines()
                last_10_lines = log_lines[-10:]
                print(colorama.Fore.RED + "- Last 10 lines of the build log:" + colorama.Fore.RESET)
                for line in last_10_lines:
                    print(line.strip())
            return
        
        final_size = os.path.getsize(data_path)
        print(colorama.Fore.CYAN + f"- Finished processing group {group_id} (Final size: {bytes_to_human(final_size)})" + colorama.Fore.RESET)
    except Exception as e:
        print(colorama.Fore.RED + f"- Error running tg-archive for group {group_id}." + colorama.Fore.RESET)
        print(e)

import os
import subprocess

def get_directory_size(path):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size

def process_groups():
    groups = asyncio.get_event_loop().run_until_complete(get_groups())
    cache_groups(groups)
    for group in groups:
        dir_size = get_directory_size(group['directory'])
        print(f"ID: {group['id']}, Name: {group['name']}, Type: {group['type']}, Size: {bytes_to_human(dir_size)}")
        run_tg_archive(group['id'], group['directory'])
        print('debug')
        break
    print(f"\nTotal groups: {len(groups)}")

def run_periodically(interval, func, *args, **kwargs):
    while True:
        func(*args, **kwargs)
        time.sleep(interval)

def gen_session_config():
    config_src = '/app/mysite/config.yaml'
    with open(config_src, 'r') as src_file:
        config_content = src_file.read()
        config_content = config_content.replace('--GROUP-ID--', str(1))
        config_content = config_content.replace('--ID--', str(API_ID))
        config_content = config_content.replace('--HASH--', str(API_HASH))
    return config_content

if __name__ == '__main__':
    import asyncio
    import time
    print(f'SESSION_PATH: {SESSION_PATH}')
    config_path = os.path.join('/session', 'config.yaml')
    config_content = gen_session_config()
    with open(config_path, 'w') as f:
        f.write(config_content)
    if not os.path.exists(SESSION_PATH):
        print(colorama.Fore.RED + f"Session {SESSION_PATH} not found." + colorama.Fore.RESET)
        print(colorama.Fore.RED + f"Please enter the Docker instance to generate a session file." + colorama.Fore.RESET)
        print(colorama.Fore.RED + f"Or copy the session file to the container." + colorama.Fore.RESET)
        print(colorama.Fore.RED + f"Run docker exec -it tg-archive /bin/bash" + colorama.Fore.RESET)
        print(colorama.Fore.RED + f"cd /session && /usr/local/bin/tg-archive --sync" + colorama.Fore.RESET)
        while not os.path.exists(SESSION_PATH):
            time.sleep(10)
    process_groups()
    run_periodically(3600, process_groups)
