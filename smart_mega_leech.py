import asyncio
import os
import time
import logging
import shutil

LOGGER = logging.getLogger(__name__)

class SmartMegaLeecher:
    def __init__(self, download_path, proxies_file="proxies.txt"):
        self.download_path = download_path
        self.proxies = self._load_proxies(proxies_file)
        self.current_proxy_index = 0
        # 1.8 GB in bytes - Safety limit
        self.limit_bytes = 1.8 * 1024 * 1024 * 1024 
        self.is_cancelled = False

    def _load_proxies(self, filepath):
        if not os.path.exists(filepath):
            LOGGER.warning(f"Proxy file {filepath} not found!")
            return []
        with open(filepath, 'r') as f:
            proxies = []
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    if "://" not in line:
                        proxies.append(f"http://{line}")
                    else:
                        proxies.append(line)
            return proxies

    def get_next_proxy(self):
        if not self.proxies:
            return None
        proxy = self.proxies[self.current_proxy_index]
        self.current_proxy_index = (self.current_proxy_index + 1) % len(self.proxies)
        return proxy
    
    def _get_dir_size(self, path):
        total_size = 0
        for root, _, files in os.walk(path):
            for filename in files:
                filepath = os.path.join(root, filename)
                if not os.path.islink(filepath):
                    total_size += os.path.getsize(filepath)
        return total_size

    async def get_downloading_file_path(self, mega_link):
        cmd = ["megadl", "--info", mega_link]
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await proc.communicate()
        if proc.returncode != 0:
            return None
        
        try:
            output = stdout.decode().strip()
            for line in output.split('\n'):
                if line.startswith("filename:"):
                    filename = line.split("filename:", 1)[1].strip()
                    return os.path.join(self.download_path, filename)
        except:
            pass
        return None

    async def download(self, mega_link, update_status_func=None):
        if not os.path.exists(self.download_path):
            os.makedirs(self.download_path)

        existing_entries = set(os.listdir(self.download_path))
        baseline_size = self._get_dir_size(self.download_path)
        file_path = await self.get_downloading_file_path(mega_link)
        if not file_path:
            LOGGER.warning("Could not fetch file info from Mega. Proceeding without filename.")

        LOGGER.info(f"Starting Smart Download for: {mega_link}")

        while not self.is_cancelled:
            current_proxy = self.get_next_proxy()
            
            cmd = ["megadl", "--path", self.download_path, mega_link]
            if current_proxy:
                cmd.extend(["--proxy", current_proxy])
                LOGGER.info(f"Using Proxy: {current_proxy}")
            
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            limit_reached = False
            while True:
                if self.is_cancelled:
                    process.kill()
                    return False, "Cancelled"

                if process.returncode is not None:
                    break

                size = None
                if file_path and os.path.exists(file_path):
                    size = os.path.getsize(file_path)
                else:
                    size = self._get_dir_size(self.download_path) - baseline_size
                    
                if update_status_func:
                    await update_status_func(f"Downloading... {size / (1024*1024):.2f} MB\nProxy: {current_proxy}")

                if size >= self.limit_bytes:
                    LOGGER.info("1.8GB Limit hit. Switching Proxy...")
                    process.kill()
                    limit_reached = True
                    break
                
                try:
                    await asyncio.wait_for(process.wait(), timeout=2)
                except asyncio.TimeoutError:
                    pass
            
            if process.returncode == 0 and not limit_reached:
                new_entries = [entry for entry in os.listdir(self.download_path) if entry not in existing_entries]
                if len(new_entries) == 1:
                    entry_path = os.path.join(self.download_path, new_entries[0])
                    if os.path.isdir(entry_path):
                        archive_base = os.path.join(self.download_path, new_entries[0])
                        archive_path = shutil.make_archive(archive_base, "zip", entry_path)
                        return True, archive_path
                    return True, entry_path
                if file_path and os.path.exists(file_path):
                    return True, file_path
                return False, "Download finished but output could not be determined."
            
            if limit_reached or process.returncode != 0:
                LOGGER.info("Resuming with new proxy...")
                await asyncio.sleep(1)
                continue
        
        return False, "Unknown Error"
