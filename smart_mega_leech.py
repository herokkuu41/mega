import asyncio
import os
import time
import logging
import shutil
import uuid

LOGGER = logging.getLogger(__name__)

class SmartMegaLeecher:
    def __init__(self, download_path, proxies_file="proxies.txt"):
        self.download_path = download_path
        self.proxies = self._load_proxies(proxies_file)
        self.current_proxy_index = 0
        # 1.9 GB in bytes - Safety limit
        self.limit_bytes = 1.9 * 1024 * 1024 * 1024
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
    
    def _create_session_dir(self):
        os.makedirs(self.download_path, exist_ok=True)
        session_dir = os.path.join(
            self.download_path, f"job_{int(time.time())}_{uuid.uuid4().hex[:8]}"
        )
        os.makedirs(session_dir, exist_ok=True)
        return session_dir

    def _resolve_output(self, session_dir, file_path=None):
        if file_path and os.path.exists(file_path):
            return file_path

        entries = os.listdir(session_dir)
        if len(entries) == 1:
            entry_path = os.path.join(session_dir, entries[0])
            if os.path.isfile(entry_path):
                return entry_path
        archive_path = shutil.make_archive(session_dir, "zip", session_dir)
        return archive_path

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
        session_dir = self._create_session_dir()
        baseline_size = self._get_dir_size(session_dir)
        file_path = await self.get_downloading_file_path(mega_link)
        if not file_path:
            LOGGER.warning("Could not fetch file info from Mega. Proceeding without filename.")
        else:
            file_path = os.path.join(session_dir, os.path.basename(file_path))

        LOGGER.info(f"Starting Smart Download for: {mega_link}")

        while not self.is_cancelled:
            current_proxy = self.get_next_proxy()
            
            cmd = ["megadl", "--path", session_dir, mega_link]
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

                if file_path and os.path.exists(file_path):
                    size = os.path.getsize(file_path)
                else:
                    size = self._get_dir_size(session_dir) - baseline_size
                    
                if update_status_func:
                    proxy_label = current_proxy or "direct"
                    await update_status_func(
                        f"Downloading... {size / (1024*1024):.2f} MB\nProxy: {proxy_label}"
                    )

                if size >= self.limit_bytes:
                    LOGGER.info("1.9GB Limit hit. Switching Proxy...")
                    process.kill()
                    limit_reached = True
                    break
                
                try:
                    await asyncio.wait_for(process.wait(), timeout=2)
                except asyncio.TimeoutError:
                    pass
            
            if process.returncode == 0 and not limit_reached:
                output_path = self._resolve_output(session_dir, file_path)
                return True, output_path
            
            if limit_reached:
                LOGGER.info("Resuming with new proxy...")
                await asyncio.sleep(1)
                continue
            
            _, stderr = await process.communicate()
            error_message = stderr.decode().strip() if stderr else "Unknown error"
            return False, f"Download failed: {error_message}"
        
        return False, "Unknown Error"
