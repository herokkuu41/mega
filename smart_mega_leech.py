import asyncio
import os
import time
import logging

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

        file_path = await self.get_downloading_file_path(mega_link)
        if not file_path:
            LOGGER.error("Could not fetch file info from Mega.")
            return False, "Invalid Link"

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

                if os.path.exists(file_path):
                    size = os.path.getsize(file_path)
                    
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
                return True, file_path
            
            if limit_reached or process.returncode != 0:
                LOGGER.info("Resuming with new proxy...")
                await asyncio.sleep(1)
                continue
        
        return False, "Unknown Error"
