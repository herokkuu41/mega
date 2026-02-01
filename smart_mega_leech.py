import asyncio
import os
import time
import logging
import uuid
import shutil

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
        proxies = []
        # Always add 'None' first to ensure we try Direct connection
        proxies.append(None)
        
        if os.path.exists(filepath):
            with open(filepath, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        if "://" not in line:
                            proxies.append(f"http://{line}")
                        else:
                            proxies.append(line)
            LOGGER.info(f"Loaded {len(proxies)-1} proxies. (Direct connection included)")
        else:
            LOGGER.info("No proxies file found. Using Direct connection only.")
            
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

    def _resolve_output(self, session_dir):
        # Recursively find all files in the session directory
        files = []
        for root, _, filenames in os.walk(session_dir):
            for filename in filenames:
                files.append(os.path.join(root, filename))
        
        # Sort files to ensure they upload in order (1.mp4, 2.mp4, etc.)
        return sorted(files)

    async def download(self, mega_link, update_status_func=None):
        session_dir = self._create_session_dir()
        baseline_size = self._get_dir_size(session_dir)
        
        LOGGER.info(f"Starting Folder Download for: {mega_link}")

        # Attempt download loop
        retry_count = 0
        max_retries = len(self.proxies) * 2 

        while not self.is_cancelled:
            current_proxy = self.get_next_proxy()
            proxy_label = current_proxy if current_proxy else "Direct (No Proxy)"
            
            # megadl will download the full folder structure into session_dir
            cmd = ["megadl", "--path", session_dir, mega_link]
            if current_proxy:
                cmd.extend(["--proxy", current_proxy])
            
            LOGGER.info(f"Attempting download via: {proxy_label}")
            
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            limit_reached = False
            
            while True:
                if self.is_cancelled:
                    try: process.kill()
                    except: pass
                    return False, "Cancelled"

                if process.returncode is not None:
                    break

                # Calculate total size of the folder being downloaded
                current_size = self._get_dir_size(session_dir) - baseline_size
                    
                if update_status_func:
                    await update_status_func(
                        f"Downloading Folder...\nSize: {current_size / (1024*1024):.2f} MB\nUsing: {proxy_label}"
                    )

                # Check Mega Bandwidth Limit
                if current_size >= self.limit_bytes:
                    LOGGER.info("Bandwidth limit hit. Switching connection...")
                    try: process.kill()
                    except: pass
                    limit_reached = True
                    break
                
                try:
                    await asyncio.wait_for(process.wait(), timeout=2)
                except asyncio.TimeoutError:
                    pass
            
            # Check result
            if process.returncode == 0 and not limit_reached:
                output_files = self._resolve_output(session_dir)
                if output_files:
                    return True, output_files
                else:
                    return False, "Download finished but folder is empty."
            
            # Handle resume or failure
            if limit_reached:
                LOGGER.info("Resuming with next proxy...")
                await asyncio.sleep(1)
                continue
            
            # If we are here, the process failed
            _, stderr = await process.communicate()
            error_message = stderr.decode().strip() if stderr else "Unknown error"
            
            LOGGER.warning(f"Failed with {proxy_label}: {error_message}")
            
            retry_count += 1
            if retry_count > max_retries:
                 return False, f"All proxies failed. Last error: {error_message}"
            
            await asyncio.sleep(1)
        
        return False, "Unknown Error"
