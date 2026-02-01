import asyncio
import os
import time
import logging
import uuid
import re
from datetime import datetime, timedelta
from motor.motor_asyncio import AsyncIOMotorClient

LOGGER = logging.getLogger(__name__)

class ProxyManager:
    def __init__(self, proxies, limit_bytes=1.8 * 1024**3):
        self.proxies = proxies  # List of proxy strings (including None for Direct)
        self.limit_bytes = limit_bytes
        self.mongo_url = os.environ.get("MONGO_URL")
        self.collection = None
        if self.mongo_url:
            try:
                client = AsyncIOMotorClient(self.mongo_url)
                self.collection = client.mega_leech_v2.proxy_usage
                LOGGER.info("Connected to MongoDB for Proxy Tracking.")
            except Exception as e:
                LOGGER.error(f"Failed to connect to MongoDB: {e}")

    async def get_best_proxy(self):
        # If no DB, just rotate simply (fallback)
        if not self.collection:
            if not self.proxies: return None
            # Simple round-robin or random could go here, but we default to first available
            return self.proxies[0] 

        now = datetime.utcnow()
        
        # Check all proxies to find one that is available
        for proxy in self.proxies:
            proxy_key = proxy if proxy else "DIRECT"
            
            doc = await self.collection.find_one({"_id": proxy_key})
            
            if not doc:
                return proxy # Fresh proxy, never used
            
            last_updated = doc.get("last_updated", datetime.min)
            used_bytes = doc.get("used_bytes", 0)
            
            # Check if 12 hours have passed
            if (now - last_updated) > timedelta(hours=12):
                # Reset usage in DB
                await self.collection.update_one(
                    {"_id": proxy_key},
                    {"$set": {"used_bytes": 0, "last_updated": now}}
                )
                return proxy
            
            # Check bandwidth limit
            if used_bytes < self.limit_bytes:
                return proxy
            
        return None # No proxies available

    async def update_usage(self, proxy, bytes_downloaded):
        if not self.collection: return
        
        proxy_key = proxy if proxy else "DIRECT"
        await self.collection.update_one(
            {"_id": proxy_key},
            {
                "$inc": {"used_bytes": bytes_downloaded},
                "$set": {"last_updated": datetime.utcnow()}
            },
            upsert=True
        )

class SmartMegaLeecher:
    def __init__(self, download_path, proxies_file="proxies.txt"):
        self.download_path = download_path
        self.proxies_list = self._load_proxies(proxies_file)
        self.proxy_manager = ProxyManager(self.proxies_list)
        self.is_cancelled = False

    def _load_proxies(self, filepath):
        proxies = [None] # Start with Direct
        if os.path.exists(filepath):
            with open(filepath, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        if "://" not in line:
                            proxies.append(f"http://{line}")
                        else:
                            proxies.append(line)
        return proxies
    
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

    async def _get_mega_file_order(self, mega_link):
        """Runs megadl --info to get the exact order of files in the folder."""
        cmd = ["megadl", "--info", mega_link]
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await proc.communicate()
        
        ordered_names = []
        if proc.returncode == 0:
            try:
                output = stdout.decode().strip()
                for line in output.split('\n'):
                    # Output format usually: filename: /Root/Folder/file.mp4
                    if line.strip().startswith("filename:"):
                        full_path = line.split("filename:", 1)[1].strip()
                        # We only care about the filename to match with disk
                        filename = os.path.basename(full_path)
                        ordered_names.append(filename)
            except Exception as e:
                LOGGER.error(f"Error parsing order: {e}")
        return ordered_names

    def _resolve_output_ordered(self, session_dir, ordered_names):
        """Matches downloaded files on disk to the original Mega order."""
        found_files_map = {}
        extra_files = []

        # Scan directory
        for root, _, filenames in os.walk(session_dir):
            for filename in filenames:
                full_path = os.path.join(root, filename)
                found_files_map[filename] = full_path
                extra_files.append(full_path)

        final_list = []
        
        # 1. Add files in the order Mega listed them
        for name in ordered_names:
            if name in found_files_map:
                final_list.append(found_files_map[name])
                # Remove from map so we don't add it again
                if found_files_map[name] in extra_files:
                    extra_files.remove(found_files_map[name])
        
        # 2. Add any remaining files (alphabetically) that weren't in the info list for some reason
        if extra_files:
            final_list.extend(sorted(extra_files))
            
        return final_list

    async def download(self, mega_link, update_status_func=None):
        session_dir = self._create_session_dir()
        baseline_size = self._get_dir_size(session_dir)
        
        LOGGER.info(f"Fetching file order for: {mega_link}")
        ordered_filenames = await self._get_mega_file_order(mega_link)
        
        retry_count = 0
        max_retries = 10 # Prevent infinite loops

        while not self.is_cancelled:
            # Get a valid proxy from DB
            current_proxy = await self.proxy_manager.get_best_proxy()
            
            if current_proxy is None and retry_count > 0:
                return False, "All proxies (and Direct) are exhausted/limited."

            proxy_label = current_proxy if current_proxy else "Direct"
            
            cmd = ["megadl", "--path", session_dir, mega_link]
            if current_proxy:
                cmd.extend(["--proxy", current_proxy])
            
            LOGGER.info(f"Starting Download via: {proxy_label}")
            
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            limit_reached = False
            last_db_update = time.time()
            current_download_usage = 0

            while True:
                if self.is_cancelled:
                    try: process.kill()
                    except: pass
                    return False, "Cancelled"

                if process.returncode is not None:
                    break

                # Calculate size delta
                total_current_size = self._get_dir_size(session_dir)
                downloaded_so_far = total_current_size - baseline_size
                
                # Update DB every 5 seconds or if size changed significantly
                if time.time() - last_db_update > 5:
                    delta = downloaded_so_far - current_download_usage
                    if delta > 0:
                        await self.proxy_manager.update_usage(current_proxy, delta)
                        current_download_usage = downloaded_so_far
                        last_db_update = time.time()

                if update_status_func:
                    await update_status_func(
                        f"Downloading...\nSize: {downloaded_so_far / (1024**2):.2f} MB\nProxy: {proxy_label}"
                    )

                # Check if this specific session is getting too huge for one proxy
                # (Optional: You can rely purely on the initial check, but strict enforcement is here)
                if downloaded_so_far > (1.9 * 1024**3): 
                     LOGGER.info("Session exceeded safe limit for this proxy. Switching...")
                     try: process.kill()
                     except: pass
                     limit_reached = True
                     break
                
                try:
                    await asyncio.wait_for(process.wait(), timeout=2)
                except asyncio.TimeoutError:
                    pass
            
            # Final DB update for this run
            total_current_size = self._get_dir_size(session_dir)
            final_delta = (total_current_size - baseline_size) - current_download_usage
            if final_delta > 0:
                await self.proxy_manager.update_usage(current_proxy, final_delta)

            if process.returncode == 0 and not limit_reached:
                output_files = self._resolve_output_ordered(session_dir, ordered_filenames)
                if output_files:
                    return True, output_files
                return False, "Download finished but folder is empty."
            
            if limit_reached or process.returncode != 0:
                # If failed or limited, force update DB to ensure this proxy is marked as used
                # Then loop again to pick a NEW proxy (get_best_proxy will skip the full one)
                LOGGER.info("Switching proxy and resuming...")
                retry_count += 1
                await asyncio.sleep(2)
                continue
        
        return False, "Unknown Error"
