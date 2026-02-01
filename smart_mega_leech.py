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
        self.proxies = proxies
        self.limit_bytes = limit_bytes
        self.mongo_url = os.environ.get("MONGO_URL")
        self.collection = None
        if self.mongo_url:
            try:
                client = AsyncIOMotorClient(self.mongo_url)
                # Using a specific database and collection
                self.collection = client["mega_leech_v2"]["proxy_usage"]
                LOGGER.info("Connected to MongoDB for Proxy Tracking.")
            except Exception as e:
                LOGGER.error(f"Failed to connect to MongoDB: {e}")

    async def get_best_proxy(self):
        # FIX: Check explicitly against None
        if self.collection is None:
            if not self.proxies: return None
            return self.proxies[0] 

        now = datetime.utcnow()
        
        for proxy in self.proxies:
            proxy_key = proxy if proxy else "DIRECT"
            
            try:
                doc = await self.collection.find_one({"_id": proxy_key})
            except Exception as e:
                LOGGER.error(f"DB Error: {e}")
                return proxy # Fallback if DB fails
            
            if not doc:
                return proxy 
            
            last_updated = doc.get("last_updated", datetime.min)
            used_bytes = doc.get("used_bytes", 0)
            
            # Reset after 12 hours
            if (now - last_updated) > timedelta(hours=12):
                await self.collection.update_one(
                    {"_id": proxy_key},
                    {"$set": {"used_bytes": 0, "last_updated": now}}
                )
                return proxy
            
            if used_bytes < self.limit_bytes:
                return proxy
            
        return None

    async def update_usage(self, proxy, bytes_downloaded):
        # FIX: Check explicitly against None
        if self.collection is None: return
        
        proxy_key = proxy if proxy else "DIRECT"
        try:
            await self.collection.update_one(
                {"_id": proxy_key},
                {
                    "$inc": {"used_bytes": bytes_downloaded},
                    "$set": {"last_updated": datetime.utcnow()}
                },
                upsert=True
            )
        except Exception as e:
            LOGGER.error(f"Failed to update usage stats: {e}")

class SmartMegaLeecher:
    def __init__(self, download_path, proxies_file="proxies.txt"):
        self.download_path = download_path
        self.proxies_list = self._load_proxies(proxies_file)
        self.proxy_manager = ProxyManager(self.proxies_list)
        self.is_cancelled = False

    def _load_proxies(self, filepath):
        proxies = [None] # Direct connection first
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
        """
        Runs megadl --info to get the full list of files.
        It parses the output to get the relative path structure (Folder/Sub/File.mp4).
        """
        cmd = ["megadl", "--info", mega_link]
        # Allow time for info gathering on large folders (1TB+)
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            stdout, _ = await proc.communicate()
        except Exception as e:
            LOGGER.error(f"Failed to fetch mega info: {e}")
            return []
        
        ordered_paths = []
        if proc.returncode == 0:
            try:
                output = stdout.decode("utf-8", errors="ignore").strip()
                for line in output.split('\n'):
                    if line.strip().startswith("filename:"):
                        # Extract: /RootFolder/SubFolder/File.mp4
                        # remove leading "filename: " and then strip leading slashes
                        raw_path = line.split("filename:", 1)[1].strip().lstrip("/")
                        ordered_paths.append(raw_path)
            except Exception as e:
                LOGGER.error(f"Error parsing order: {e}")
        return ordered_paths

    def _resolve_output_ordered(self, session_dir, ordered_paths):
        """
        Matches files on disk to the Mega folder structure.
        """
        found_files_map = {}
        extra_files = []

        # 1. Map all files currently on disk by their RELATIVE path
        #    Example: session_dir/MyFolder/Video.mp4 -> Key: MyFolder/Video.mp4
        for root, _, filenames in os.walk(session_dir):
            for filename in filenames:
                full_path = os.path.join(root, filename)
                # Get path relative to the download session root
                rel_path = os.path.relpath(full_path, session_dir)
                found_files_map[rel_path] = full_path
                extra_files.append(full_path)

        final_list = []
        
        # 2. Reconstruct list based on Mega's order
        for mega_path in ordered_paths:
            # We try to find the exact path match
            if mega_path in found_files_map:
                final_list.append(found_files_map[mega_path])
                if found_files_map[mega_path] in extra_files:
                    extra_files.remove(found_files_map[mega_path])
        
        # 3. If any files were downloaded but not in the info list, add them at the end
        if extra_files:
            final_list.extend(sorted(extra_files))
            
        return final_list

    async def download(self, mega_link, update_status_func=None):
        session_dir = self._create_session_dir()
        baseline_size = self._get_dir_size(session_dir)
        
        if update_status_func:
            await update_status_func("Fetching folder structure... (This may take time for large folders)")

        ordered_filenames = await self._get_mega_file_order(mega_link)
        LOGGER.info(f"Found {len(ordered_filenames)} files in Mega structure.")
        
        retry_count = 0
        max_retries = 15

        while not self.is_cancelled:
            current_proxy = await self.proxy_manager.get_best_proxy()
            
            if current_proxy is None and retry_count > 0:
                return False, "All proxies (and Direct) are exhausted/limited for now."

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

                total_current_size = self._get_dir_size(session_dir)
                downloaded_so_far = total_current_size - baseline_size
                
                # Update DB Usage
                if time.time() - last_db_update > 5:
                    delta = downloaded_so_far - current_download_usage
                    if delta > 0:
                        await self.proxy_manager.update_usage(current_proxy, delta)
                        current_download_usage = downloaded_so_far
                        last_db_update = time.time()

                if update_status_func:
                    msg = f"Downloading Folder...\nSize: {downloaded_so_far / (1024**2):.2f} MB\nProxy: {proxy_label}"
                    if len(ordered_filenames) > 0:
                        msg += f"\nTotal Files: {len(ordered_filenames)}"
                    await update_status_func(msg)

                # Safety Check: If one proxy downloads > 1.9GB, kill it to rotate
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
            
            # Final DB update
            total_current_size = self._get_dir_size(session_dir)
            final_delta = (total_current_size - baseline_size) - current_download_usage
            if final_delta > 0:
                await self.proxy_manager.update_usage(current_proxy, final_delta)

            # Success Check
            if process.returncode == 0 and not limit_reached:
                output_files = self._resolve_output_ordered(session_dir, ordered_filenames)
                if output_files:
                    return True, output_files
                return False, "Download finished but folder is empty."
            
            # Failure/Limit Handling
            if limit_reached or process.returncode != 0:
                LOGGER.info("Switching proxy and resuming...")
                retry_count += 1
                await asyncio.sleep(2)
                continue
        
        return False, "Unknown Error"
