# archive_leaderboards.py
import os
import json
import asyncio
import aiohttp
import lzma
from datetime import datetime
from dotenv import load_dotenv
import logging
import subprocess

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class LeaderboardArchiver:
    def __init__(self):
        self.auth_token = os.getenv('USER_TOKEN')
        self.dblink = os.getenv('DB_LINK')
        
        if not self.auth_token or not self.dblink:
            raise ValueError("Missing required environment variables: USER_TOKEN and DB_LINK")
        
        self.base_url = f"{self.dblink}/api/getScores"
        
    async def get_scores(self, session, display_type, control_type, pb_type):
        """Fetch scores for a specific combination of parameters"""
        try:
            payload = {
                "display_type": display_type,
                "control_type": control_type,
                "pb_type": pb_type
            }
            
            headers = {
                'Authorization': self.auth_token,
                'Content-Type': 'application/json'
            }
            
            async with session.post(self.base_url, json=payload, headers=headers) as response:
                if response.status == 200:
                    text_data = await response.text()
                    return {
                        "status": True,
                        "data": text_data,
                        "display_type": display_type,
                        "control_type": control_type,
                        "pb_type": pb_type
                    }
                else:
                    return {
                        "status": False,
                        "message": f"Error: ${response.status} ${response.statusText}",
                        "display_type": display_type,
                        "control_type": control_type,
                        "pb_type": pb_type
                    }
                    
        except Exception as e:
            return {
                "status": False,
                "message": f"Request failed: {str(e)}",
                "display_type": display_type,
                "control_type": control_type,
                "pb_type": pb_type
            }
    
    def compress_and_save_archive(self, all_data):
        """Compress all data using LZMA with maximum compression"""
        try:
            # Create simple archive structure
            archive_data = {
                "timestamp": datetime.now().isoformat(),
                "data": all_data
            }
            
            # Convert to JSON with minimal whitespace
            json_data = json.dumps(archive_data, separators=(',', ':'))
            original_size = len(json_data.encode('utf-8'))
            
            # Create archives directory if it doesn't exist
            os.makedirs("archives", exist_ok=True)
            
            # Add date to filename
            current_date = datetime.now().strftime("%Y%m%d")
            archive_filename = f"leaderboard_{current_date}.lzma"
            archive_path = os.path.join("archives", archive_filename)
            
            # Compress with LZMA using maximum compression (same as testing)
            with lzma.open(archive_path, 'wb', preset=9) as f:
                f.write(json_data.encode('utf-8'))
            
            compressed_size = os.path.getsize(archive_path)
            compression_ratio = (compressed_size / original_size) * 100
            
            logger.info(f"LZMA archive saved: {archive_path}")
            logger.info(f"Compression: {original_size:,} → {compressed_size:,} bytes ({compression_ratio:.1f}%)")
            
            return archive_path
            
        except Exception as e:
            logger.error(f"Failed to compress archive: {e}")
            return None
    
    async def archive_all_combinations(self):
        """Archive all possible combinations of parameters"""
        # Parameter ranges
        display_types = range(1, 21)  # 1 to 20
        control_types = range(0, 4)   # 0 to 3
        pb_types = range(1, 4)        # 1 to 3
        
        # Prepare all combinations
        combinations = [
            (dt, ct, pt) 
            for dt in display_types 
            for ct in control_types 
            for pt in pb_types
        ]
        
        total_combinations = len(combinations)
        successful_archives = 0
        failed_archives = 0
        
        logger.info(f"Starting archive of {total_combinations} combinations...")
        
        all_data = {}
        
        # Use aiohttp for async requests
        async with aiohttp.ClientSession() as session:
            # Create tasks for all combinations
            tasks = []
            for dt, ct, pt in combinations:
                task = self.get_scores(session, dt, ct, pt)
                tasks.append(task)
            
            # Process results as they complete
            for future in asyncio.as_completed(tasks):
                result = await future
                
                if result["status"] and "data" in result:
                    # Store successful data with combination key
                    key = f"{result['display_type']}_{result['control_type']}_{result['pb_type']}"
                    all_data[key] = result["data"]
                    successful_archives += 1
                else:
                    failed_archives += 1
                    logger.error(f"Failed for {result['display_type']}_{result['control_type']}_{result['pb_type']}: {result.get('message', 'Unknown error')}")
                
                # Progress update
                current_progress = successful_archives + failed_archives
                if current_progress % 20 == 0:  # Log every 20 requests
                    logger.info(f"Progress: {current_progress}/{total_combinations}")
        
        # Compress and save all data with LZMA
        if successful_archives > 0:
            archive_path = self.compress_and_save_archive(all_data)
            logger.info(f"Archive saved to: {archive_path}")
        else:
            logger.error("No successful archives to compress!")
        
        # Summary
        logger.info(f"Archive completed! Successful: {successful_archives}, Failed: {failed_archives}")

def git_update():
    os.chdir(r"C:\coding\leaderboardArchiver")
    subprocess.run(["git", "add", "."])
    subprocess.run(["git", "commit", "-m", "update"])
    subprocess.run(["git", "push", "-u", "origin", "main"])

async def main():
    """Main function to run the archiver"""
    try:
        archiver = LeaderboardArchiver()
        await archiver.archive_all_combinations()
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")

if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())
    git_update()