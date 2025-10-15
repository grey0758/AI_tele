import os
import asyncio
import requests
from datetime import datetime, date
from typing import List, Dict, Any
from sqlalchemy import text
from app.db.database import Database
from app.core.logger import get_logger

logger = get_logger(__name__)

class CallRecordDownloader:
    def __init__(self, db: Database):
        self.db = db
        self.session = None
        
    async def get_today_call_records(self, limit: int = 50) -> List[Dict[str, Any]]:
        query = text("""
            SELECT 
                cr.id,
                cr.phone,
                cr.time_len,
                cr.conversation_content,
                cr.cloud_url,
                cr.call_quality_score,
                cr.created_at
            FROM call_records cr
            WHERE 
                cr.created_at > '2025-10-15 14:00:00'
                AND cr.advisor_group_id = 2
                AND cr.cloud_url IS NOT NULL
                AND cr.conversation_content IS NOT NULL
                AND cr.call_quality_score > 0
                AND cr.time_len > 20
                AND cr.phone NOT IN ('13189300627', '18028260616', '17369322905','13302752724')
            ORDER BY cr.created_at DESC
            LIMIT :limit
        """)
        
        async with self.db.get_session() as session:
            result = await session.execute(query, {"limit": limit})
            records = []
            for row in result:
                records.append({
                    "id": row.id,
                    "phone": row.phone,
                    "time_len": row.time_len,
                    "conversation_content": row.conversation_content,
                    "cloud_url": row.cloud_url,
                    "call_quality_score": row.call_quality_score,
                    "created_at": row.created_at
                })
            return records
    
    def download_audio_file(self, url: str, file_path: str) -> bool:
        try:
            response = requests.get(url, stream=True, timeout=30)
            if response.status_code == 200:
                with open(file_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                return True
            else:
                logger.error("下载失败，状态码: %s, URL: %s", response.status_code, url)
                return False
        except Exception as e:
            logger.error("下载音频文件时出错: %s, URL: %s", e, url)
            return False
    
    def create_download_folder(self, base_path: str = "./downloads") -> str:
        today = date.today().strftime("%Y%m%d")
        folder_name = f"call_records_{today}"
        folder_path = os.path.join(base_path, folder_name)
        
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
            logger.info("创建下载文件夹: %s", folder_path)
        
        return folder_path
    
    def get_safe_filename(self, phone: str, created_at: datetime, index: int) -> str:
        timestamp = created_at.strftime("%Y%m%d_%H%M%S")
        safe_phone = phone.replace("+", "").replace("-", "").replace(" ", "")
        return f"{index:02d}_{safe_phone}_{timestamp}.mp3"
    
    async def download_call_records(self, limit: int = 40, base_path: str = "./downloads") -> Dict[str, Any]:
        try:
            logger.info("开始下载前%d条通话录音...", limit)
            
            records = await self.get_today_call_records(limit)
            if not records:
                logger.warning("没有找到符合条件的通话记录")
                return {"success": False, "message": "没有找到符合条件的通话记录", "downloaded": 0}
            
            folder_path = self.create_download_folder(base_path)
            downloaded_count = 0
            failed_count = 0
            failed_urls = []
            
            for index, record in enumerate(records, 1):
                cloud_url = record.get("cloud_url")
                if not cloud_url:
                    logger.warning("记录%d没有云存储URL，跳过", index)
                    failed_count += 1
                    continue
                
                filename = self.get_safe_filename(
                    record["phone"], 
                    record["created_at"], 
                    index
                )
                file_path = os.path.join(folder_path, filename)
                
                logger.info("正在下载第%d条录音: %s", index, filename)
                success = self.download_audio_file(cloud_url, file_path)
                
                if success:
                    downloaded_count += 1
                    logger.info("成功下载: %s", filename)
                else:
                    failed_count += 1
                    failed_urls.append(cloud_url)
                    logger.error("下载失败: %s", filename)
            
            result = {
                "success": True,
                "message": "下载完成，成功: %d, 失败: %d" % (downloaded_count, failed_count),
                "downloaded": downloaded_count,
                "failed": failed_count,
                "folder_path": folder_path,
                "failed_urls": failed_urls
            }
            
            logger.info("下载任务完成: %s", result['message'])
            return result
            
        except Exception as e:
            logger.error("下载通话录音时出错: %s", e)
            return {"success": False, "message": "下载出错: %s" % str(e), "downloaded": 0}

async def download_today_call_records(limit: int = 40, base_path: str = "./downloads") -> Dict[str, Any]:
    db = Database()
    try:
        await db.initialize()
        downloader = CallRecordDownloader(db)
        result = await downloader.download_call_records(limit, base_path)
        return result
    finally:
        await db.close()

if __name__ == "__main__":
    async def main():
        result = await download_today_call_records(40)
        print(f"下载结果: {result}")
    
    asyncio.run(main())
