"""下载通话记录"""
import os
import asyncio
from datetime import datetime, date
from typing import List, Dict, Any
import requests
from sqlalchemy import text
from app.db.database import Database
from app.core.logger import get_logger

logger = get_logger(__name__)

class CallRecordDownloader:
    """通话记录下载器"""
    def __init__(self, db: Database):
        self.db = db
        self.session = None
        
    async def get_call_records(
        self, 
        limit: int = 50,
        start_time: str = None,
        end_time: str = None,
        advisor_group_id: int = None,
        check_cloud_url: bool = None,
        check_conversation_content: bool = None,
        min_call_quality_score: float = None,
        min_time_len: int = None,
        max_time_len: int = None,
        exclude_phones: list = None
    ) -> List[Dict[str, Any]]:
        """获取通话记录"""
        # 构建基础查询
        base_query = """
            SELECT 
                cr.id,
                cr.phone,
                cr.time_len,
                cr.conversation_content,
                cr.cloud_url,
                cr.call_quality_score,
                cr.created_at
            FROM call_records cr
            WHERE 1=1
        """
        
        # 构建动态WHERE条件
        conditions = []
        params = {"limit": limit}
        
        # 时间区间筛选
        if start_time:
            conditions.append("cr.created_at >= :start_time")
            params["start_time"] = start_time
            
        if end_time:
            conditions.append("cr.created_at <= :end_time")
            params["end_time"] = end_time
            
        # 顾问组ID筛选
        if advisor_group_id is not None:
            conditions.append("cr.advisor_group_id = :advisor_group_id")
            params["advisor_group_id"] = advisor_group_id
            
        # 云存储URL筛选
        if check_cloud_url is not None:
            if check_cloud_url:
                conditions.append("cr.cloud_url IS NOT NULL")
            else:
                conditions.append("cr.cloud_url IS NULL")
                
        # 对话内容筛选
        if check_conversation_content is not None:
            if check_conversation_content:
                conditions.append("cr.conversation_content IS NOT NULL")
            else:
                conditions.append("cr.conversation_content IS NULL")
                
        # 通话质量评分筛选
        if min_call_quality_score is not None:
            conditions.append("cr.call_quality_score >= :min_call_quality_score")
            params["min_call_quality_score"] = min_call_quality_score
            
        # 通话时长区间筛选
        if min_time_len is not None:
            conditions.append("cr.time_len >= :min_time_len")
            params["min_time_len"] = min_time_len
            
        if max_time_len is not None:
            conditions.append("cr.time_len <= :max_time_len")
            params["max_time_len"] = max_time_len
            
        # 排除指定电话号码
        if exclude_phones and len(exclude_phones) > 0:
            placeholders = ", ".join([f":exclude_phone_{i}" for i in range(len(exclude_phones))])
            conditions.append(f"cr.phone NOT IN ({placeholders})")
            for i, phone in enumerate(exclude_phones):
                params[f"exclude_phone_{i}"] = phone
        
        # 组合查询
        if conditions:
            where_clause = " AND " + " AND ".join(conditions)
        else:
            where_clause = ""
            
        full_query = base_query + where_clause + " ORDER BY cr.created_at DESC LIMIT :limit"
        
        query = text(full_query)
        
        async with self.db.get_session() as session:
            result = await session.execute(query, params)
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
    
    async def get_today_call_records(self, limit: int = 50) -> List[Dict[str, Any]]:
        """获取今天的通话记录（向后兼容方法）"""
        today = datetime.now().strftime("%Y-%m-%d")
        return await self.get_call_records(
            limit=limit,
            start_time=f"{today} 00:00:00",
            end_time=f"{today} 23:59:59",
            advisor_group_id=2,
            check_cloud_url=True,
            check_conversation_content=True,
            min_call_quality_score=0,
            min_time_len=20,
            exclude_phones=['13189300627', '18028260616', '17369322905', '13302752724']
        )
    
    def download_audio_file(self, url: str, file_path: str) -> bool:
        """下载音频文件"""
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
        """创建下载文件夹"""
        today = date.today().strftime("%Y%m%d")
        folder_name = f"call_records_{today}"
        folder_path = os.path.join(base_path, folder_name)
        
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
            logger.info("创建下载文件夹: %s", folder_path)
        
        return folder_path
    
    def get_safe_filename(self, phone: str, created_at: datetime, index: int) -> str:
        """获取安全文件名"""
        timestamp = created_at.strftime("%Y%m%d_%H%M%S")
        safe_phone = phone.replace("+", "").replace("-", "").replace(" ", "")
        return f"{index:02d}_{safe_phone}_{timestamp}.mp3"
    
    async def download_call_records(
        self, 
        limit: int = 40, 
        base_path: str = "./downloads",
        start_time: str = None,
        end_time: str = None,
        advisor_group_id: int = None,
        check_cloud_url: bool = None,
        check_conversation_content: bool = None,
        min_call_quality_score: float = None,
        min_time_len: int = None,
        max_time_len: int = None,
        exclude_phones: list = None
    ) -> Dict[str, Any]:
        """下载通话记录"""
        try:
            logger.info("开始下载前%d条通话录音...", limit)
            
            # 如果提供了自定义参数，使用新的查询方法；否则使用默认的今天记录方法
            if any([start_time, end_time, advisor_group_id is not None, check_cloud_url is not None, 
                   check_conversation_content is not None, min_call_quality_score is not None,
                   min_time_len is not None, max_time_len is not None, exclude_phones]):
                records = await self.get_call_records(
                    limit=limit,
                    start_time=start_time,
                    end_time=end_time,
                    advisor_group_id=advisor_group_id,
                    check_cloud_url=check_cloud_url,
                    check_conversation_content=check_conversation_content,
                    min_call_quality_score=min_call_quality_score,
                    min_time_len=min_time_len,
                    max_time_len=max_time_len,
                    exclude_phones=exclude_phones
                )
            else:
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
                "message": f"下载完成，成功: {downloaded_count}, 失败: {failed_count}",
                "downloaded": downloaded_count,
                "failed": failed_count,
                "folder_path": folder_path,
                "failed_urls": failed_urls
            }
            
            logger.info("下载任务完成: %s", result['message'])
            return result
            
        except Exception as e:
            logger.error("下载通话录音时出错: %s", e)
            return {"success": False, "message": f"下载出错: {str(e)}", "downloaded": 0}

async def download_today_call_records(
    limit: int = 40, 
    base_path: str = "./downloads",
    start_time: str = None,
    end_time: str = None,
    advisor_group_id: int = None,
    check_cloud_url: bool = None,
    check_conversation_content: bool = None,
    min_call_quality_score: float = None,
    min_time_len: int = None,
    max_time_len: int = None,
    exclude_phones: list = None
) -> Dict[str, Any]:
    """下载通话记录"""
    db = Database()
    try:
        await db.initialize()
        downloader = CallRecordDownloader(db)
        result = await downloader.download_call_records(
            limit=limit,
            base_path=base_path,
            start_time=start_time,
            end_time=end_time,
            advisor_group_id=advisor_group_id,
            check_cloud_url=check_cloud_url,
            check_conversation_content=check_conversation_content,
            min_call_quality_score=min_call_quality_score,
            min_time_len=min_time_len,
            max_time_len=max_time_len,
            exclude_phones=exclude_phones
        )
        return result
    finally:
        await db.close()

if __name__ == "__main__":
    async def main():
        """主函数"""
        # 示例1: 使用默认参数（今天的记录）
        # result = await download_today_call_records(40)
        # print(f"下载结果: {result}")
        
        result = await download_today_call_records(
            limit=200,
            start_time="2025-10-17 11:40:00",
            end_time="2025-10-17 13:00:00",
            advisor_group_id=2,
            check_cloud_url=True,
            check_conversation_content= None,
            min_call_quality_score= 20, # 质量评分大于0.5
            min_time_len=None,  # 通话时长大于30秒
            max_time_len=None,  # 通话时长小于300秒
            exclude_phones=None  # 排除指定号码
        )
        print(f"自定义筛选结果: {result}")
    
    asyncio.run(main())
