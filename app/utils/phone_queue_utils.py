import csv
import re
import asyncio
import os
import random
from typing import List
from sqlalchemy import text
from app.db.database import Database
from app.core.logger import get_logger

logger = get_logger(__name__)


class PhoneQueueUtils:
    def __init__(self, db: Database):
        self.db = db

    def is_valid_phone(self, phone: str) -> bool:
        """验证是否为有效的电话号码"""
        if not phone or not isinstance(phone, str):
            return False
        
        # 去除所有空格、横线、括号等字符
        clean_phone = re.sub(r'[\s\-\(\)\+]', '', phone)
        
        # 检查是否只包含数字
        if not clean_phone.isdigit():
            return False
        
        # 检查长度：中国手机号11位，固话7-8位，国际号码6-15位
        if len(clean_phone) < 6 or len(clean_phone) > 15:
            return False
        
        # 中国手机号特殊检查：以1开头，第二位为3-9
        if len(clean_phone) == 11 and clean_phone.startswith('1'):
            if clean_phone[1] in '3456789':
                return True
        
        # 中国固话检查：区号+号码
        if len(clean_phone) >= 7 and len(clean_phone) <= 12:
            # 3位区号+7-8位号码 或 4位区号+7-8位号码
            if (len(clean_phone) == 10 or len(clean_phone) == 11 or 
                len(clean_phone) == 12):
                return True
        
        # 国际号码：6-15位数字
        if 6 <= len(clean_phone) <= 15:
            return True
        
        return False

    def filter_valid_phones(self, phones_data: List[str]) -> List[str]:
        """过滤出有效的电话号码"""
        valid_phones = []
        invalid_count = 0
        
        for phone in phones_data:
            if self.is_valid_phone(phone):
                valid_phones.append(phone)
            else:
                invalid_count += 1
                logger.debug("无效电话号码: %s", phone)
        
        logger.info("电话号码过滤完成 - 有效: %d, 无效: %d", len(valid_phones), invalid_count)
        return valid_phones

    async def load_phones_from_csv(self, csv_file_path: str) -> List[str]:
        """从CSV文件读取电话号码数据"""
        phones_data = []
        
        if not os.path.exists(csv_file_path):
            raise FileNotFoundError(f"CSV文件不存在: {csv_file_path}")
        
        try:
            # 尝试多种编码格式
            encodings = ['utf-8', 'gbk', 'gb2312', 'utf-8-sig']
            
            for encoding in encodings:
                try:
                    with open(csv_file_path, 'r', encoding=encoding) as file:
                        csv_reader = csv.reader(file)
                        for row in csv_reader:
                            if len(row) >= 1 and row[0].strip():
                                phone = row[0].strip()
                                if phone and phone != "电话号码":  # 跳过标题行
                                    phones_data.append(phone)
                    logger.info("成功使用 %s 编码读取CSV文件", encoding)
                    break
                except UnicodeDecodeError:
                    continue
            
            if not phones_data:
                raise ValueError("无法使用任何编码格式读取CSV文件")
            
            logger.info("从CSV文件读取到 %d 条电话号码数据", len(phones_data))
            return phones_data
            
        except Exception as e:
            logger.error("读取CSV文件失败: %s", e)
            raise

    async def batch_insert_phones(self, phones_data: List[str], batch_size: int = 100):
        """批量插入电话号码到数据库，自动处理重复数据"""
        if not phones_data:
            logger.warning("没有数据需要插入")
            return
        
        success_count = 0
        duplicate_count = 0
        error_count = 0
        
        for i in range(0, len(phones_data), batch_size):
            batch = phones_data[i:i + batch_size]
            
            try:
                async with self.db.get_session() as session:
                    for phone in batch:
                        try:
                            # 使用 INSERT IGNORE 自动跳过重复数据
                            insert_sql = text("""
                                INSERT IGNORE INTO phone_call_queue (phone, is_called) 
                                VALUES (:phone, :is_called)
                            """)
                            result = await session.execute(insert_sql, {
                                "phone": phone,
                                "is_called": False
                            })
                            
                            if result.rowcount > 0:
                                success_count += 1
                            else:
                                duplicate_count += 1
                                logger.debug("电话号码 %s 已存在，跳过", phone)
                            
                        except Exception as e:
                            error_count += 1
                            logger.error("插入电话号码 %s 失败: %s", phone, e)
                            continue
                    
                    await session.commit()
                    
            except Exception as e:
                logger.error("批量插入第 %d 批数据失败: %s", i//batch_size + 1, e)
                error_count += len(batch)
        
        logger.info("批量插入完成 - 成功: %d, 重复: %d, 失败: %d", success_count, duplicate_count, error_count)

    async def shuffle_phone_queue(self):
        """随机打乱电话队列中未拨打的电话号码顺序"""
        try:
            async with self.db.get_session() as session:
                # 获取所有未拨打的电话号码
                select_sql = text("""
                    SELECT id, phone 
                    FROM phone_call_queue 
                    WHERE is_called = FALSE 
                    ORDER BY created_at ASC
                """)
                result = await session.execute(select_sql)
                rows = result.fetchall()
                
                if not rows:
                    logger.info("没有未拨打的电话号码需要打乱")
                    return
                
                # 提取电话号码列表
                phones = [(row[0], row[1]) for row in rows]
                
                # 随机打乱顺序
                random.shuffle(phones)
                
                # 批量更新创建时间以实现重新排序
                for i, (phone_id, phone) in enumerate(phones):
                    update_sql = text("""
                        UPDATE phone_call_queue 
                        SET created_at = DATE_ADD(NOW(), INTERVAL :offset SECOND)
                        WHERE id = :phone_id
                    """)
                    await session.execute(update_sql, {
                        "offset": i,
                        "phone_id": phone_id
                    })
                
                await session.commit()
                logger.info("成功打乱了 %d 个未拨打电话号码的顺序", len(phones))
                
        except Exception as e:
            logger.error("打乱电话队列失败: %s", e)
            raise

    async def add_phones_from_csv(self, csv_file_path: str = None, shuffle: bool = True):
        """从CSV文件添加电话号码到队列的主函数"""
        if csv_file_path is None:
            csv_file_path = os.path.join(os.path.dirname(__file__), "公海名单20240201以前.csv")
        
        try:
            phones_data = await self.load_phones_from_csv(csv_file_path)
            valid_phones = self.filter_valid_phones(phones_data)
            await self.batch_insert_phones(valid_phones)
            
            if shuffle:
                await self.shuffle_phone_queue()
                logger.info("电话号码队列添加完成并已打乱顺序")
            else:
                logger.info("电话号码队列添加完成")
            
        except Exception as e:
            logger.error("添加电话号码队列失败: %s", e)
            raise

async def main():
    """主函数：添加CSV文件中的电话号码到队列"""
    db = Database()
    phone_utils = PhoneQueueUtils(db)
    
    try:
        await db.initialize()
        logger.info("数据库连接初始化成功")
        
        await phone_utils.add_phones_from_csv(shuffle=True)
        logger.info("电话号码队列添加完成并已打乱顺序")
        
    except Exception as e:
        logger.error("执行失败: %s", e)
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
