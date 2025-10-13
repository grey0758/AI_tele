import csv
import asyncio
import os
from typing import List, Tuple
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from app.db.database import Database
from app.core.logger import get_logger

logger = get_logger(__name__)


class PhoneQueueUtils:
    def __init__(self, db: Database):
        self.db = db

    async def load_phones_from_csv(self, csv_file_path: str) -> List[Tuple[str, str]]:
        """从CSV文件读取电话号码数据"""
        phones_data = []
        
        if not os.path.exists(csv_file_path):
            raise FileNotFoundError(f"CSV文件不存在: {csv_file_path}")
        
        try:
            with open(csv_file_path, 'r', encoding='utf-8') as file:
                csv_reader = csv.reader(file)
                for row in csv_reader:
                    if len(row) >= 2 and row[0].strip() and row[1].strip():
                        id_num, phone = row[0].strip(), row[1].strip()
                        phones_data.append((id_num, phone))
            
            logger.info("从CSV文件读取到 %d 条电话号码数据", len(phones_data))
            return phones_data
            
        except Exception as e:
            logger.error("读取CSV文件失败: %s", e)
            raise

    async def batch_insert_phones(self, phones_data: List[Tuple[str, str]], batch_size: int = 100):
        """批量插入电话号码到数据库"""
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
                    for _, phone in batch:
                        try:
                            insert_sql = text("""
                                INSERT INTO phone_call_queue (phone, is_called) 
                                VALUES (:phone, :is_called)
                            """)
                            await session.execute(insert_sql, {
                                "phone": phone,
                                "is_called": False
                            })
                            success_count += 1
                            
                        except IntegrityError:
                            duplicate_count += 1
                            logger.debug("电话号码 %s 已存在，跳过", phone)
                            continue
                        except Exception as e:
                            error_count += 1
                            logger.error("插入电话号码 %s 失败: %s", phone, e)
                            continue
                    
                    await session.commit()
                    
            except Exception as e:
                logger.error("批量插入第 %d 批数据失败: %s", i//batch_size + 1, e)
                error_count += len(batch)
        
        logger.info("批量插入完成 - 成功: %d, 重复: %d, 失败: %d", success_count, duplicate_count, error_count)

    async def add_phones_from_csv(self, csv_file_path: str = None):
        """从CSV文件添加电话号码到队列的主函数"""
        if csv_file_path is None:
            csv_file_path = os.path.join(os.path.dirname(__file__), "四月线索表.csv")
        
        try:
            phones_data = await self.load_phones_from_csv(csv_file_path)
            await self.batch_insert_phones(phones_data)
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
        
        await phone_utils.add_phones_from_csv()
        logger.info("电话号码队列添加完成")
        
    except Exception as e:
        logger.error("执行失败: %s", e)
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
