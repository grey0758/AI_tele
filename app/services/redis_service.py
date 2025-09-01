import redis
import json
import logging
import uuid
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, asdict
from app.core.config import settings

logger = logging.getLogger(__name__)

@dataclass
class Device:
    """设备信息数据类"""
    id: int
    instance: int
    model: int
    btConnect: int
    btDeviceName: str
    phoneName: str
    deviceId: str
    userId: str
    firmWareVer: str
    valid: int
    error: str

@dataclass
class DeviceInfo:
    """设备信息主数据类"""
    notify: str
    devid: str
    version: str
    recordmode: int
    devices: List[Device]
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DeviceInfo':
        """从字典创建实例"""
        devices = [Device(**device) for device in data.get('devices', [])]
        return cls(
            notify=data.get('notify', ''),
            devid=data.get('devid', ''),
            version=data.get('version', ''),
            recordmode=data.get('recordmode', 0),
            devices=devices
        )

@dataclass
class CallRecord:
    """电话记录数据类"""
    call_id: str  # UUID
    phone_number: str  # 电话号码
    call_type: str  # 呼叫类型：呼出/呼入
    status: str  # 当前状态：呼出/接通/已挂断
    start_time: str  # 开始时间
    end_time: Optional[str] = None  # 结束时间（挂断时设置）
    duration: Optional[int] = None  # 通话时长（秒）
    device_instance: Optional[int] = None  # 设备实例ID
    notes: Optional[str] = None  # 备注信息
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CallRecord':
        """从字典创建实例"""
        return cls(**data)

class RedisService:
    """Redis 服务类 - 统一管理设备信息和电话记录"""
    
    # Redis 键名常量
    DEVICE_INFO_KEY = "device_info"  # 设备信息的固定键名
    CALL_RECORD_PREFIX = "call_record:"  # 电话记录键前缀
    
    def __init__(self):
        """初始化 Redis 连接"""
        try:
            self.redis_client = redis.Redis(
                host=settings.REDIS_HOST,
                port=settings.REDIS_PORT,
                password=settings.REDIS_PASSWORD,
                db=settings.REDIS_DB,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True
            )
            # 测试连接
            self.redis_client.ping()
            logger.info("Redis 连接成功")
        except Exception as e:
            logger.error(f"Redis 连接失败: {e}")
            raise
    
    # ==================== 设备信息管理 ====================
    
    def set_device_info(self, device_info: DeviceInfo) -> bool:
        """
        设置设备信息（创建或更新）
        
        Args:
            device_info: 设备信息对象
            
        Returns:
            bool: 操作是否成功
        """
        try:
            device_data = device_info.to_dict()
            self.redis_client.set(
                self.DEVICE_INFO_KEY, 
                json.dumps(device_data, ensure_ascii=False)
            )
            logger.info(f"设备信息已保存: devid={device_info.devid}")
            return True
        except Exception as e:
            logger.error(f"保存设备信息失败: {e}")
            return False
    
    def get_device_info(self) -> Optional[DeviceInfo]:
        """
        获取设备信息
        
        Returns:
            DeviceInfo: 设备信息对象，不存在时返回 None
        """
        try:
            data = self.redis_client.get(self.DEVICE_INFO_KEY)
            if not data:
                logger.warning("设备信息不存在")
                return None
            
            device_dict = json.loads(data)
            device_info = DeviceInfo.from_dict(device_dict)
            logger.info(f"获取设备信息成功: devid={device_info.devid}")
            return device_info
        except Exception as e:
            logger.error(f"获取设备信息失败: {e}")
            return None
    
    def get_default_device_instance(self, device_index: int = 0) -> Optional[int]:
        """
        获取默认设备的 instance 值
        
        Args:
            device_index: 设备索引，默认为 0（第一个设备）
            
        Returns:
            int: 设备的 instance 值，不存在时返回 None
        """
        try:
            device_info = self.get_device_info()
            if not device_info or not device_info.devices:
                logger.warning("设备信息不存在或设备列表为空")
                return None
            
            if device_index >= len(device_info.devices):
                logger.warning(f"设备索引 {device_index} 超出范围，设备总数: {len(device_info.devices)}")
                return None
            
            instance = device_info.devices[device_index].instance
            logger.info(f"获取默认设备 instance 成功: {instance}")
            return instance
        except Exception as e:
            logger.error(f"获取默认设备 instance 失败: {e}")
            return None
    
    def device_exists(self) -> bool:
        """
        检查设备信息是否存在
        
        Returns:
            bool: 设备信息是否存在
        """
        try:
            return self.redis_client.exists(self.DEVICE_INFO_KEY) > 0
        except Exception as e:
            logger.error(f"检查设备信息存在性失败: {e}")
            return False
    
    def delete_device_info(self) -> bool:
        """
        删除设备信息
        
        Returns:
            bool: 操作是否成功
        """
        try:
            result = self.redis_client.delete(self.DEVICE_INFO_KEY)
            if result:
                logger.info("设备信息已删除")
                return True
            else:
                logger.warning("设备信息不存在，无需删除")
                return False
        except Exception as e:
            logger.error(f"删除设备信息失败: {e}")
            return False
    
    def get_device_count(self) -> int:
        """
        获取设备数量
        
        Returns:
            int: 设备数量
        """
        device_info = self.get_device_info()
        return len(device_info.devices) if device_info else 0
    
    def get_all_device_instances(self) -> List[int]:
        """
        获取所有设备的 instance 列表
        
        Returns:
            List[int]: 所有设备的 instance 值列表
        """
        device_info = self.get_device_info()
        if not device_info:
            return []
        return [device.instance for device in device_info.devices]

    # ==================== 电话记录管理 ====================
    
    def create_call_record(self, call_id: str, phone_number: str, call_type: str = "呼出", device_instance: Optional[int] = None) -> str:
        """
        创建新的电话记录
        
        Args:
            phone_number: 电话号码
            call_type: 呼叫类型（呼出/呼入）
            device_instance: 设备实例ID
            
        Returns:
            str: 生成的UUID
        """
        try:
            call_record = CallRecord(
                call_id=call_id,
                phone_number=phone_number,
                call_type=call_type,
                status="呼出",
                start_time=self._get_current_time(),
                device_instance=device_instance
            )
            
            key = f"{self.CALL_RECORD_PREFIX}{call_id}"
            self.redis_client.set(
                key,
                json.dumps(call_record.to_dict(), ensure_ascii=False),
                ex=86400  # 24小时过期
            )
            
            logger.info(f"创建电话记录成功: {call_id}, 号码: {phone_number}")
            return call_id
        except Exception as e:
            logger.error(f"创建电话记录失败: {e}")
            raise
    
    def update_call_status(self, call_id: str, status: str, **kwargs) -> bool:
        """
        更新电话记录状态
        
        Args:
            call_id: 电话记录ID
            status: 新状态（呼出/接通/已挂断）
            **kwargs: 其他要更新的字段
            
        Returns:
            bool: 操作是否成功
        """
        try:
            key = f"{self.CALL_RECORD_PREFIX}{call_id}"
            call_record = self.get_call_record(call_id)
            
            if not call_record:
                logger.warning(f"电话记录不存在: {call_id}")
                return False
            
            # 更新状态
            call_record.status = status
            
            # 更新其他字段
            for field, value in kwargs.items():
                if hasattr(call_record, field):
                    setattr(call_record, field, value)
            
            # 如果状态是已挂断，设置结束时间和计算通话时长
            if status == "已挂断" and not call_record.end_time:
                call_record.end_time = self._get_current_time()
                if call_record.start_time:
                    # 这里可以添加时间计算逻辑
                    pass
            
            # 保存更新后的记录
            self.redis_client.set(
                key,
                json.dumps(call_record.to_dict(), ensure_ascii=False),
                ex=86400  # 24小时过期
            )
            
            logger.info(f"更新电话记录状态成功: {call_id}, 新状态: {status}")
            return True
        except Exception as e:
            logger.error(f"更新电话记录状态失败: {e}")
            return False
    
    def get_call_record(self, call_id: str) -> Optional[CallRecord]:
        """
        获取电话记录
        
        Args:
            call_id: 电话记录ID
            
        Returns:
            CallRecord: 电话记录对象，不存在时返回 None
        """
        try:
            key = f"{self.CALL_RECORD_PREFIX}{call_id}"
            data = self.redis_client.get(key)
            
            if not data:
                logger.warning(f"电话记录不存在: {call_id}")
                return None
            
            call_dict = json.loads(data)
            call_record = CallRecord.from_dict(call_dict)
            logger.info(f"获取电话记录成功: {call_id}")
            return call_record
        except Exception as e:
            logger.error(f"获取电话记录失败: {e}")
            return None
    
    def get_all_call_records(self) -> List[CallRecord]:
        """
        获取所有电话记录
        
        Returns:
            List[CallRecord]: 所有电话记录列表
        """
        try:
            pattern = f"{self.CALL_RECORD_PREFIX}*"
            keys = self.redis_client.keys(pattern)
            
            call_records = []
            for key in keys:
                data = self.redis_client.get(key)
                if data:
                    call_dict = json.loads(data)
                    call_record = CallRecord.from_dict(call_dict)
                    call_records.append(call_record)
            
            logger.info(f"获取所有电话记录成功，共 {len(call_records)} 条")
            return call_records
        except Exception as e:
            logger.error(f"获取所有电话记录失败: {e}")
            return []
    
    def delete_call_record(self, call_id: str) -> bool:
        """
        删除电话记录
        
        Args:
            call_id: 电话记录ID
            
        Returns:
            bool: 操作是否成功
        """
        try:
            key = f"{self.CALL_RECORD_PREFIX}{call_id}"
            result = self.redis_client.delete(key)
            
            if result:
                logger.info(f"删除电话记录成功: {call_id}")
                return True
            else:
                logger.warning(f"电话记录不存在: {call_id}")
                return False
        except Exception as e:
            logger.error(f"删除电话记录失败: {e}")
            return False
    
    def get_call_records_by_phone(self, phone_number: str) -> List[CallRecord]:
        """
        根据电话号码获取电话记录
        
        Args:
            phone_number: 电话号码
            
        Returns:
            List[CallRecord]: 匹配的电话记录列表
        """
        try:
            all_records = self.get_all_call_records()
            matching_records = [
                record for record in all_records 
                if record.phone_number == phone_number
            ]
            
            logger.info(f"根据电话号码获取记录成功: {phone_number}, 共 {len(matching_records)} 条")
            return matching_records
        except Exception as e:
            logger.error(f"根据电话号码获取记录失败: {e}")
            return []
    
    # ==================== 私有工具方法 ====================
    
    def _get_current_time(self) -> str:
        """
        获取当前时间字符串
        
        Returns:
            str: 当前时间字符串
        """
        from datetime import datetime
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# 创建全局 Redis 服务实例
redis_service = RedisService()

# 导出
__all__ = ["redis_service", "DeviceInfo", "Device", "CallRecord"]
