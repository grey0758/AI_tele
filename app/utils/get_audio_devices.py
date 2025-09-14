import pyaudio
from typing import List, Tuple, Dict
from app.models.device_info import InputDevice, OutputDevice

def get_audio_devices(deduplicate: bool = True) -> Tuple[List[InputDevice], List[OutputDevice]]:
    """
    获取所有音频设备信息
    
    Args:
        deduplicate: 是否去重，选择最小索引
    
    Returns:
        Tuple[List[InputDevice], List[OutputDevice]]: (输入设备列表, 输出设备列表)
    """
    p = pyaudio.PyAudio()
    
    try:
        device_count = p.get_device_count()
        input_devices = []
        output_devices = []
        
        # 用于去重的字典：设备名称 -> 最小索引的设备信息
        input_device_map: Dict[str, InputDevice] = {}
        output_device_map: Dict[str, OutputDevice] = {}
        
        for i in range(device_count):
            try:
                device_info = p.get_device_info_by_index(i)
                device_name = device_info.get('name', '')
                
                # 如果是输入设备
                if device_info['maxInputChannels'] > 0:
                    input_device = InputDevice(
                        index=i,
                        name=device_name,
                        maxInputChannels=device_info.get('maxInputChannels'),
                        maxOutputChannels=device_info.get('maxOutputChannels'),
                        defaultSampleRate=device_info.get('defaultSampleRate'),
                        deviceType="input"
                    )
                    
                    if deduplicate:
                        # 去重：只保留最小索引的设备
                        if device_name not in input_device_map or i < input_device_map[device_name].index:
                            input_device_map[device_name] = input_device
                    else:
                        input_devices.append(input_device)
                
                # 如果是输出设备
                if device_info['maxOutputChannels'] > 0:
                    output_device = OutputDevice(
                        index=i,
                        name=device_name,
                        maxInputChannels=device_info.get('maxInputChannels'),
                        maxOutputChannels=device_info.get('maxOutputChannels'),
                        defaultSampleRate=device_info.get('defaultSampleRate'),
                        deviceType="output"
                    )
                    
                    if deduplicate:
                        # 去重：只保留最小索引的设备
                        if device_name not in output_device_map or i < output_device_map[device_name].index:
                            output_device_map[device_name] = output_device
                    else:
                        output_devices.append(output_device)
                        
            except Exception as e:
                print(f"获取设备 {i} 信息时出错: {e}")
                continue
        
        # 如果启用去重，从字典中提取设备列表
        if deduplicate:
            input_devices = list(input_device_map.values())
            output_devices = list(output_device_map.values())
            
            # 按索引排序
            input_devices.sort(key=lambda x: x.index)
            output_devices.sort(key=lambda x: x.index)
        
        return input_devices, output_devices
        
    finally:
        p.terminate()

def get_input_devices(deduplicate: bool = True) -> List[InputDevice]:
    """
    仅获取输入设备列表
    
    Args:
        deduplicate: 是否去重，选择最小索引
    
    Returns:
        List[InputDevice]: 输入设备列表
    """
    input_devices, _ = get_audio_devices(deduplicate=deduplicate)
    return input_devices

def get_output_devices(deduplicate: bool = True) -> List[OutputDevice]:
    """
    仅获取输出设备列表
    
    Args:
        deduplicate: 是否去重，选择最小索引
    
    Returns:
        List[OutputDevice]: 输出设备列表
    """
    _, output_devices = get_audio_devices(deduplicate=deduplicate)
    return output_devices

def find_device_by_name(device_name: str, device_type: str = "both") -> Tuple[int, int]:
    """
    根据设备名称查找最小索引
    
    Args:
        device_name: 设备名称（支持部分匹配）
        device_type: "input", "output", 或 "both"
    
    Returns:
        Tuple[int, int]: (input_index, output_index)，如果没找到则为None
    """
    input_devices, output_devices = get_audio_devices(deduplicate=True)
    
    input_index = None
    output_index = None
    
    if device_type in ["input", "both"]:
        for device in input_devices:
            if device_name in device.name:
                input_index = device.index
                break
    
    if device_type in ["output", "both"]:
        for device in output_devices:
            if device_name in device.name:
                output_index = device.index
                break
    
    return input_index, output_index

# 使用示例
if __name__ == "__main__":
    print("=== 去重后的设备列表（最小索引）===")
    
    # 获取去重后的设备
    input_devices, output_devices = get_audio_devices(deduplicate=True)
    
    print("输入设备:")
    for device in input_devices:
        print(f"  索引 {device.index}: {device.name} - {device.defaultSampleRate}Hz")
    
    print("\n输出设备:")
    for device in output_devices:
        print(f"  索引 {device.index}: {device.name} - {device.defaultSampleRate}Hz")
    