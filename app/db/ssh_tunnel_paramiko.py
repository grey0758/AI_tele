import socket
import threading
import time
from contextlib import contextmanager
from typing import Optional
from app.core.logger import logger

try:
    import paramiko
except ImportError:
    logger.error("paramiko not installed. Please install it with: pip install paramiko")
    paramiko = None


class SSHTunnelParamiko:
    """基于paramiko的SSH隧道管理器"""
    
    def __init__(
        self,
        ssh_host: str,
        ssh_port: int = 22,
        ssh_username: str = None,
        ssh_password: str = None,
        ssh_key_path: str = None,
        remote_host: str = "localhost",
        remote_port: int = 3306,  # MySQL默认端口
        local_port: int = 3307    # 本地MySQL端口
    ):
        if paramiko is None:
            raise ImportError("paramiko is required for SSH tunnel functionality")
            
        self.ssh_host = ssh_host
        self.ssh_port = ssh_port
        self.ssh_username = ssh_username
        self.ssh_password = ssh_password
        self.ssh_key_path = ssh_key_path
        self.remote_host = remote_host
        self.remote_port = remote_port
        self.local_port = local_port
        
        self.ssh_client = None
        self.transport = None
        self.local_socket = None
        self.tunnel_thread = None
        self.stop_event = threading.Event()
        
    def _connect_ssh(self) -> bool:
        """建立SSH连接"""
        try:
            self.ssh_client = paramiko.SSHClient()
            self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            # 准备连接参数
            connect_kwargs = {
                'hostname': self.ssh_host,
                'port': self.ssh_port,
                'timeout': 30,
                'allow_agent': False,
                'look_for_keys': False
            }
            
            if self.ssh_username:
                connect_kwargs['username'] = self.ssh_username
                
            if self.ssh_password:
                connect_kwargs['password'] = self.ssh_password
                
            if self.ssh_key_path:
                connect_kwargs['key_filename'] = self.ssh_key_path
                connect_kwargs['look_for_keys'] = True
                
            logger.info(f"Connecting to SSH server: {self.ssh_host}:{self.ssh_port}")
            self.ssh_client.connect(**connect_kwargs)
            
            self.transport = self.ssh_client.get_transport()
            logger.info("SSH connection established successfully")
            
            # 添加远程服务器诊断
            self._diagnose_remote_server()
            
            return True
            
        except paramiko.AuthenticationException as e:
            logger.error(f"SSH authentication failed: {e}")
            return False
        except paramiko.SSHException as e:
            logger.error(f"SSH connection error: {e}")
            return False
        except socket.error as e:
            logger.error(f"Socket error during SSH connection: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error during SSH connection: {e}")
            return False
    
    def _diagnose_remote_server(self):
        """诊断远程服务器上的MySQL服务状态"""
        try:
            logger.info("Diagnosing remote MySQL server...")
            
            # 检查MySQL进程
            stdin, stdout, stderr = self.ssh_client.exec_command("ps aux | grep mysql | grep -v grep")
            mysql_processes = stdout.read().decode().strip()
            if mysql_processes:
                logger.info(f"MySQL processes found:\n{mysql_processes}")
            else:
                logger.warning("No MySQL processes found on remote server")
            
            # 检查MySQL服务状态
            stdin, stdout, stderr = self.ssh_client.exec_command("systemctl status mysql 2>/dev/null || systemctl status mysqld 2>/dev/null || service mysql status 2>/dev/null")
            service_status = stdout.read().decode().strip()
            if service_status:
                logger.info(f"MySQL service status:\n{service_status}")
            
            # 检查端口监听
            stdin, stdout, stderr = self.ssh_client.exec_command(f"netstat -tlnp | grep :{self.remote_port} || ss -tlnp | grep :{self.remote_port}")
            port_status = stdout.read().decode().strip()
            if port_status:
                logger.info(f"Port {self.remote_port} status:\n{port_status}")
            else:
                logger.warning(f"Port {self.remote_port} is not listening on remote server")
            
            # 检查MySQL配置文件中的bind-address
            stdin, stdout, stderr = self.ssh_client.exec_command("grep -r 'bind-address' /etc/mysql/ 2>/dev/null || grep -r 'bind-address' /etc/my.cnf* 2>/dev/null")
            bind_config = stdout.read().decode().strip()
            if bind_config:
                logger.info(f"MySQL bind-address configuration:\n{bind_config}")
            
            # 尝试本地连接测试
            stdin, stdout, stderr = self.ssh_client.exec_command(f"nc -z localhost {self.remote_port} && echo 'Connection successful' || echo 'Connection failed'")
            connection_test = stdout.read().decode().strip()
            logger.info(f"Local connection test to MySQL: {connection_test}")
            
        except Exception as e:
            logger.error(f"Error during remote server diagnosis: {e}")
    
    def _handle_connection(self, client_socket, client_addr):
        """处理单个客户端连接"""
        remote_channel = None
        try:
            logger.debug(f"Handling connection from {client_addr}")
            
            # 通过SSH隧道创建到远程MySQL服务器的连接
            remote_channel = self.transport.open_channel(
                'direct-tcpip',
                (self.remote_host, self.remote_port),
                client_addr
            )
            
            logger.debug(f"Opened channel to {self.remote_host}:{self.remote_port}")
            
            # 创建双向数据转发线程
            def forward_data(src, dst, direction):
                try:
                    while not self.stop_event.is_set():
                        try:
                            data = src.recv(4096)
                            if not data:
                                logger.debug(f"No data received, closing {direction}")
                                break
                            dst.send(data)
                            logger.debug(f"Forwarded {len(data)} bytes ({direction})")
                        except socket.timeout:
                            continue
                        except Exception as e:
                            logger.debug(f"Error in data forwarding ({direction}): {e}")
                            break
                except Exception as e:
                    logger.debug(f"Data forwarding thread error ({direction}): {e}")
                finally:
                    try:
                        src.close()
                        dst.close()
                    except:
                        pass
            
            # 设置socket超时
            client_socket.settimeout(1.0)
            remote_channel.settimeout(1.0)
            
            # 启动双向转发
            forward_thread1 = threading.Thread(
                target=forward_data, 
                args=(client_socket, remote_channel, "client->remote"),
                daemon=True
            )
            forward_thread2 = threading.Thread(
                target=forward_data, 
                args=(remote_channel, client_socket, "remote->client"),
                daemon=True
            )
            
            forward_thread1.start()
            forward_thread2.start()
            
            # 等待转发完成
            forward_thread1.join()
            forward_thread2.join()
            
            logger.debug(f"Connection handling completed for {client_addr}")
            
        except Exception as e:
            logger.error(f"Error handling connection from {client_addr}: {e}")
        finally:
            try:
                if remote_channel:
                    remote_channel.close()
                client_socket.close()
            except:
                pass
            
    def _create_tunnel(self) -> bool:
        """创建SSH隧道"""
        try:
            if not self.transport:
                logger.error("SSH transport not available")
                return False
            
            # 创建本地监听socket
            self.local_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.local_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.local_socket.bind(('localhost', self.local_port))
            self.local_socket.listen(5)
            self.local_socket.settimeout(1.0)  # 设置超时以便检查停止事件
            
            logger.info(f"SSH tunnel created: localhost:{self.local_port} -> {self.remote_host}:{self.remote_port}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create SSH tunnel: {e}")
            return False
            
    def _tunnel_worker(self):
        """隧道工作线程"""
        try:
            logger.info(f"Tunnel worker started, listening on localhost:{self.local_port}")
            
            while not self.stop_event.is_set():
                try:
                    if not self.transport or not self.transport.is_active():
                        logger.warning("SSH transport is not active")
                        break
                        
                    # 接受客户端连接
                    client_socket, client_addr = self.local_socket.accept()
                    logger.debug(f"Accepted connection from {client_addr}")
                    
                    # 在新线程中处理连接
                    connection_thread = threading.Thread(
                        target=self._handle_connection,
                        args=(client_socket, client_addr),
                        daemon=True
                    )
                    connection_thread.start()
                    
                except socket.timeout:
                    # 超时是正常的，继续循环
                    continue
                except Exception as e:
                    if not self.stop_event.is_set():
                        logger.error(f"Tunnel worker error: {e}")
                    break
                    
        except Exception as e:
            logger.error(f"Fatal tunnel worker error: {e}")
        finally:
            logger.info("Tunnel worker stopped")
            
    def start(self) -> bool:
        """启动SSH隧道"""
        try:
            # 建立SSH连接
            if not self._connect_ssh():
                return False
                
            # 创建隧道
            if not self._create_tunnel():
                self.stop()
                return False
                
            # 启动隧道工作线程
            self.stop_event.clear()
            self.tunnel_thread = threading.Thread(target=self._tunnel_worker, daemon=True)
            self.tunnel_thread.start()
            
            # 等待隧道建立
            time.sleep(2)
            
            # 验证隧道是否工作
            if self._check_tunnel():
                logger.info("SSH tunnel started successfully")
                return True
            else:
                logger.error("SSH tunnel verification failed")
                self.stop()
                return False
                
        except Exception as e:
            logger.error(f"Error starting SSH tunnel: {e}")
            self.stop()
            return False
            
    def stop(self):
        """停止SSH隧道"""
        try:
            # 设置停止事件
            self.stop_event.set()
            
            # 关闭本地socket
            if self.local_socket:
                try:
                    self.local_socket.close()
                except:
                    pass
                self.local_socket = None
                
            # 等待隧道线程结束
            if self.tunnel_thread:
                self.tunnel_thread.join(timeout=5)
                self.tunnel_thread = None
                
            # 关闭SSH连接
            if self.ssh_client:
                self.ssh_client.close()
                self.ssh_client = None
                self.transport = None
                
            logger.info("SSH tunnel stopped")
            
        except Exception as e:
            logger.error(f"Error stopping SSH tunnel: {e}")
            
    def _check_tunnel(self) -> bool:
        """检查隧道是否建立成功"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex(("localhost", self.local_port))
            sock.close()
            return result == 0
        except Exception:
            return False
            
    def is_active(self) -> bool:
        """检查隧道是否活跃"""
        if not self.transport:
            return False
        return self.transport.is_active() and not self.stop_event.is_set()
        
    @contextmanager
    def tunnel(self):
        """上下文管理器，自动管理隧道生命周期"""
        try:
            if self.start():
                yield self
            else:
                raise Exception("Failed to start SSH tunnel")
        finally:
            self.stop()


def create_ssh_tunnel_paramiko_from_env() -> Optional[SSHTunnelParamiko]:
    """从环境变量创建基于paramiko的SSH隧道"""
    from app.core.config import settings
    
    if not settings.ssh_host:
        return None
        
    # 优先使用ssh_username，如果没有则使用ssh_user
    username = settings.ssh_username or settings.ssh_user
        
    return SSHTunnelParamiko(
        ssh_host=settings.ssh_host,
        ssh_port=settings.ssh_port,
        ssh_username=username,
        ssh_password=settings.ssh_password,
        ssh_key_path=settings.ssh_key_path,
        remote_host=settings.ssh_remote_host,
        remote_port=settings.ssh_remote_port,  # 应该是3306
        local_port=settings.ssh_local_port     # 应该是3307
    )
