from typing import List, Optional
from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    app_name: str = Field(default="AI Tele", description="Application name")
    debug: bool = Field(default=False, description="Debug mode")
    log_level: str = Field(default="INFO", description="Log level")
    computer_config_id: str = Field(default="", description="Computer config ID")
    
    # Database
    database_url: str = Field(default="sqlite:///./ai_tele.db", description="Database connection URL")
    
    # SSH Tunnel Configuration
    ssh_host: Optional[str] = Field(default=None, description="SSH server host")
    ssh_port: int = Field(default=22, description="SSH server port")
    ssh_username: Optional[str] = Field(default=None, description="SSH username")
    ssh_password: Optional[str] = Field(default=None, description="SSH password")
    ssh_key_path: Optional[str] = Field(default=None, description="SSH private key path")
    ssh_remote_host: str = Field(default="localhost", description="Remote database host")
    ssh_remote_port: int = Field(default=3306, description="Remote database port")
    ssh_local_port: int = Field(default=3307, description="Local tunnel port")
    
    # Database Connection (for SSH tunnel)
    db_username: Optional[str] = Field(default=None, description="Database username")
    db_password: Optional[str] = Field(default=None, description="Database password")
    db_name: Optional[str] = Field(default=None, description="Database name")
    
    # Additional database fields from .env
    db_host: Optional[str] = Field(default=None, description="Database host")
    db_port: Optional[str] = Field(default=None, description="Database port")
    db_user: Optional[str] = Field(default=None, description="Database user")
    db_name2: Optional[str] = Field(default=None, description="Database name 2")
    db_name3: Optional[str] = Field(default=None, description="Database name 3")
    
    # SSH user (alternative to ssh_username)
    ssh_user: Optional[str] = Field(default=None, description="SSH user")
    
    # OpenAI Configuration
    openai_url: Optional[str] = Field(default=None, description="OpenAI URL")
    openai_api_key: Optional[str] = Field(default=None, description="OpenAI API key")

    # TTS Configuration
    tts_appid_c: Optional[str] = Field(default=None, description="TTS appid")
    tts_apikey_c: Optional[str] = Field(default=None, description="TTS apikey")
    tts_apisecret_c: Optional[str] = Field(default=None, description="TTS apisecret")
    
    # RTASR Configuration
    rtasr_appid: Optional[str] = Field(default=None, description="RTASR appid")
    rtasr_api_key: Optional[str] = Field(default=None, description="RTASR api key")
    
    # CORS
    allowed_origins: List[str] = Field(default=["http://localhost:3000"], description="Allowed CORS origins")
    
    # File Upload
    max_file_size: int = Field(default=10485760, description="Maximum file size in bytes")
    upload_dir: str = Field(default="./uploads", description="Upload directory")
    
    # Server Configuration
    host: str = Field(default="0.0.0.0", description="Server host")
    port: int = Field(default=8020, description="Server port")

    # Redis Configuration
    redis_host: str = Field(default="localhost", description="Redis host")
    redis_port: int = Field(default=6379, description="Redis port")
    redis_db: int = Field(default=0, description="Redis database")
    redis_username: Optional[str] = Field(default="default", description="Redis username")
    redis_password: Optional[str] = Field(default=None, description="Redis password")
    redis_ssl: bool = Field(default=False, description="Redis SSL")
    redis_decode_responses: bool = Field(default=True, description="Redis decode responses")
    
    # WebSocket Configuration
    websocket_ping_interval: int = Field(default=20, description="WebSocket ping interval in seconds")
    websocket_ping_timeout: int = Field(default=5, description="WebSocket ping timeout in seconds")
    websocket_heartbeat_interval: int = Field(default=25, description="WebSocket heartbeat interval in seconds")
    websocket_connection_timeout: int = Field(default=60, description="WebSocket connection timeout in seconds")
    websocket_max_reconnect_attempts: int = Field(default=5, description="WebSocket max reconnect attempts")
    websocket_reconnect_delay: int = Field(default=2, description="WebSocket reconnect delay in seconds")
    websocket_max_reconnect_delay: int = Field(default=60, description="WebSocket max reconnect delay in seconds")
    
    # EventBus Configuration
    worker_count: int = Field(default=4, description="Number of event processing workers")
    max_queue_size: int = Field(default=1000, description="Maximum size of each event queue")
    dead_letter_queue_size: int = Field(default=100, description="Maximum size of dead letter queue")
    default_wait_for_result: bool = Field(default=True, description="Default wait for result")
    default_timeout: float = Field(default=30.0, description="Default event timeout in seconds")
    max_retry_count: int = Field(default=3, description="Maximum number of event retries")
    retry_delay: float = Field(default=1.0, description="Base delay between retries in seconds")
    health_check_interval: int = Field(default=30, description="Health check interval in seconds")
    enable_persistence: bool = Field(default=True, description="Enable event persistence to disk")
    persistence_path: str = Field(default="./logs/events", description="Path for event persistence")

    #ai tele record service
    ai_tele_record_service_url: str = Field(default="http://localhost:8081", description="AI Tele record service URL")
    
    class Config:
        env_file = ".env"
        case_sensitive = False


settings = Settings()
