"""配置类"""
from typing import List
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field


class Settings(BaseSettings):
    """配置类"""

    app_name: str = Field(default="AI Tele", description="Application name")
    debug: bool = Field(default=False, description="Debug mode")
    log_level: str = Field(default="INFO", description="Log level")
    computer_config_id: str = Field(default="", description="Computer config ID")
    instance: int = Field(default=6, description="Instance")

    # SSH Tunnel Configuration
    ssh_host: str | None = Field(default=None, description="SSH server host")
    ssh_port: int = Field(default=22, description="SSH server port")
    ssh_username: str | None = Field(default=None, description="SSH username")
    ssh_password: str | None = Field(default=None, description="SSH password")
    ssh_key_path: str | None = Field(default=None, description="SSH private key path")
    ssh_remote_host: str = Field(default="localhost", description="Remote database host")
    ssh_remote_port: int = Field(default=3306, description="Remote database port")
    ssh_local_port: int = Field(default=3307, description="Local tunnel port")

    # Database
    db_username: str | None = Field(default=None, description="Database username")
    db_password: str | None = Field(default=None, description="Database password")
    db_host: str | None = Field(default=None, description="Database host")
    db_port: str | None = Field(default=None, description="Database port")
    db_name: str | None = Field(default=None, description="Database name")

    # OpenAI Configuration
    openai_url: str | None = Field(default=None, description="OpenAI URL")
    openai_api_key: str | None = Field(default=None, description="OpenAI API key")

    # TTS Configuration
    tts_appid_c: str | None = Field(default=None, description="TTS appid")
    tts_apikey_c: str | None = Field(default=None, description="TTS apikey")
    tts_apisecret_c: str | None = Field(default=None, description="TTS apisecret")

    # RTASR Configuration
    rtasr_appid: str | None = Field(default=None, description="RTASR appid")
    rtasr_api_key: str | None = Field(default=None, description="RTASR api key")

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
    redis_username: str = Field(default="default", description="Redis username")
    redis_password: str | None = Field(default=None, description="Redis password")
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

    model_config = SettingsConfigDict(env_file=".env", arbitrary_types_allowed=True)

settings = Settings()
