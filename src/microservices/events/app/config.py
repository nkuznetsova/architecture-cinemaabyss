import os
from pydantic_settings import BaseSettings, SettingsConfigDict


if kafka_brokers := os.getenv("KAFKA_BROKERS"):
    kafka_brokers = kafka_brokers.split(',')
else:
    kafka_brokers = ["localhost:9092"]


class Settings(BaseSettings):
    # Kafka
    kafka_bootstrap_servers: list[str] = kafka_brokers
    kafka_consumer_group: str = "fastapi-app"
    kafka_auto_offset_reset: str = "latest"  # "earliest" или "latest"

    # Topics
    users_topic: str = "user-events"
    payment_topic: str = "payment-events"
    movie_topic: str = "movie-events"

    # App
    app_name: str = "Kafka FastAPI"
    debug: bool = False

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )


settings = Settings()
