import json
import logging
from typing import Any, Optional

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

from app.config import settings

logger = logging.getLogger(__name__)


class KafkaProducerClient:
    """Singleton Kafka Producer для отправки событий."""

    def __init__(self):
        self.producer: Optional[AIOKafkaProducer] = None

    async def start(self):
        """Создаёт и запускает producer"""
        self.producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            # Гарантии доставки
            acks='all',  # Ждём подтверждения от всех реплик
            # Производительность
            compression_type='gzip',
            max_batch_size=16384,
            linger_ms=10,  # Ждём 10ms для батчинга
        )
        await self.producer.start()
        logger.info("Kafka Producer started")

    async def stop(self):
        """Останавливает producer"""
        if self.producer:
            await self.producer.stop()
            logger.info("Kafka Producer stopped")

    async def send(
            self,
            topic: str,
            value: dict[str, Any],
            key: Optional[str] = None,
    ) -> None:
        """
        Отправляет событие в Kafka.

        Args:
            topic: Имя топика
            value: Payload события (будет сериализован в JSON)
            key: Ключ для партиционирования (optional)
        """
        if not self.producer:
            raise RuntimeError("Producer not started")

        try:
            # Ключ определяет партицию (события с одним ключом → одна партиция)
            key_bytes = key.encode('utf-8') if key else None

            await self.producer.send_and_wait(
                topic=topic,
                value=value,
                key=key_bytes,
            )

            logger.debug(f"Sent to {topic}: {value}")

        except KafkaError as e:
            logger.error(f"Failed to send to {topic}: {e}")
            raise


# Singleton instance
_producer_client: Optional[KafkaProducerClient] = None


async def get_kafka_producer() -> KafkaProducerClient:
    """Dependency injection для FastAPI"""
    global _producer_client

    if _producer_client is None:
        _producer_client = KafkaProducerClient()
        await _producer_client.start()

    return _producer_client
