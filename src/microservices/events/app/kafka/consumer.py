import asyncio
import json
import logging
from typing import Callable, Awaitable

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError

from app.config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class KafkaConsumerClient:
    """
    Kafka Consumer для чтения событий.
    """

    def __init__(
            self,
            topics: list[str],
            handler: Callable[[dict, str], Awaitable[None]],
            group_id: str | None = None,
    ):
        self.topics = topics
        self.handler = handler
        self.group_id = group_id or settings.kafka_consumer_group
        self.consumer: AIOKafkaConsumer | None = None
        self._running = False

    async def start(self):
        """Создаёт consumer и начинает обработку"""
        self.consumer = AIOKafkaConsumer(
            *self.topics,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset=settings.kafka_auto_offset_reset,
            enable_auto_commit=False,  # Ручной commit для надёжности
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        )

        await self.consumer.start()
        logger.info(f"Consumer started for topics: {', '.join(self.topics)}")

        self._running = True
        asyncio.create_task(self._consume_loop())

    async def stop(self):
        """Останавливает consumer"""
        self._running = False
        if self.consumer:
            await self.consumer.stop()
            logger.info(f"Consumer stopped for topic: {self.topics}")

    async def _consume_loop(self):
        """Основной цикл чтения событий"""
        try:
            async for message in self.consumer:
                try:
                    # Обрабатываем событие
                    await self.handler(message.value, message.topic)

                    # Commit offset после успешной обработки
                    await self.consumer.commit()

                    logger.debug(
                        f"Processed message from {message.topic}: "
                        f"partition={message.partition}, offset={message.offset}"
                    )

                except Exception as e:
                    logger.error(f"Error processing message: {e}", exc_info=True)
                    # Не делаем commit → сообщение будет перечитано

        except KafkaError as e:
            logger.error(f"Kafka error: {e}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
