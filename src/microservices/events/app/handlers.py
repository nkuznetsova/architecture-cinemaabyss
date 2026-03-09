import logging

from app.kafka.consumer import KafkaConsumerClient
from app.config import settings

logger = logging.getLogger(__name__)

# Глобальная переменная для consumer
_consumer: KafkaConsumerClient | None = None


async def handle_events(event: dict, event_type: str):
    """
    Обработчик событий.
    """
    logger.info(f"Processing {event_type}, event: {event}")

    try:
        # Эмуляция работы
        logger.info(f"{event_type} processed successfully")

    except Exception as e:
        logger.error(f"Failed to process event {event_type}: {e}")
        raise  # Retry через Kafka



async def start_consumer():
    """Запускает consumer для обработки заказов"""
    global _consumer

    _consumer = KafkaConsumerClient(
        topics=[
            settings.movie_topic,
            settings.users_topic,
            settings.payment_topic,
        ],
        handler=handle_events,
    )

    await _consumer.start()


async def stop_consumer():
    """Останавливает consumer"""
    if _consumer:
        await _consumer.stop()
