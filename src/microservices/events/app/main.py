import os
import uvicorn
from contextlib import asynccontextmanager

from fastapi import FastAPI

from .config import settings
from .kafka.producer import get_kafka_producer, _producer_client
from .handlers import start_consumer, stop_consumer
from .routers import router


@asynccontextmanager
async def lifespan(_app: FastAPI):
    """Lifecycle: startup and shutdown"""

    # Startup: запускаем Kafka Producer
    await get_kafka_producer()

    # Startup: запускаем Kafka Consumers
    await start_consumer()

    yield

    # Shutdown: останавливаем Producer и Consumers
    if _producer_client:
        await _producer_client.stop()

    await stop_consumer()


app = FastAPI(
    title=settings.app_name,
    lifespan=lifespan,
)

# Подключаем роутеры
app.include_router(router)


if __name__ == "__main__":
    api_port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=api_port)
