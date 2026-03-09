from fastapi import APIRouter, Depends, status
from pydantic import BaseModel

from app.kafka.producer import KafkaProducerClient, get_kafka_producer
from app.config import settings

router = APIRouter(prefix="/api/events", tags=["events"])


class MovieCreate(BaseModel):
    movie_id: int
    title: str
    action: str
    user_id: int


class UserEvent(BaseModel):
    user_id: int
    username: str
    action: str
    timestamp: str


class PaymentEvent(BaseModel):
    payment_id: int
    user_id: int
    amount: float
    status: str
    timestamp: str
    method_type: str


class ApiResponse(BaseModel):
    id: int
    status: str = "success"


@router.get("/health")
async def health():
    """Health check"""
    return {
        "status": True,
        "message": "Proxy API is running",
    }


@router.post("/movie", response_model=ApiResponse, status_code=status.HTTP_201_CREATED)
async def create_movie_event(
        movie: MovieCreate,
        producer: KafkaProducerClient = Depends(get_kafka_producer),
):
    """
    Создаёт фильм и отправляет событие в Kafka.

    Возвращает 201.
    """

    movie_id = movie.movie_id

    # Отправляем событие в Kafka
    await producer.send(
        topic=settings.movie_topic,
        value={
            "event_type": "movie.created",
            "movie_id": movie_id,
            "user_id": movie.user_id,
            "title": movie.title,
            "action": movie.action,
        },
        key=str(movie.user_id),  # Партиция по user_id
    )

    return ApiResponse(id=movie_id)


@router.post("/user", response_model=ApiResponse, status_code=status.HTTP_201_CREATED)
async def create_user_event(
        user: UserEvent,
        producer: KafkaProducerClient = Depends(get_kafka_producer),
):
    """
    Создаёт событие действия пользователя и отправляет событие в Kafka.

    Возвращает 201.
    """

    user_id = user.user_id

    # Отправляем событие в Kafka
    await producer.send(
        topic=settings.users_topic,
        value={
            "event_type": "user.event",
            "user_id": user_id,
            "username": user.username,
            "timestamp": user.timestamp,
            "action": user.action,
        },
        key=str(user.user_id),  # Партиция по user_id
    )

    return ApiResponse(id=user_id)


@router.post("/payment", response_model=ApiResponse, status_code=status.HTTP_201_CREATED)
async def create_payment_event(
        payment: PaymentEvent,
        producer: KafkaProducerClient = Depends(get_kafka_producer),
):
    """
    Создаёт событие оплаты и отправляет событие в Kafka.

    Возвращает 201.
    """

    payment_id = payment.payment_id

    # Отправляем событие в Kafka
    await producer.send(
        topic=settings.users_topic,
        value={
            "event_type": "payment.event",
            "payment_id": payment_id,
            "user_id": payment.user_id,
            "timestamp": payment.timestamp,
            "amount": payment.amount,
            "status": payment.status,
            "method_type": payment.method_type,
        },
        key=str(payment_id),  # Партиция по user_id
    )

    return ApiResponse(id=payment_id)
