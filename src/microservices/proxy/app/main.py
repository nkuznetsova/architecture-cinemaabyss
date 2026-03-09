import os
import httpx
import random
import uvicorn
from fastapi import FastAPI, Request, Response

app = FastAPI(title="Cinemaabyss")

MONOLITH_URL = os.getenv('MONOLITH_URL', 'http://localhost:8080')
MOVIES_SERVICE_URL = os.getenv('MOVIES_SERVICE_URL')
USERS_SERVICE_URL = os.getenv('USERS_SERVICE_URL')
EVENTS_SERVICE_URL = os.getenv('EVENTS_SERVICE_URL')
MOVIES_MIGRATION_PERCENT = int(os.getenv('MOVIES_MIGRATION_PERCENT'))


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "message": "Proxy API is running",
    }


@app.api_route(
    "/{path:path}",
    methods=["GET", "POST", "PUT", "DELETE", "PATCH"],
)
async def redirect(path: str, request: Request):
    rnd = random.randint(1, 100)
    use_monolith = False if rnd <= MOVIES_MIGRATION_PERCENT else True
    if use_monolith:
        base_url = MONOLITH_URL
        print('use monolith')
    else:
        match path:
            case s if s.startswith("api/movies"):
                base_url = MOVIES_SERVICE_URL or MONOLITH_URL
            case s if s.startswith("api/users"):
                base_url = USERS_SERVICE_URL or MONOLITH_URL
                print(base_url)
            case s if s.startswith("api/events"):
                base_url = EVENTS_SERVICE_URL or MONOLITH_URL
            case _:
                base_url = MONOLITH_URL

    target_url = f"{base_url}/{path}"
    if query_str := request.url.query:
        target_url += f"?{query_str}"

    async with httpx.AsyncClient() as client:
        response = await client.request(
            method=request.method,
            url=target_url,
            params=request.query_params,
            content=await request.body()
        )
        return Response(
            content=response.content,
            status_code=response.status_code,
            headers=dict(response.headers)
        )


if __name__ == "__main__":
    api_port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=int(api_port) if api_port else 8000)
