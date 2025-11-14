import os
import asyncio
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field
from datetime import datetime
from redis.asyncio import Redis  # ✅ REPLACEMENT FOR aioredis

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
STREAM_KEY = "analytics:events"

app = FastAPI(title="Analytics Ingestion")

# Pydantic model
class Event(BaseModel):
    site_id: str = Field(..., min_length=1)
    event_type: str = Field(..., min_length=1)
    path: str = "/"
    user_id: str | None = None
    timestamp: datetime

# Redis client
redis: Redis | None = None


@app.on_event("startup")
async def startup():
    """
    Create Redis connection on startup
    """
    global redis
    redis = Redis.from_url(REDIS_URL, decode_responses=True)  # ✅ correct way


@app.on_event("shutdown")
async def shutdown():
    """
    Close Redis connection
    """
    global redis
    if redis:
        await redis.aclose()  # ✅ correct shutdown


@app.post("/event", status_code=202)
async def post_event(event: Event, request: Request):
    """
    Validate and enqueue event to Redis Stream as quickly as possible.
    Returns 202 Accepted immediately after enqueue.
    """
    payload = {
        "site_id": event.site_id,
        "event_type": event.event_type,
        "path": event.path,
        "user_id": event.user_id or "",
        "timestamp": event.timestamp.isoformat()
    }

    try:
        # XADD with '*' id for server-generated ID
        await redis.xadd(STREAM_KEY, payload)
    except Exception as e:
        raise HTTPException(status_code=500, detail="enqueue failed")

    return {"status": "accepted"}
