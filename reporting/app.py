# reporting/app.py
import os
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
import asyncpg
from datetime import date, datetime

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://analytics:analytics@localhost:5432/analytics")

app = FastAPI(title="Analytics Reporting")
db = None

class StatsResponse(BaseModel):
    site_id: str
    date: str
    total_views: int
    unique_users: int
    top_paths: list

@app.on_event("startup")
async def startup():
    global db
    db = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)

@app.on_event("shutdown")
async def shutdown():
    global db
    await db.close()

@app.get("/stats", response_model=StatsResponse)
async def get_stats(site_id: str = Query(...), date_str: str | None = Query(None)):
    """
    Returns aggregates for given site_id and optional date (YYYY-MM-DD). If no date provided, returns today's date.
    """
    try:
        if date_str:
            the_date = date.fromisoformat(date_str)
        else:
            the_date = date.today()
    except Exception:
        raise HTTPException(status_code=400, detail="date must be YYYY-MM-DD")

    async with db.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT total_views, unique_users FROM daily_stats
            WHERE site_id = $1 AND date = $2
        """, site_id, the_date)
        if not row:
            total_views = 0
            unique_users = 0
        else:
            total_views = row["total_views"]
            unique_users = row["unique_users"]

        rows = await conn.fetch("""
            SELECT path, views FROM daily_path_views
            WHERE site_id = $1 AND date = $2
            ORDER BY views DESC
            LIMIT 10
        """, site_id, the_date)

        top_paths = [{"path": r["path"], "views": r["views"]} for r in rows]

    return {
        "site_id": site_id,
        "date": the_date.isoformat(),
        "total_views": total_views,
        "unique_users": unique_users,
        "top_paths": top_paths
    }
