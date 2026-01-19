from typing import Annotated, Optional

from fastapi import FastAPI, Depends, Query, Path, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from api.database import get_db
from api import schemas
from api import queries

app = FastAPI(
    title="Telegram Medical Analytics API",
    version="1.0.0",
    description=(
        "Task 4 Analytical API backed by dbt marts in PostgreSQL. "
        "Provides curated analytics endpoints for non-technical stakeholders."
    ),
)


@app.get(
    "/health",
    response_model=schemas.HealthResponse,
    summary="Service health check",
    description="Returns API liveness and verifies database connectivity via a lightweight SQL query.",
    tags=["health"],
)
def health(db: Session = Depends(get_db)):
    try:
        db.execute(queries.q_health()).fetchone()
        return schemas.HealthResponse(ok=True, database="connected")
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Health failed: {repr(e)}")


@app.get(
    "/api/reports/top-products",
    response_model=schemas.TopProductsResponse,
    summary="Top products/terms",
    description=(
        "Returns the most frequently mentioned terms/products across all channels. "
        "This is a lightweight term-frequency report computed from message text."
    ),
    tags=["reports"],
)
def top_products(
    limit: Annotated[
        int,
        Query(
            ge=1,
            le=100,
            description="Number of terms to return (1–100).",
        ),
    ] = 10,
    db: Session = Depends(get_db),
):
    try:
        rows = db.execute(queries.q_top_products(), {
                          "limit": limit}).fetchall()
        items = [schemas.TopProductItem(
            term=r.term, mentions=r.mentions) for r in rows]
        return schemas.TopProductsResponse(limit=limit, items=items)
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")


@app.get(
    "/api/channels/{channel_name}/activity",
    response_model=schemas.ChannelActivityResponse,
    summary="Channel activity time series",
    description=(
        "Returns posting activity and trends for a specific channel. "
        "Includes message count and average views aggregated by the requested time grain."
    ),
    tags=["channels"],
)
def channel_activity(
    channel_name: Annotated[
        str,
        Path(
            ...,
            min_length=1,
            description="Exact channel name to analyze (must exist in the marts).",
        ),
    ],
    grain: Annotated[
        str,
        Query(
            pattern="^(day|week|month)$",
            description="Aggregation grain for the time series: day, week, or month.",
        ),
    ] = "day",
    days: Annotated[
        int,
        Query(
            ge=1,
            le=365,
            description="Lookback window in days (1–365).",
        ),
    ] = 30,
    db: Session = Depends(get_db),
):
    try:
        q = queries.q_channel_activity(grain=grain)
        rows = db.execute(
            q, {"channel_name": channel_name, "days": days}).fetchall()

        points = [
            schemas.ActivityPoint(
                period_start=r.period_start,
                message_count=r.message_count,
                avg_views=r.avg_views,
            )
            for r in rows
        ]

        return schemas.ChannelActivityResponse(
            channel_name=channel_name,
            grain=grain,
            days=days,
            points=points,
        )
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")


@app.get(
    "/api/search/messages",
    response_model=schemas.MessageSearchResponse,
    summary="Search messages by keyword",
    description=(
        "Searches messages containing a keyword using a case-insensitive match against message text. "
        "Supports optional channel filter and offset/limit pagination."
    ),
    tags=["search"],
)
def search_messages(
    q: Annotated[
        str,
        Query(
            ...,
            min_length=1,
            description="Search term (case-insensitive ILIKE match on message_text).",
        ),
    ],
    channel_name: Annotated[
        Optional[str],
        Query(
            min_length=1,
            description="Optional filter to return results from a single channel only.",
        ),
    ] = None,
    limit: Annotated[
        int,
        Query(
            ge=1,
            le=100,
            description="Maximum number of results to return (1–100).",
        ),
    ] = 20,
    offset: Annotated[
        int,
        Query(
            ge=0,
            description="Pagination offset (0 or greater).",
        ),
    ] = 0,
    db: Session = Depends(get_db),
):
    try:
        rows = db.execute(
            queries.q_search_messages(),
            {
                "q": q,
                "channel_name": channel_name,
                "limit": limit,
                "offset": offset,
            },
        ).fetchall()

        items = [
            schemas.MessageSearchItem(
                message_id=r.message_id,
                channel_name=r.channel_name,
                message_ts=r.message_ts,
                message_text=r.message_text,
                message_length=r.message_length,
                views=r.views,
                forwards=r.forwards,
                reply_count=r.reply_count,
                has_image=r.has_image,
            )
            for r in rows
        ]

        return schemas.MessageSearchResponse(
            q=q,
            channel_name=channel_name,
            limit=limit,
            offset=offset,
            items=items,
        )
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")


@app.get(
    "/api/reports/visual-content",
    response_model=schemas.VisualContentResponse,
    summary="Visual content statistics",
    description=(
        "Returns statistics about image usage across channels. "
        "Includes total unique images and a breakdown by image category."
    ),
    tags=["reports"],
)
def visual_content(
    channel_name: Annotated[
        Optional[str],
        Query(
            min_length=1,
            description="Optional filter by channel_name to scope the statistics.",
        ),
    ] = None,
    db: Session = Depends(get_db),
):
    try:
        total_row = db.execute(
            queries.q_visual_total_images(),
            {"channel_name": channel_name},
        ).fetchone()

        total_images = (
            int(total_row.total_images)
            if total_row and total_row.total_images is not None
            else 0
        )

        rows = db.execute(
            queries.q_visual_content(),
            {"channel_name": channel_name},
        ).fetchall()

        category_counts = [
            schemas.CategoryCount(image_category=r.image_category, n=r.n) for r in rows
        ]

        return schemas.VisualContentResponse(
            channel_name=channel_name,
            total_images=total_images,
            category_counts=category_counts,
        )
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")
