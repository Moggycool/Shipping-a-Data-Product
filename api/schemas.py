from pydantic import BaseModel, Field
from typing import List, Optional


class HealthResponse(BaseModel):
    ok: bool
    database: str


class TopProductItem(BaseModel):
    term: str
    mentions: int


class TopProductsResponse(BaseModel):
    limit: int
    items: List[TopProductItem]


class ActivityPoint(BaseModel):
    period_start: str
    message_count: int
    avg_views: Optional[float] = None


class ChannelActivityResponse(BaseModel):
    channel_name: str
    grain: str = Field(description="day|week|month")
    days: int
    points: List[ActivityPoint]


class MessageSearchItem(BaseModel):
    message_id: int
    channel_name: str
    message_ts: str
    message_text: Optional[str] = None
    message_length: Optional[int] = None
    views: Optional[int] = None
    forwards: Optional[int] = None
    reply_count: Optional[int] = None
    has_image: Optional[bool] = None


class MessageSearchResponse(BaseModel):
    q: str
    channel_name: Optional[str]
    limit: int
    offset: int
    items: List[MessageSearchItem]


class CategoryCount(BaseModel):
    image_category: str
    n: int


class VisualContentResponse(BaseModel):
    channel_name: Optional[str]
    total_images: int
    category_counts: List[CategoryCount]
