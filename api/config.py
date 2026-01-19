from pydantic import BaseModel
import os


class Settings(BaseModel):
    # Example:
    # postgresql+psycopg2://user:password@localhost:5432/warehouse
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL", "postgresql+psycopg2://postgres:postgres@localhost:5432/postgres")

    # schema where dbt marts live
    MART_SCHEMA: str = os.getenv("MART_SCHEMA", "analytics")


settings = Settings()
