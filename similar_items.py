import logging
from contextlib import asynccontextmanager
import pandas as pd
from fastapi import FastAPI
from pydantic import BaseModel

logger = logging.getLogger("uvicorn.error")

# S3 credentials
storage_options = {
    'key': 'YCAJE3Nlz8iDILW5VTYM1ihQB',
    'secret': 'YCPjvS7uwhvJpUj3bKm8X-IX4QAwBIVsvX61IL44',
    'client_kwargs': {'endpoint_url': 'https://storage.yandexcloud.net'}
}
bucket_name = 's3-student-mle-20250729-0060996a6e-freetrack'

class SimilarRequest(BaseModel):
    item_id: int
    k: int = 10

class SimilarItems:
    def __init__(self):
        self._similar_items = None
    
    def load(self, path, **kwargs):
        logger.info(f"Loading similar items from S3")
        self._similar_items = pd.read_parquet(path, storage_options=storage_options, **kwargs)
        self._similar_items = self._similar_items.set_index("track_id_enc")
        logger.info(f"Loaded {len(self._similar_items)} items")
    
    def get(self, item_id: int, k: int = 10):
        try:
            i2i = self._similar_items.loc[item_id].head(k)
            i2i = i2i[["similar_track_id_enc", "score"]].to_dict(orient="list")
        except KeyError:
            logger.error(f"No similar items for {item_id}")
            i2i = {"similar_track_id_enc": [], "score": []}
        return i2i

sim_items_store = SimilarItems()

@asynccontextmanager
async def lifespan(app: FastAPI):
    sim_items_store.load(
        f"s3://{bucket_name}/recsys/recommendations/similar.parquet",
        columns=["track_id_enc", "similar_track_id_enc", "score"]
    )
    logger.info("Ready!")
    yield

app = FastAPI(title="similar_items", lifespan=lifespan)

@app.post("/similar_items")
async def similar_items(req: SimilarRequest):
    i2i = sim_items_store.get(req.item_id, req.k)
    return i2i