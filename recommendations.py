import logging
from contextlib import asynccontextmanager
import pandas as pd
from fastapi import FastAPI
import requests
from pydantic import BaseModel

logger = logging.getLogger("uvicorn.error")

# S3 credentials
storage_options = {
    'key': 'YCAJE3Nlz8iDILW5VTYM1ihQB',
    'secret': 'YCPjvS7uwhvJpUj3bKm8X-IX4QAwBIVsvX61IL44',
    'client_kwargs': {'endpoint_url': 'https://storage.yandexcloud.net'}
}
bucket_name = 's3-student-mle-20250729-0060996a6e-freetrack'

events_store_url = "http://localhost:8001"
features_store_url = "http://localhost:8002"

class UserRequest(BaseModel):
    user_id: int
    k: int = 100

class Recommendations:
    def __init__(self):
        self._recs = {"personal": None, "default": None}
        self._stats = {"request_personal_count": 0, "request_default_count": 0}

    def load(self, type, path, **kwargs):
        logger.info(f"Loading recommendations from S3, type: {type}")
        self._recs[type] = pd.read_parquet(path, storage_options=storage_options, **kwargs)
        if type == "personal":
            self._recs[type] = self._recs[type].set_index("user_id_enc")
        logger.info(f"Loaded {len(self._recs[type])} records")

    def get(self, user_id: int, k: int = 100):
        try:
            recs = self._recs["personal"].loc[user_id]["track_id_enc"].to_list()[:k]
            self._stats["request_personal_count"] += 1
        except KeyError:
            recs = self._recs["default"]["track_id_enc"].to_list()[:k]
            self._stats["request_default_count"] += 1
        except:
            logger.error("No recommendations found")
            recs = []
        return recs

    def stats(self):
        logger.info("Stats for recommendations")
        for name, value in self._stats.items():
            logger.info(f"{name:<30} {value}")

rec_store = Recommendations()

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting")
    rec_store.load("personal", f"s3://{bucket_name}/recsys/recommendations/personal_als.parquet")
    rec_store.load("default", f"s3://{bucket_name}/recsys/recommendations/top_popular.parquet")
    yield
    logger.info("Stopping")
    rec_store.stats()

app = FastAPI(title="recommendations", lifespan=lifespan)

def dedup_ids(ids):
    seen = set()
    return [id for id in ids if not (id in seen or seen.add(id))]

@app.post("/recommendations")
async def recommendations(req: UserRequest):
    recs = rec_store.get(req.user_id, req.k)
    return {"recs": recs}

@app.post("/recommendations_online")
async def recommendations_online(req: UserRequest):
    headers = {"Content-type": "application/json", "Accept": "text/plain"}

    # Получаем события пользователя
    resp = requests.post(events_store_url + "/get", json={"user_id": req.user_id, "k": 3}, headers=headers)
    events = resp.json().get("events", [])

    if not events:
        logger.info("No online events — fallback to personal/default")
        return {"recs": rec_store.get(req.user_id, req.k)}

    items = []
    scores = []
    for item_id in events:
        resp = requests.post(features_store_url + "/similar_items", json={"item_id": item_id, "k": req.k}, headers=headers)
        item_similar = resp.json()
        items += item_similar.get("similar_track_id_enc", [])
        scores += item_similar.get("score", [])

    combined = sorted(zip(items, scores), key=lambda x: x[1], reverse=True)
    recs = dedup_ids([item for item, _ in combined])[:req.k]
    return {"recs": recs}
