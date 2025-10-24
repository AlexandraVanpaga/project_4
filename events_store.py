import logging
from fastapi import FastAPI
from pydantic import BaseModel

logger = logging.getLogger("uvicorn.error")

class EventStore:
    def __init__(self, max_events_per_user=10):
        self.events = {}
        self.max_events_per_user = max_events_per_user

    def put(self, user_id, item_id):
        user_events = self.events.get(user_id, [])
        self.events[user_id] = [item_id] + user_events[:self.max_events_per_user]

    def get(self, user_id, k):
        return self.events.get(user_id, [])[:k]

events_store = EventStore()
app = FastAPI(title="events")

class EventRequest(BaseModel):
    user_id: int
    item_id: int = None
    k: int = 10

@app.post("/put")
async def put(req: EventRequest):
    if req.item_id is None:
        return {"error": "item_id required"}
    events_store.put(req.user_id, req.item_id)
    return {"result": "ok"}

@app.post("/get")
async def get(req: EventRequest):
    events = events_store.get(req.user_id, req.k)
    return {"events": events}
