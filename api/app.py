from fastapi import FastAPI
from pydantic import BaseModel


app = FastAPI(title="Ecommerce Training API")


class OrderSummary(BaseModel):
    batch_id: str
    order_count: int
    total_amount: float


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/notifications/orders")
def notify_orders(summary: OrderSummary):
    return {
        "status": "accepted",
        "message": f"Batch {summary.batch_id} received",
        "order_count": summary.order_count,
        "total_amount": summary.total_amount,
    }

