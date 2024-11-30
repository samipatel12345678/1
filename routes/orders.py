from fastapi import APIRouter, HTTPException
from config.supabase import supabase
from pydantic import BaseModel

router = APIRouter()


class OrderCreateRequest(BaseModel):
    user_id: str
    product_name: str
    quantity: int
@router.post("/orders")
def create_order(order: OrderCreateRequest):
    response = supabase.table("orders").insert({
        "user_id": order.user_id,
        "product_name": order.product_name,
        "quantity": order.quantity
    }).execute()

    return {"message": "Order created successfully", "order": response}

@router.get("/orders/{id}")
def get_order_by_id(id: str):
    
    orderheck = supabase.table("orders").select("*").eq("id", id).execute()
    if not orderheck.data or len(orderheck.data) == 0:
        raise HTTPException(status_code=400, detail="Orders not found")

    response = supabase.table("orders").select("*").eq("id", id).execute()

    return {"order": response}
@router.put("/orders/{id}")
def update_order(id: str, order: OrderCreateRequest):
    orderheck = supabase.table("orders").select("*").eq("id", id).execute()
    if not orderheck.data or len(orderheck.data) == 0:
        raise HTTPException(status_code=400, detail="Orders not found")
    updates = {key: value for key, value in {"user_id": order.user_id, "product_name": order.product_name, "quantity": order.quantity}.items() if value is not None}
    response = supabase.table("orders").update(updates).eq("id", id).execute()

   
    return {"message": "Order updated successfully", "order": response}

@router.delete("/orders/{id}")
def delete_order(id: str):
    orderheck = supabase.table("orders").select("*").eq("id", id).execute()
    if not orderheck.data or len(orderheck.data) == 0:
        raise HTTPException(status_code=400, detail="Orders not found")
    response = supabase.table("orders").delete().eq("id", id).execute()

    if not response["data"]:
        raise HTTPException(status_code=404, detail="Order not found")

    return {"message": "Order deleted successfully", "response": response}
