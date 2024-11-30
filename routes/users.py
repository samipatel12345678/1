from fastapi import APIRouter, HTTPException,Form
from config.supabase import supabase
from pydantic import BaseModel

router = APIRouter()

class UserCreateRequest(BaseModel):
    name: str
    email: str

# POST /users - Create a new user
@router.post("/users")
def create_user(user: UserCreateRequest):
    response = supabase.table("users").insert({"name": user.name, "email": user.email}).execute()
    
    return {"message": "User created successfully", "user": response}

# GET /users/{id} - Fetch user details by ID
@router.get("/users/{id}")
def get_user_by_id(id: str):
    # Fetch user data from Supabase
    response = supabase.table("users").select("*").eq("id", id).execute()
    # Check if data exists
    if not response.data or len(response.data) == 0:
        raise HTTPException(status_code=400, detail="Users not found")
    
    # Return user data if found
    return {"user": response.data}


# PUT /users/{id} - Update user information
@router.put("/users/{id}")
def update_user(user:UserCreateRequest, id: str):
    userheck = supabase.table("users").select("*").eq("id", id).execute()
    # Check if data exists
    if not userheck.data or len(userheck.data) == 0:
        raise HTTPException(status_code=400, detail="Users not found")
    
    updates = {key: value for key, value in {"name": user.name, "email": user.email}.items() if value is not None}
    response = supabase.table("users").update(updates).eq("id", id).execute()
   
    return {"message": "User updated successfully", "user": response}

# DELETE /users/{id} - Delete a user by ID
@router.delete("/users/{id}")
def delete_user(id: str):
    userheck = supabase.table("users").select("*").eq("id", id).execute()
    # Check if data exists
    if not userheck.data or len(userheck.data) == 0:
        raise HTTPException(status_code=400, detail="Users not found")
    response = supabase.table("users").delete().eq("id", id).execute()
    
    return {"message": "User deleted successfully", "response": response }
