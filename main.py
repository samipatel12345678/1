from fastapi import FastAPI
from routes import users,orders, processing, metrics
app = FastAPI()


app.include_router(users.router, prefix="/api", tags=["Users"])
app.include_router(orders.router, prefix="/api", tags=["Orders"] )
app.include_router(processing.router, prefix="/api", tags=["Process"] )
app.include_router(metrics.router, prefix="/api", tags=["Metrics"] )
@app.get("/")
def read_root():
    return {"message": "Welcome to FastAPI!"}

