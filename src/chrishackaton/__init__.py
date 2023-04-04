
from fastapi import Depends, FastAPI

from .routers import auth, job_manager

app = FastAPI()

app.include_router(auth.router,  prefix="/auth",)
app.include_router(job_manager.router,  prefix="/jobs",)

@app.get("/")
async def root():
    return {"message": "Hello Bigger Applications!"}
