from fastapi import FastAPI, BackgroundTasks
from main import run_script
import asyncio

app = FastAPI()

@app.get("/start")
async def start(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_script)
    return {"status": "Task started"}
