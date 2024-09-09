from fastapi import FastAPI
from main import run_script
import asyncio

app = FastAPI()

@app.get("/start")
async def start():
    await run_script()
