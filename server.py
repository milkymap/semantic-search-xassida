import asyncio 
import signal 

from uuid import uuid4

import zmq 
import zmq.asyncio as aiozmq 

import uvicorn

from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import JSONResponse

from log import logger

from enum import Enum 

from typing import Dict, List, Optional

class TaskStatus(str, Enum):
    PENDING :str = "PENDING"
    RUNNING :str = "RUNNING"
    COMPLETED :str = "COMPLETED"
    FAILED :str = "FAILED"

class APIServer:
    def __init__(self, host:str, port:int, broker_address:str):
        self.app = FastAPI()
        self.host = host
        self.port = port
        self.broadcast_address = broker_address

        self.app.add_event_handler("startup", self.startup)
        self.app.add_event_handler("shutdown", self.shutdown)

        self.app.add_api_route("/sort_images", self.sort_images, methods=["GET"])
        self.app.add_api_route("/monitor_task", self.monitor_task, methods=["GET"])
        self.app.add_api_route("/get_all_tasks", self.get_all_tasks, methods=["GET"])
    
    async def destroy_resources(self):
        tasks = asyncio.all_tasks()
        background_tasks = [task for task in tasks if task.get_name().startswith("background-worker") ]
        for task in background_tasks:
            task.cancel()
        
        await asyncio.gather(*background_tasks, return_exceptions=True)
        self.server.should_exit = True

    async def startup(self):
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(
            sig=signal.SIGINT,
            callback=lambda: asyncio.create_task(self.destroy_resources())
        )
        self.ctx = aiozmq.Context()
        self.shared_mutex = asyncio.Lock()
        self.map_task_id2status:Dict[str, TaskStatus] = {}
        logger.info("Starting up")

    async def shutdown(self):
        self.ctx.term()
        logger.info("Shutting down")
    
    async def get_all_tasks(self):
        async with self.shared_mutex:
            return JSONResponse(
                status_code=200,
                content={
                    "tasks": self.map_task_id2status
                }
            )

    async def monitor_task(self, task_id:str):
        logger.info(f"Monitoring task {task_id}")
        async with self.shared_mutex:
            status = self.map_task_id2status[task_id]
            return JSONResponse(
                status_code=200,
                content={
                    "task_id": task_id,
                    "status": status,
                }
            )
            
    async def sort_images(self, path2dir:str, background_tasks: BackgroundTasks):
        task_id = str(uuid4())
        logger.info(f"Sorting images from {path2dir}")
        async with self.shared_mutex:
            self.map_task_id2status[task_id] = TaskStatus.PENDING

        background_tasks.add_task(self.sort_images_handler, task_id, path2dir)
        return JSONResponse(
            status_code=200,
            content={"message": "Sorting images", "task_id": task_id}
        )

    async def sort_images_handler(self, task_id:str, path2dir:str):
        task = asyncio.current_task()
        task.set_name(f"background-worker-{task_id}")
        dealer_socket = self.ctx.socket(zmq.DEALER)
        dealer_socket.connect(self.broadcast_address)

        try:
            await asyncio.sleep(3)
            async with self.shared_mutex:
                self.map_task_id2status[task_id] = TaskStatus.RUNNING
            
            
            await dealer_socket.send_multipart([b'', path2dir.encode("utf-8")])
            logger.info(f"Sent message to broker {path2dir}")
            _, response = await dealer_socket.recv_multipart()
            logger.info(f"Received response from broker {response}")
            async with self.shared_mutex:
                self.map_task_id2status[task_id] = TaskStatus.COMPLETED
        except asyncio.CancelledError:
            logger.info(f"Task {task_id} cancelled")
            async with self.shared_mutex:
                self.map_task_id2status[task_id] = TaskStatus.FAILED

        finally:
            dealer_socket.close(linger=0)
        logger.info(f"Task {task_id} has finished")

    def start_service(self):
        self.config = uvicorn.Config(app=self.app, host=self.host, port=self.port)
        self.server = uvicorn.Server(config=self.config)
        self.server.run()

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type:
            logger.error(f"Exception: {exc_type} {exc_value}")
            logger.exception(traceback)