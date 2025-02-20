# endpoints.py

from fastapi import APIRouter, HTTPException
import asyncio
import logging
from proxi_API.model import setup_city
from proxi_API.data.settings import CITY
import datetime
import uuid
from proxi_API.schemas import schemas
from proxi_API.model.mobility_indices import metric_comp


router = APIRouter()

logger = logging.getLogger("uvicorn.error")
tasks = {}


def after_task_done(task, task_id):
    tasks[task_id].status = "Completed"


@router.get("/setup")
async def setup():
    """'
    GET call to /. Returns a hello message. User must be authenticated.

    """
    task_id = str(uuid.uuid4())  # Generate a unique ID for the task
    logger.info(f"Starting run with task ID: {task_id}")

    async def setup_task(task_id):
        try:

            logger.info("Running - Preparing datasets")
            await asyncio.to_thread(setup_city.main, CITY)

            logger.info(f"Run with task ID: {task_id} finished")
        except asyncio.CancelledError:
            logger.info(f"Run with task ID: {task_id} cancelled")
            raise  # Propagate the cancellation exception

    time = str(datetime.datetime.now())

    task = asyncio.create_task(setup_task(task_id))
    task_ob = schemas.ModelTask(
        task=task, start_time=time, type="City_setup", city=CITY
    )
    tasks[task_id] = task_ob
    task.add_done_callback(lambda t: after_task_done(t, task_id))
    return {"task_id": task_id}


# Endpoint to compute the ponderated average of metrics
@router.post("/proximity_time")
async def prox_time(input: schemas.InputSliders):

    result = metric_comp(input.sliders)

    return result


# Endpoint to check tasks running
@router.get("/list", summary="Query running and completed tasks.")
async def check_tasks():
    """
    Checks which tasks are being executed or finished in the backend.
    """

    return tasks


# Endpoint to check the status of a specific task
@router.get("/status/{task_id}", summary="Check the status of a given task.")
async def status(task_id: str):
    """
    Checks the current status of a task by ID.
    """
    try:
        task_ob = tasks.get(task_id)
    except:
        raise HTTPException(status_code=404, detail="Task not found")

    task = task_ob.task

    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    return {"status": task_ob.status}


# Endpoint to stop the task
@router.delete("/stop/{task_id}", summary="Stop a running task.")
async def stop_model(task_id: str):
    """
    Stops a running task, provided its ID.
    """

    try:
        task_ob = tasks.get(task_id)
    except:
        raise HTTPException(status_code=404, detail="Task not found")

    task = task_ob.task

    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    if task.done():
        raise HTTPException(status_code=400, detail="Task already completed")

    task.cancel()  # Cancel the running task

    try:
        await task  # Wait for the task to handle the cancellation
    except asyncio.CancelledError:
        logger.info(f"Task {task_id} successfully cancelled")
    finally:
        tasks.pop(task_id, None)  # Clean up the task from the dictionary

    return {"status": "Task cancelled", "task_id": task_id}
