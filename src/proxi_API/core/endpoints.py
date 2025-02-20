# endpoints.py
from typing import Annotated
from enum import Enum
from fastapi import APIRouter, HTTPException
import asyncio
import logging
from proxi_API.model import setup_city
import datetime
import uuid
from proxi_API.schemas import schemas
from proxi_API.model.mobility_indices import metric_comp
from proxi_API.data.settings import H3_ZOOM
from pathlib import Path

router = APIRouter()  # Loading the endpoints in a router.

logger = logging.getLogger("uvicorn.error")  # Logger for logging info
tasks = {}  # Dictionary to store the queue of tasks running/completed


# Metadata for the tags in the documentation
tags_metadata = [
    {
        "name": "Proximity time",
        "description": "Operations to compute the proximity time and metrics for the city.",
    },
    {
        "name": "Task management",
        "description": "Operations to manage the tasks being executed in the backend.",
    },
]


def after_task_done(task, task_id):
    """
    After a task is finished, it changes its status in the queue to completed.

    ### Parameters:
    - task (asyncio.task): Task being executed.
    - task_id (str): ID of the task, acting as the key of a dictionary.
    """

    tasks[task_id].status = "Completed"


#####################
###Proximity time###
####################
# 

# We set city defaults
class AvailableCities(str, Enum):
    Barcelona = "Barcelona"
    Madrid = 'Madrid'
    Oviedo = "Oviedo"
    Viladecans = 'Viladecans'
    

# Endpoint to setup the model for a given city (currently, the city is being read from data/settings.py)
@router.get(
    "/setup/{city}", summary="Setup the app for a given city.", tags=["Proximity time"]
)
async def setup(city: AvailableCities):
    """
    Perform asynchronous setup for processing a city model.

    This function orchestrates the setup of a city's model by:
    - Generating a unique task identifier.
    - Creating necessary folder structures inside (`data/cities`)
    - Spawning an asynchronous task that sequentially runs:
        - Serves a map in geojson format containing the proximity time for the given city.
        - Computes proximity time for the different types of pedestrians (residents, tourists, workers/students, leisure, access to hospitality, access to public transport)
    - Logging the start, progress, and completion (or cancellation) of the task.
    - Returning a dictionary containing the unique task identifier.

    ### Parameters:
    - city (choice): City to setup the model for. Select from the list.
    
    ### Returns:
    - `dict`: A dictionary with the key `'task_id'` mapped to the unique identifier of the asynchronous task.

    ### Raises:
    - `Exception`: Propagates any exceptions, including task cancellations, encountered during the asynchronous
      operations for network or POI preparations.

    """
    task_id = str(uuid.uuid4())  # Generate a unique ID for the task
    logger.info(f"Starting run with task ID: {task_id}")

    async def setup_task(task_id):
        try:

            logger.info("Running - Preparing datasets")
            await asyncio.to_thread(setup_city.main, city.value)

            logger.info(f"Run with task ID: {task_id} finished")
        except asyncio.CancelledError:
            logger.info(f"Run with task ID: {task_id} cancelled")
            raise  # Propagate the cancellation exception

    time = str(datetime.datetime.now())

    task = asyncio.create_task(setup_task(task_id))
    task_ob = schemas.ModelTask(
        task=task, start_time=time, type="City_setup", city=city.value
    )
    tasks[task_id] = task_ob
    task.add_done_callback(lambda t: after_task_done(t, task_id))
    return {"task_id": task_id}


# Endpoint to compute the ponderated average of metrics
@router.post(
    "/proximity_time/{city}",
    summary="Computes the proximity time and metrics.",
    tags=["Proximity time"],
)
async def prox_time(city: AvailableCities,input: schemas.InputSliders):
    """
    Computes global metrics associated to te accesibility of the city.

    This function takes a numerical value for the weights given to any of the different pedestrian groups
    - Residents
    - Tourists
    - Workers/Students
    - Leisure
    - Access to hospitality
    - Access to public transport

    and returns the proximity time value (in minutes) for each category, averaged to the whole city.
    It also provides a weighted averaged considering the user input and produces global inequality metrics based on this
    - Gini index (in %)
    - Theil index
    - Theil index converted to % through 100*(1-exp(-t))

    ### Parameters:
    - city (choice): City to compute the metrics for. Select from the list.
    - `sliders` (array): Six dimensional array containing the numerical weights for each category of pedestrians

    ### Returns:
    - `dict`: A dictionary containing the metrics and indices.
    """
    out = Path(__file__).parents[1] / "data" / "cities"

    if not Path(out / f"{city.value}_{H3_ZOOM}_agg.geojson").is_file():
        raise HTTPException(status_code=404, detail=f"{city.value} is not available. Run Setup first. ")

    result = metric_comp(city.value, input.sliders)

    return result


#####################
###Task management###
####################
# Endpoint to check tasks running
@router.get(
    "/list",
    summary="Query running and completed tasks.",
    tags=["Task management"],
)
async def check_tasks():
    """
    Checks which tasks are being executed or finished in the backend.
    """

    return tasks


# Endpoint to check the status of a specific task
@router.get(
    "/status/{task_id}",
    summary="Check the status of a given task.",
    tags=["Task management"],
)
async def status(task_id: str):
    """
    Checks the current status of a task by ID.

    ### Parameters:
        - Task_id (str): ID of the task to check.
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
@router.delete(
    "/stop/{task_id}",
    summary="Stop a running task.",
    tags=["Task management"],
)
async def stop_model(task_id: str):
    """
    Stops a running task, provided its ID.

    ### Parameters:
        - Task_id (str): ID of the task to stop.
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
