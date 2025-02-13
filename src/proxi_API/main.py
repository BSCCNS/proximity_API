# main.py

from fastapi import FastAPI
from proxi_API.core.config import settings
from proxi_API.core.routers import api_router


# Mounts the API and include all routers onto it

app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.PROJECT_VERSION,
    description=settings.DESCRIPTION,
    contact=settings.CONTACT,
)
app.include_router(api_router)
