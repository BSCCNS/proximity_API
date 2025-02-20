# routers.py

from fastapi import APIRouter
from proxi_API.core import endpoints

# We define a router that collects everything together
api_router = APIRouter()
api_router.include_router(endpoints.router, prefix="")  # Basic endpoint
