# schemas.py

from pydantic import BaseModel, Field, ConfigDict, conlist, field_validator
from typing import Optional, List
import asyncio
import numpy as np


class ModelTask(BaseModel):
    city: str
    task: Optional[asyncio.Task] = Field(default=None, exclude=True)
    start_time: str
    type: str
    status: str = "Running"

    model_config = ConfigDict(arbitrary_types_allowed=True)


class InputSliders(BaseModel):
    sliders: conlist(float, min_length=6, max_length=6) = [1, 1, 1, 1, 1, 1]
    # En este orden:
    # Residentes, turistas, trabajadores/estudiantes
    # Compras/ocio, acceso hostelería, acceso transporte público
