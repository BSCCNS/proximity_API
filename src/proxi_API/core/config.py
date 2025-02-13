# config.py

from proxi_API.data.fake_db import users_db

# Setting for the API


class Settings:
    PROJECT_NAME: str = "Proximity API"
    DESCRIPTION: str = (
        "API for proximity model."
    )
    CONTACT: dict[str] = {"name": "M. Herrero", "e-mail": "mherrero@bsc.es"}
    PROJECT_VERSION: str = "0.1"
    

settings = Settings()
