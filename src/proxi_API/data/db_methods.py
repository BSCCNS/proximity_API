# db_methods.py

from proxi_API.core.config import settings
import json


def save_users_db():
    """
    Saves the user database to disk.
    """
    TEMP_DB = settings.USERS_DB.copy()
    for key in TEMP_DB.keys():
        TEMP_DB[key]["hashed_password"] = TEMP_DB[key]["hashed_password"].decode()

    with open("data/users_db_fake.json", "w") as outfile:
        json.dump(TEMP_DB, outfile)
