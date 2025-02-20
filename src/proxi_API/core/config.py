# config.py


# Setting for the API


class Settings:
    '''
    Class containing the basic information of the API.
    '''
    PROJECT_NAME: str = "Proximity API"
    DESCRIPTION: str = "API for proximity model."
    CONTACT: dict[str] = {"name": "M. Herrero", "e-mail": "mherrero@bsc.es"}
    PROJECT_VERSION: str = "0.1"


settings = Settings()
