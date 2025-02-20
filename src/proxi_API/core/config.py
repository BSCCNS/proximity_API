# config.py


# Setting for the API


class Settings:
    '''
    Class containing the basic information of the API.
    '''
    PROJECT_NAME: str = "Proximity API"
    DESCRIPTION: str = "Computing accesibility time (in minutes) for pedestrians in the city."
    CONTACT: dict[str] = {"name": "M. Herrero", "e-mail": "mherrero@bsc.es"}
    PROJECT_VERSION: str = "0.1"
    SUMMARY: str = "Accessibility made easy"


settings = Settings()
