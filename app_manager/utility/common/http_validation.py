import secrets
import base64


from app_manager.app_config_manager import ConfigManager
from app_manager.utility.common.app_constants import ConfigConstants
from app_manager.utility.common.exceptions.CustomExceptions import UnauthorisedError
from app_manager.utility.logs.app_logger import Logger

logger = Logger().get_logger()

class BasicAuth:

    @classmethod
    def validate_credentials(cls, credentials):
        system_property = ConfigManager().get_system_property()
        correct_username = secrets.compare_digest(credentials.username, system_property.get(ConfigConstants.app_username))
        correct_password = secrets.compare_digest(credentials.password, base64.b64decode(
            system_property.get(ConfigConstants.app_password).encode("ascii")).decode("ascii"))
        if not (correct_username and correct_password):
            logger.error("UnauthorisedException: Incorrect username or password")
            raise UnauthorisedError("Incorrect username or password")
        return credentials.username