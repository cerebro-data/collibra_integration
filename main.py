import uvicorn
from app_manager.app import app
from app_manager.app_config_manager import ConfigManager
from app_manager.utility.common.app_constants import ConfigConstants
from app_manager.utility.logs.app_logger import Logger



config_manager = None


def load_app():
    configure_manager = ConfigManager()
    configure_manager.load_config()
    
   
def start_app():
    load_app()

    app_config = ConfigManager().get_system_property()
    Logger().logger.debug("started")
    uvicorn.run(app,
                host=app_config.get(ConfigConstants.app_host),
                port=int(app_config.get(ConfigConstants.app_port)))



if __name__ == '__main__':
    start_app()
