import os
import logging



from app_manager.utility.common.singleton import Singleton
from app_manager.app_config_manager import ConfigManager
from app_manager.utility.common.app_constants import ConfigConstants
from datetime import datetime


class Logger(metaclass=Singleton):

    def __init__(self):

        config_manger = ConfigManager()
        system_property = config_manger.get_system_property()
        self.logger = logging.getLogger("app_log")
        dir_name = "Log"

        if not os.path.isdir(dir_name):
            os.mkdir(dir_name)

        self.log_path = os.path.join(dir_name, system_property.get(ConfigConstants.log_path) 
                         + '{:%Y-%m-%d}'.format(datetime.now()) + ".logs")

        
        self.logger.setLevel(int(system_property.get(ConfigConstants.log_level)))
        f_handler = logging.handlers.RotatingFileHandler(self.log_path,
                                                         maxBytes=int(system_property.get(ConfigConstants.
                                                                      max_bytes)),
                                                         backupCount=int(system_property.get(ConfigConstants.
                                                                         backup_count)))
        f_handler.setLevel(int(system_property.get(ConfigConstants.log_level)))

        if int(system_property.get(ConfigConstants.log_level)) == 10:
            f_format = logging.Formatter('%(asctime)s  %(levelname)s %(thread)s  %(module)s:%(funcName)s %(lineno)s : %(message)s')
        else:
            f_format = logging.Formatter('%(asctime)s %(levelname)s: %(thread)s > %(message)s')

        f_handler.setFormatter(f_format)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(f_format)
        self.logger.addHandler(f_handler)
        self.logger.addHandler(console_handler)

    def get_log_path(self):
        return self.log_path

    def get_logger(self):
        return self.logger
