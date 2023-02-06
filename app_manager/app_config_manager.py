import os
import yaml
from jproperties import Properties
from app_manager.utility.common.singleton import Singleton
from app_manager.utility.common.app_constants import ConfigConstants
from app_manager.utility.common.exceptions.CustomExceptions import ConfigNotLoadedException


class ConfigManager(metaclass=Singleton):
    def __init__(self):
        self.system_property = {}
        self.load_config()


    def load_config(self):
        configs = Properties()
        with open(os.path.join("conf/app.properties"), "rb") as read_prop:
            configs.load(read_prop)

        items_view = configs.items()
 
        for item in items_view:
            self.system_property[item[0]] = item[1].data

    def get_system_property(self):
        if len(self.system_property) == 0:
            raise ConfigNotLoadedException
        return self.system_property

    def get_key(self, key_name):
        if len(self.system_property) == 0:
            raise ConfigNotLoadedException
        return self.system_property.get(key_name)

    