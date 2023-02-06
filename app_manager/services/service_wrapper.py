import importlib
from app_manager.services.service_config import INSTANCE_HELPER
from app_manager.services.service_config import HelperConstants


class ServiceWrapper:
    @classmethod
    def get_module(cls, instance_id):
        module = importlib.import_module(INSTANCE_HELPER[instance_id][HelperConstants.module_name])
        return module
    
    @classmethod
    def get_class_name(cls, instance_id):
        return INSTANCE_HELPER[instance_id]["class_name"]

    @classmethod
    def get_instance(cls, instance_id, input_object):
        instance_method = getattr(cls.get_module(instance_id), cls.get_class_name(instance_id))
        return instance_method(input_object)
