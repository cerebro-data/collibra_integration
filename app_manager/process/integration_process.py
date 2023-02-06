from contextlib import contextmanager
import json
import datetime
import time
import pandas as pd
import re
import collections
import sys
from abc import ABC,abstractmethod
from sqlalchemy import update

from app_manager.connector.dgc.form_tvc import TableViewConfig
from app_manager.utility.common.status_manager import Status
from app_manager.utility.logs.app_logger import Logger
from app_manager.app_config_manager import ConfigManager
from app_manager.connector.dgc.dgc_connector import DGCConnector
from app_manager.connector.okera.okera_connector import OkeraConnector
from app_manager.utility.common.exceptions.CustomExceptions import OkeraCommonError
from app_manager.utility.common.app_constants import ConfigConstants


Tag = collections.namedtuple('Tag', ['tag', 'namespace', 'key'])


logger = Logger().get_logger()
class CatalogIntegration(ABC):
    def __init__(self):
        self.input_object = None
        self.system_property = ConfigManager().get_system_property()
        self.dgc_connector = DGCConnector()
        self.okera_connector = OkeraConnector()
        self.tvc_obj = TableViewConfig()
        self.status_obj = Status()
        self.dgc_delimiter = self.system_property.get(ConfigConstants.dgc_asset_load)
        self.okera_delimiter = self.system_property.get(ConfigConstants.okera_asset_load)
        self.opr_level = None
        self.tbl_list = []
        self.dgc_loaded_from_edge = self.system_property.get(ConfigConstants.dgc_loaded_from_edge)
        self.dgc_tag_as_relation = self.system_property.get(ConfigConstants.dgc_security_classification_as_relation).lower()

    
    def format_okera_tags(self,attribute_values,mapped_namespaces):
       
        attributes = set()
        try:
            if attribute_values:
                for attribute in attribute_values:
                    
                    if attribute.attribute.attribute_namespace in mapped_namespaces:
                        tag_name = '.'.join(
                            [attribute.attribute.attribute_namespace, attribute.attribute.key])
                        tag = Tag(tag_name, attribute.attribute.attribute_namespace,
                                        attribute.attribute.key)
                        
                        attributes.add(attribute.attribute.attribute_namespace + '.' + attribute.attribute.key) 

                return attributes
        except Exception as e:
            raise OkeraCommonError(e)

        return None

    
    def get_mapped_attributes(self):
        okera_namespaces = set()

        okera_namespaces.add(self.system_property.get(ConfigConstants.okera_namespace).replace("-", "_").replace(" ", "_"))
        okera_namespaces.add(self.system_property.get(ConfigConstants.okera_namespace_status))

        return okera_namespaces

    
    @abstractmethod
    def process(self):
        pass
    
    
    def execute(self, input_object):
        logger.debug('Begin')
        self.input_object = input_object  

        self.process()

        notification_status = self.status_obj.get_status(self.input_object["sync_id"])
        logger.info(notification_status)
        self.status_obj.store_status(self.input_object["sync_id"])
        
        logger.debug('end')