import os
import json
import time
import datetime
import threading
from msilib.schema import Error

from app_manager.utility.common.singleton import Singleton
from app_manager.app_config_manager import ConfigManager
from app_manager.utility.common.app_constants import ConfigConstants

class Status(metaclass=Singleton):
    def __init__(self):
        self.system_property = ConfigManager().get_system_property()
        self.execution_status = {}
        self.notification_status = {}

        if not os.path.exists(self.system_property.get(ConfigConstants.app_status_folder)):
            os.mkdir(self.system_property.get(ConfigConstants.app_status_folder))

    def status_delete(self):

        now = time.time()
        for filename in os.listdir(self.system_property.get(ConfigConstants.app_status_folder)):
            file_stamp = os.stat(os.path.join(self.system_property.get(ConfigConstants.app_status_folder), filename)).st_mtime
            file_compare = now - float(self.system_property.get(ConfigConstants.app_status_maintanence_days)) * 86400
            if file_stamp < file_compare:
                os.remove(os.path.join(self.system_property.get(ConfigConstants.app_status_folder), filename))

    def get_status(self, uuid):
        if uuid in self.notification_status:
            return self.notification_status[uuid]
        else:
            uuid_path = os.path.join(self.system_property.get(ConfigConstants.app_status_folder),uuid+".json")
            if os.path.exists(uuid_path):
                uuid_file_obj = open(uuid_path,)
                nf_data = json.load(uuid_file_obj)
                uuid_file_obj.close()
                return nf_data
            else:
                return "Status not Available"

    def store_status(self, uuid):        
        uuid_path = os.path.join(self.system_property.get(ConfigConstants.app_status_folder),uuid+".json")
        uuid_file_obj = open(uuid_path,"w")
        json.dump(self.notification_status[uuid], uuid_file_obj, indent = 4)
        uuid_file_obj.close()
        del self.notification_status[uuid]

    def initialize_status(self, uuid):
        self.status_delete()
        self.execution_status[uuid] = {}

        self.notification_status[uuid] = {
            "Message": "Execution In Progress",
            "Status" : "IN_PROGRESS",            
            "StartTime": str(datetime.datetime.now()),
            "EndTime": "",
            "SUCCESS" : [],
            "ERROR" : [],
            "WARN" : []

        }
    
    def get_notification_status(self, uuid):
        return json.dumps(self.notification_status[uuid])

    def update_warn_message(self, uuid, error_message):
        time_now = str(datetime.datetime.now())
        warn_message_dict = {
            "Date" : time_now,
            "JobID" : uuid,
            "Status": "WARN",
            "Message": error_message
        }
        if "WARN" in self.notification_status[uuid]:
            self.notification_status[uuid]["WARN"] = [warn_message_dict] + self.notification_status[uuid]["WARN"]
        else:
            self.notification_status[uuid]["WARN"] = [warn_message_dict]


    def update_error_message(self, uuid, error_message):
        time_now = str(datetime.datetime.now())
        error_message_dict = {
            "Date" : time_now,
            "JobID" : uuid,
            "Status": "ERROR",
            "Message": error_message
        }
        if "ERROR" in self.notification_status[uuid]:
            self.notification_status[uuid]["ERROR"] = [error_message_dict] + self.notification_status[uuid]["ERROR"]
        else:
            self.notification_status[uuid]["ERROR"] = [error_message_dict]

    def complete_status(self, uuid, status):
        time_now = str(datetime.datetime.now())
        self.notification_status[uuid]["Message"] = "Integration Process Completed"
        self.notification_status[uuid]["Status"] = status
        self.notification_status[uuid]["EndTime"] = time_now
        message_dict = {
            "Date" : time_now,
            "JobID" : uuid        
        }

        if status == "COMPLETED":
            message_dict["Message"] = "Dataset Summary : " + json.dumps(self.execution_status[uuid])
            message_dict["Status"] = "SUCCESS"
            self.notification_status[uuid]["SUCCESS"] = [message_dict]
            
        elif status == "ERROR":            
            message_dict["Message"] = "Dataset Summary : " +json.dumps(self.execution_status[uuid]) +  " Completed with Error" + ":" + "Please Refer logs"
            message_dict["Status"] = status
            if "ERROR" not in self.notification_status[uuid]:
                self.notification_status[uuid]["ERROR"] = [message_dict]
            else:
                self.notification_status[uuid]["ERROR"] = [message_dict] + self.notification_status[uuid]["ERROR"]
        
    def add_dataset_status(self, uuid, update_obj):
        self.execution_status[uuid][ConfigConstants.asset_type_table] = int(update_obj[ConfigConstants.asset_type_table])

    def add_dataset_column_status(self, uuid, update_obj):
        self.execution_status[uuid][ConfigConstants.asset_type_column] = int(update_obj[ConfigConstants.asset_type_column])
    
    def update_execution_status(self, uuid, update_obj):
        
        self.add_dataset_status(uuid, update_obj)
        self.add_dataset_column_status(uuid, update_obj)
        
       

    

    
    

        
        

    