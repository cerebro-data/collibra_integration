import uuid

from typing import Union
from fastapi import APIRouter,Depends,BackgroundTasks
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from app_manager.controllers.model.dgc_okera_model import DgcOkeraModel
from app_manager.controllers.model.dgc_okera_model import DgcOkeraConstants
from app_manager.services.service_wrapper import ServiceWrapper
from app_manager.services.service_config import ServiceId
from app_manager.utility.common.http_validation import BasicAuth
from app_manager.utility.common.status_manager import Status



controller = APIRouter()
security = HTTPBasic()
status_obj = Status()

@controller.post("/v1.0/scan/okeratocollibra")
async def execute_okera_collibra():
    input_object = {"welcome": "test"}
    
    return input_object

@controller.post("/v1.0/scan/collibratookera")
async def execute_collibra_okera(community:str,domain:str,request_data:DgcOkeraModel,schema_name:Union[str, None] = "", system_name:Union[str, None] = ""):
   
    input_object = {}
    input_object['database_name'] = request_data.database_name
    input_object['table_list'] = request_data.table_list
    input_object['process_code'] = DgcOkeraConstants.process_code
    sync_id = str(uuid.uuid1())
    input_object['sync_id'] = sync_id
    input_object['community_name'] = community
    input_object['domain_name'] = domain
    input_object['schema_name'] = schema_name
    input_object['system_name'] = system_name


    service_instance = ServiceWrapper.get_instance(ServiceId.app_service, input_object)
    result_object = service_instance.execute()
    return result_object


@controller.get("/status")
async def status(job_id: str, credentials: HTTPBasicCredentials = Depends(security)):
    BasicAuth.validate_credentials(credentials)
    nf_data = status_obj.get_status(job_id)
    return nf_data





