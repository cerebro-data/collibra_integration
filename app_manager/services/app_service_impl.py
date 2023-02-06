
import _thread

from app_manager.process.collibra_okera_intergration_process import DgcOkeraIntegrator
from app_manager.services.base_service import BaseService 
from app_manager.utility.logs.app_logger import Logger
from app_manager.controllers.model.dgc_okera_model import DgcOkeraConstants

integration_module = dict()
integration_module[DgcOkeraConstants.process_code] = DgcOkeraIntegrator

logger = Logger().get_logger()

class AppServiceImpl(BaseService):

    def __init__(self, input_object):
        super().__init__(input_object)

    def prepare(self):
        logger.debug("begin")
        self.sync_id = self.input_object['sync_id']        
        logger.debug("end")
    
    def process(self):
        logger.debug("begin")
        logger.debug(self.input_object)
        _thread.start_new_thread(integration_module[self.input_object["process_code"]]().execute,(self.input_object,))
        logger.debug("end")
    
    def persist(self):
        logger.debug("begin")
        self.result_object["status_id"] = self.sync_id
        self.result_object["message"] = "Integration started successfully"
        logger.debug("end")
