import base64
import requests
import json
from io import BytesIO, BufferedReader
from requests.auth import HTTPBasicAuth
from requests.exceptions import ConnectionError

from app_manager.app_config_manager import ConfigManager
from app_manager.utility.common.app_constants import ConfigConstants
from app_manager.utility.common.exceptions.CustomExceptions import DGCError
from app_manager.utility.logs.app_logger import Logger
from app_manager.utility.common.exceptions.CustomExceptions import DGCUnspecifiedError
from app_manager.utility.common.exceptions.CustomExceptions import DGCConnectionError



logger = Logger().get_logger()

class DGCConnectorConstants:
    content_type_key = 'Content-type'
    content_type_value = 'application/json;charset=ISO-8859-1'

    connection_error = "Not able to connect with DGC System"
    
class DGCConnector:
    def __init__(self):
        self.system_property = ConfigManager().get_system_property()
        self.ssl_reqd = False
        self.session = requests.Session()
        if self.system_property.get(ConfigConstants.dgc_ssl_verify_reqd) == "true":
            self.ssl_reqd = True

    def fetch_attribute_id(self,attribute_name):
        try:
            url = self.system_property.get(ConfigConstants.dgc_baseurl) + self.system_property.get(ConfigConstants.dgc_attrubute_url)
            url = url.replace('$attribute_name$', attribute_name)
            
            proxies = {}
            if len(self.system_property.get(ConfigConstants.dgc_proxy_url)) != 0:
                proxy = self.system_property.get(ConfigConstants.dgc_proxy_url)
                proxies = {self.system_property.get(ConfigConstants.dgc_proxy_protocol): proxy}
            
                
            user = self.system_property.get(ConfigConstants.dgc_username)
            password = base64.b64decode(self.system_property.get(ConfigConstants.dgc_password).encode("ascii")).decode("ascii")
            payload = {}
            headers = {
                
                DGCConnectorConstants.content_type_key: DGCConnectorConstants.content_type_value
            }
            response = self.session.request("GET", url, headers=headers, auth=HTTPBasicAuth(user, password),
                                        data=payload, proxies=proxies, verify=self.ssl_reqd)
            if response.status_code == 200:
                json_object = json.loads(response.content.decode('ISO-8859-1').encode("utf8"))
                return json_object['results'][0]['id']
            else:
                raise DGCError(url,response.status_code,response.text)
        except Exception as e:
            if isinstance(e, DGCError):
                raise e
            elif isinstance(e, ConnectionError):
                raise DGCConnectionError(DGCConnectorConstants.connection_error)
            else:
                raise DGCUnspecifiedError()

    def fetch_relation_id(self,source_name, role, corole,target):
        try:
            url = self.system_property.get(ConfigConstants.dgc_baseurl) + self.system_property.get(ConfigConstants.dgc_relation_url)
            url = url.replace('$source_name$', source_name)
            url = url.replace('$role$', role)
            url = url.replace('$corole$', corole)
            proxies = {}
            if len(self.system_property.get(ConfigConstants.dgc_proxy_url)) != 0:
                proxy = self.system_property.get(ConfigConstants.dgc_proxy_url)
                proxies = {self.system_property.get(ConfigConstants.dgc_proxy_protocol): proxy}
            user = self.system_property.get(ConfigConstants.dgc_username)
            password = base64.b64decode(self.system_property.get(ConfigConstants.dgc_password).encode("ascii")).decode("ascii")
            payload = {}
            headers = {
                DGCConnectorConstants.content_type_key: DGCConnectorConstants.content_type_value
            }
            response = self.session.request("GET", url, headers=headers, auth=HTTPBasicAuth(user, password),
                                        data=payload, proxies=proxies, verify=self.ssl_reqd)
            
            if response.status_code == 200:
                json_object = json.loads(response.content.decode('ISO-8859-1').encode("utf8"))
                return json_object['results'][0]['id']

            else:
                raise DGCError(url,response.status_code,response.text)
        except Exception as e:
            if isinstance(e, DGCError):
                raise e
            elif isinstance(e, ConnectionError):
                raise DGCConnectionError(DGCConnectorConstants.connection_error)
            else:
                raise DGCUnspecifiedError()

    def fetch_asset_json(self,tvc_dict):
        try:
            url = self.system_property.get(ConfigConstants.dgc_baseurl) + self.system_property.get(ConfigConstants.dgc_output_url)
            proxies = {}
            if len(self.system_property.get(ConfigConstants.dgc_proxy_url)) != 0:
                proxy = self.system_property.get(ConfigConstants.dgc_proxy_url)
                proxies = {self.system_property.get(ConfigConstants.dgc_proxy_protocol): proxy}
            user = self.system_property.get(ConfigConstants.dgc_username)
            password = base64.b64decode(self.system_property.get(ConfigConstants.dgc_password).encode("ascii")).decode("ascii")
            payload = json.dumps(tvc_dict)
            headers = {
                DGCConnectorConstants.content_type_key: DGCConnectorConstants.content_type_value
            }
            response = self.session.request("POST", url, headers=headers, auth=HTTPBasicAuth(user, password),
                                        data=payload, proxies=proxies, verify=self.ssl_reqd)
            if response.status_code == 200:
                print('response',response.content)
                json_object = json.loads(response.content.decode('ISO-8859-1').encode("utf8"))
                return json_object['aaData']
            else:
                raise DGCError(url,response.status_code,response.text)
        except Exception as e:
            if isinstance(e, DGCError):
                raise e
            elif isinstance(e, ConnectionError):
                raise DGCConnectionError(DGCConnectorConstants.connection_error)
            else:
                raise DGCUnspecifiedError()


    def import_asset_json(self,dgc_json_list):
        try:
            url = self.system_property.get(ConfigConstants.dgc_baseurl) + self.system_property.get(ConfigConstants.dgc_import_url)
            proxies = {}
            if len(self.system_property.get(ConfigConstants.dgc_proxy_url)) != 0:
                proxy = self.system_property.get(ConfigConstants.dgc_proxy_url)
                proxies = {self.system_property.get(ConfigConstants.dgc_proxy_protocol): proxy}
            user = self.system_property.get(ConfigConstants.dgc_username)
            password = base64.b64decode(self.system_property.get(ConfigConstants.dgc_password).encode("ascii")).decode("ascii")
            payload = {}
            headers = {}
            byts = BytesIO()
            byts.write(json.dumps(dgc_json_list).encode())
            byts.seek(0)
            _file = BufferedReader(byts)

            files = [
                ('file',
                ('file.json', _file, 'application/json'))]
            response = self.session.request("POST", url, headers=headers, auth=HTTPBasicAuth(user, password),
                                        data=payload, files=files, proxies=proxies, verify=self.ssl_reqd)
            if response.status_code == 200:
                json_object = json.loads(response.content.decode('ISO-8859-1').encode("utf8"))
                return json_object['id']
            else:
                raise DGCError(url,response.status_code,response.text)
        except Exception as e:
            if isinstance(e, DGCError):
                raise e
            elif isinstance(e, ConnectionError):
                raise DGCConnectionError(DGCConnectorConstants.connection_error)
            else:
                raise DGCUnspecifiedError()

	
    def fetch_job_status(self,status_id):
        try:
            url = self.system_property.get(ConfigConstants.dgc_baseurl) + self.system_property.get(ConfigConstants.dgc_job_status_url)
            url = url.replace('$status_id$', status_id)
            proxies = {}
            if len(self.system_property.get(ConfigConstants.dgc_proxy_url)) != 0:
                proxy = self.system_property.get(ConfigConstants.dgc_proxy_url)
                proxies = {self.system_property.get(ConfigConstants.dgc_proxy_protocol): proxy}
            user = self.system_property.get(ConfigConstants.dgc_username)
            password = base64.b64decode(self.system_property.get(ConfigConstants.dgc_password).encode("ascii")).decode("ascii")
            payload = {}
            headers = {
                DGCConnectorConstants.content_type_key: DGCConnectorConstants.content_type_value
            }
            
            response = self.session.request("GET", url, headers=headers, auth=HTTPBasicAuth(user, password),
                                        data=payload, proxies=proxies, verify=self.ssl_reqd)
            if response.status_code == 200:            
                json_object = json.loads(response.content.decode('ISO-8859-1').encode("utf8"))
                message = ""
                if "message" in json_object:
                    message = json_object['message']
                return (json_object['state'],message)
            else:
                raise DGCError(url,response.status_code,response.text)
        except Exception as e:
            if isinstance(e, DGCError):
                raise e
            elif isinstance(e, ConnectionError):
                raise DGCConnectionError(DGCConnectorConstants.connection_error)
            else:
                raise DGCUnspecifiedError()
    