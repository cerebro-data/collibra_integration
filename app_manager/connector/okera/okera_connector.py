from okera import context
from okera._thrift_api import TGetDatasetsParams


from app_manager.utility.logs.app_logger import Logger
from app_manager.app_config_manager import ConfigManager
from app_manager.utility.common.app_constants import ConfigConstants
from app_manager.utility.common.exceptions.CustomExceptions import OkeraConnectionError
from app_manager.utility.common.exceptions.CustomExceptions import OkeraUnspecifiedError
from app_manager.utility.common.exceptions.CustomExceptions import OkeraFetchError
from app_manager.utility.common.exceptions.CustomExceptions import OkeraNameError
from app_manager.app_config_manager import ConfigManager

logger = Logger().get_logger()




class OkeraConnector:
    def __init__(self):

    
        self.system_property = ConfigManager().get_system_property()
        
        #get okera token from cofig.yaml
        self.auth_token = self.get_auth_token()

        #set connection with okera
        self.conn = self.get_conn_object(self.auth_token) 

    
    def get_auth_token(self):
        
        return self.system_property.get(ConfigConstants.okera_token)
       
    def get_conn_object(self,okera_token):
        try:
            ctx = context()
            ctx.enable_token_auth(token_str=okera_token)
            
            host = self.system_property.get(ConfigConstants.okera_host)
            port = int(self.system_property.get(ConfigConstants.okera_port))

            conn = ctx.connect(host=host, port=port)
            logger.debug('Connected to Okera Successfully!')
            return conn

        except Exception as e:
            raise OkeraConnectionError(e)
            
    
    def get_database_details(self,db_name):
        count = 30
        offset = 0
        all_datasets = []
        logger.debug("Fetching details of database '%s'!", db_name)

        try:
            dataset_names = self.conn.list_dataset_names(db_name)
            
            request = TGetDatasetsParams()
            request.dataset_names = dataset_names
            request.count = count
            request.offset = offset
            
            datasets = self.conn.service.client.GetDatasets(request).datasets
            all_datasets = datasets

            while datasets != []:
                offset = offset + count
                request.count = count
                request.offset = offset

           
                datasets = self.conn.service.client.GetDatasets(request).datasets
                all_datasets = all_datasets + datasets
            logger.debug("Fetching details of database '%s' completed", db_name)
            return all_datasets

        except Exception as e:
            raise OkeraUnspecifiedError(e)

        
    def get_dataset(self,db_name,table_name):

        try:
            table = self.conn.list_datasets(db=db_name, name=table_name)
            #logger.debug("Details of table '%s' is:  '%s", table_name,table)
            return table[0]

        except Exception as e:
            raise OkeraFetchError(e)

        

    
    # Checks if the namespace and key of the tag exist in Okera,
    # if they do not exist they are created
    def check_tag_exists(self,namespace,key):
        
        okera_tags = {}
        try:
            list_namespaces = self.conn.list_attribute_namespaces()
            for nmspc in list_namespaces:
                keys = []
                list_attributes = self.conn.list_attributes(nmspc)
                
                for attr in list_attributes:
                    keys.append(attr.key)
                okera_tags[nmspc] = keys

            if not okera_tags.get(namespace) or key not in okera_tags.get(namespace):
                
                logger.info("Creating new Okera tag and namespace '%s'",namespace+'.'+key)
                self.conn.create_attribute(namespace, key)

                if okera_tags.get(namespace):
                    okera_tags[namespace].append(key)
                else:
                    okera_tags[namespace] = [key]
                
        except Exception as e:
            raise OkeraUnspecifiedError(e)

    def assign_tags(self,name,namespace,value):
        
        # Check if the tags that are being assigned already exist in Okera
        # If they don't exist, they are created
        self.check_tag_exists(namespace, value)

        try:
            obj_name = name.split('.')
            if len(obj_name) == 3:
                self.conn.assign_attribute(
                    namespace, value, obj_name[0],
                    dataset=obj_name[1], column=obj_name[2])

            elif len(obj_name) == 2:
        
                self.conn.assign_attribute(
                    namespace, value, obj_name[0], dataset=obj_name[1])

            else:
                raise OkeraNameError(name)
            
            #logger.info(assign_success_log,name)


        except Exception as e:
            raise OkeraUnspecifiedError(e)


    def unassign_tags(self,name):
        try:
            obj_name = name.split('.')
            table_tag = self.get_dataset(obj_name[0],obj_name[1])

            if len(obj_name) == 2 :
                obj_type = 'Table'
                if table_tag.attribute_values:
                    for attribute in table_tag.attribute_values:
                        tbl_nmspc = attribute.attribute.attribute_namespace
                        tbl_key = attribute.attribute.key

                    self.conn.unassign_attribute(tbl_nmspc, tbl_key,
                                            obj_name[0], dataset=obj_name[1])

                else:
                    raise OkeraFetchError("Table '%s' does not contains any tag!!",name)
                    
            elif len(obj_name) == 3:
                obj_type = 'Column'
                if obj_name[2] in [col.name for col in table_tag.schema.cols]:
                    for col in table_tag.schema.cols:
                        if col.name == obj_name[2]:
                            if col.attribute_values:
                                for attribute in col.attribute_values:
                                    col_nmspc = attribute.attribute.attribute_namespace
                                    col_key = attribute.attribute.key

                                self.conn.unassign_attribute(col_nmspc,col_key, obj_name[0],
                                            dataset=obj_name[1], column=obj_name[2])

                            else:
                                raise OkeraFetchError("Column '%s' does not contains any tag!!",name)
                else:
                    raise OkeraFetchError("Column '%s' is not present in Table!!",name)

            else:
                raise OkeraNameError("Could not unassign tag from '%s', Please provide correct parameter!",name)
                
            #trace_log.info(unassign_success_log,obj_type,name)
                

        except Exception as e:
            raise OkeraUnspecifiedError(e)

    def add_desc(self,name,description,col_type=None, tbl_type=None):
        try:
            description = '' if not description else description
            ddl = ""
            obj_name = name.split('.')

            if len(obj_name) == 2:
                ddl = "ALTER TABLE " + name + \
                    " CHANGE COMMENT '" + description + "'"

            elif len(obj_name) == 3:
                tbl_name = '.'.join(obj_name[:-1])
                col_name = obj_name[-1]
                #if tbl_type == common.SyncObjectType.VIEW:
                if tbl_type == 'view':
                    ddl = "ALTER TABLE " + tbl_name + " CHANGE " + name.column + \
                        " " + name.column + " " + col_type + \
                        " COMMENT '" + description + "'"

                #if tbl_type == common.SyncObjectType.TABLE:
                else:
                    ddl = "ALTER TABLE " + \
                        tbl_name + " CHANGE COLUMN COMMENT " + \
                        col_name + " '" + description + "'"

            else:

                raise OkeraNameError('Please provide proper name! ' + name)

        except Exception as e:
            raise OkeraUnspecifiedError(e)
            
        try:
            self.conn.execute_ddl(ddl)
            logger.debug("Successfully changed description of "+name)
            
        except Exception as e:
            raise OkeraConnectionError(e)

