import collections

from concurrent.futures import ThreadPoolExecutor
from app_manager.utility.logs.app_logger import Logger
from app_manager.utility.common.app_constants import ConfigConstants
from app_manager.process.integration_process import CatalogIntegration


logger = Logger().get_logger()


Name = collections.namedtuple('Name', ['full_name', 'db', 'table', 'column'])

TYPE_IDS = ["BOOLEAN", "TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE", "STRING",
            "VARCHAR", "CHAR", "BINARY", "TIMESTAMP_NANOS", "DECIMAL", "DATE",
            "RECORD", "ARRAY", "MAP"]


class DgcOkeraIntegrator(CatalogIntegration):
    def __init__(self):                
        super().__init__()
        self.dataset_executor = ThreadPoolExecutor(max_workers=int(self.system_property.get(ConfigConstants.app_workers)))
        self.status = ''
        self.table_list = []

    def compare_tag(self,dgc_data,okera_data,obj_type):
        try:
            catalog_tags = dgc_data.get(obj_type + '_Security_Classification')
            if okera_data.get(obj_type+'_attributes'): 
                okera_tags = okera_data.get(obj_type + '_attributes',{}).get(obj_type + '_tags')
                
            else:
                okera_tags = None
            
            name = okera_data.get('name')
            
            #catalog_tags = {tag.lower() for tag in catalog_tags if catalog_tags is not None}
            #okera_tags = {tag.lower() for tag in okera_tags if okera_tags is not None}

            logger.debug('%s : catalog tags %s and Okera tags %s',name.full_name,catalog_tags,okera_tags)

            if okera_tags and catalog_tags:
                if collections.Counter(okera_tags) != collections.Counter(catalog_tags):
                    for tag in okera_tags:
                        self.okera_connector.unassign_tags(name.full_name)
                    for tag in catalog_tags:
                        self.okera_connector.assign_tags(name.full_name,tag.split('.')[0],'.'.join(tag.split('.')[1:]))
                    obj_name = name.full_name
                else:
                    logger.info('No difference found while comparing tags of '+ name.full_name)
                    obj_name = None

            elif catalog_tags and not okera_tags:
                for tag in catalog_tags:
                    self.okera_connector.assign_tags(name.full_name,tag.split('.')[0],'.'.join(tag.split('.')[1:]))
                obj_name = name.full_name

            elif okera_tags and not catalog_tags:
                for tag in okera_tags:
                    self.okera_connector.unassign_tags(name.full_name)
                obj_name = name.full_name

            else:
                logger.info('No difference found while comparing tags of '+ name.full_name)
                obj_name = None
            return obj_name
        except Exception as e:
            logger.error('Exception occure : %s ',str(e))
            message = 'Not able to update tags of ' + name.full_name
            self.status_obj.update_warn_message(self.input_object["sync_id"],message)        

    def compare_desc(self,dgc_data,okera_data,obj_type):
        try:
            catalog_desc = dgc_data.get(obj_type + 'Description')
            if okera_data.get(obj_type + '_attributes'): 
                okera_desc = okera_data.get(obj_type + '_attributes').get(obj_type + '_description')
            else:
                okera_desc = None

            name = okera_data.get('name')
            
            logger.debug('%s : Catalog desc %s and Okera desc %s',name.full_name,catalog_desc,okera_desc)

            if (okera_desc and not catalog_desc or catalog_desc and not
                okera_desc or (okera_desc and catalog_desc and
                            okera_desc != catalog_desc)):
                logger.debug("Differences found in %s, starting Okera description operations!",name)

                if obj_type.lower() == ConfigConstants.asset_type_table:

                    self.okera_connector.add_desc(name.full_name,catalog_desc)
                    

                elif obj_type.lower() == ConfigConstants.asset_type_column:
                    self.okera_connector.add_desc(name.full_name,catalog_desc,okera_data.get('col_obj_type').get('col_data_type'))

            else:
                logger.info("No difference is found in description of %s",name)
        except Exception as e:
            logger.error('Exception occure: %s ',str(e))


    def call_compare_opr(self,dgc_data,okera_data,obj_type):
        try:
            obj_name = self.compare_tag(dgc_data,okera_data,obj_type)
            
            if self.system_property.get(ConfigConstants.okera_sync_description).lower() == 'true':
                self.compare_desc(dgc_data,okera_data,obj_type)

            return obj_name

        except Exception as e:
            logger.error('Exception occure: %s ',str(e))

    def create_objects(self,dgc_asset,okera_asset):
        try:

            status_dict = {}
            table_list = []
            col_list = []

            #check if collibra asset table is present in okera
            for dgc_name, dgc_data in dgc_asset.items():
                okera_data = okera_asset.get(dgc_name) #full name
                if okera_data:
                    
                    logger.debug(dgc_name + ' table is present in Okera instance!')
                
                    status = self.call_compare_opr(dgc_data,okera_data,ConfigConstants.asset_type_table.capitalize())
                    if status:
                        table_list.append(status)
                    #compare childrens(columns) of table
                    dgc_data_child = dgc_data.get('Columns')
                    okera_data_child =  okera_data.get('Columns')
                    
                    for dgc_child in dgc_data_child:
                        
                        match_index = [index for index, val in enumerate(okera_data_child) if dgc_child['columnName'] == val.get('name').full_name]
                        
                        if match_index:
                            
                            okera_child = okera_data_child[match_index[0]]
                            logger.debug(okera_child['col_display_name'] + ' column is present in Okera instance!')
                            
                            status = self.call_compare_opr(dgc_child,okera_child,ConfigConstants.asset_type_column.capitalize())
                            if status:
                                col_list.append(status)
                                
                        else:
                            message = dgc_child['columnName'] + ' column is not present in okera table ' + dgc_name
                            logger.warning(message)
                            self.status_obj.update_warn_message(self.input_object["sync_id"],message)

                else:
                    message = dgc_name + ' table is not present in Okera instance!'
                    logger.warning(message)
                    self.status_obj.update_warn_message(self.input_object["sync_id"],message)

            status_dict[ConfigConstants.asset_type_table] = len(table_list)
            status_dict[ConfigConstants.asset_type_column] = len(col_list)
            
            if status_dict.get('table') != 0 or status_dict.get('column') != 0:
                self.status = 'COMPLETED'
                logger.debug('COMPLETED %s',status_dict)
                self.status_obj.update_execution_status(self.input_object["sync_id"],status_dict)

            else:
                self.status = 'ERROR'
                self.status_obj.update_error_message(self.input_object["sync_id"],"Not able to compare Collibra and Okera details!!")
        
        except Exception as e:
            logger.error('Exception: %s ',str(e))

    
    def fetch_dgc_assets(self):
        
        if self.dgc_loaded_from_edge.lower() == 'true' and len(self.input_object['system_name'])!=0:
            db_name = self.input_object['system_name'] + self.dgc_delimiter + self.input_object['database_name']
        else:
            db_name = self.input_object['database_name']

        schema_name = db_name + self.dgc_delimiter + self.input_object["schema_name"]

        #if no table name mention. 'database level'
        if self.opr_level == ConfigConstants.asset_type_database:
            if self.dgc_loaded_from_edge.lower() == 'true':
                asset_tvc = self.tvc_obj.form_asset_tvc(self.input_object,db_name,schema_name=schema_name)
            else:
                asset_tvc = self.tvc_obj.form_asset_tvc(self.input_object,db_name)

            asset_export = self.dgc_connector.fetch_asset_json(asset_tvc)
            
        
            if not asset_export:
                logger.error("Not able to fetch any details from Collibra!!")
                 

        elif self.opr_level == ConfigConstants.asset_type_table:
            asset_export = []
            for tbl in self.tbl_list:
                if self.dgc_loaded_from_edge.lower() == 'true':
                    schema_name = db_name + self.dgc_delimiter + self.input_object["schema_name"]
                    full_name_tbl = db_name + self.dgc_delimiter + self.input_object["schema_name"] + self.dgc_delimiter + tbl
                    asset_tvc = self.tvc_obj.form_asset_tvc(self.input_object,db_name,tbl_name=full_name_tbl,schema_name=schema_name)
                else:
                    full_name_tbl = tbl
                    asset_tvc = self.tvc_obj.form_asset_tvc(self.input_object,db_name,tbl_name=full_name_tbl)
                
                asset_export_tbl = self.dgc_connector.fetch_asset_json(asset_tvc)
                
                if asset_export_tbl: 
                    asset_export.append(asset_export_tbl[0])
                else:
                    message = "Not able to fetch details from Collibra table " + tbl
                    logger.warning(message)
                    self.status_obj.update_warn_message(self.input_object["sync_id"],message)
                    

        else:
            message = "Not able to fetch any details from Collibra!!"
            logger.error(message)
            

        dgc_data = self.create_table_object_dgc(asset_export)
        for i,val in dgc_data.items():
            logger.debug('collibra Table %s Details %s',i, val)
            
        return dgc_data


    def fetch_okera_assets(self):

        #if no table name mention. 'database level'
        if self.opr_level == ConfigConstants.asset_type_database:
            all_datasets = self.okera_connector.get_database_details(self.input_object['database_name'])
            if not all_datasets:
                logger.error('"Not able to fetch any details from Okera!!"')
          
        elif self.opr_level == ConfigConstants.asset_type_table:
            all_datasets = []
            for tbl_name in self.tbl_list:
                tbl = self.okera_connector.get_dataset(self.input_object['database_name'],tbl_name)
                if tbl:
                    all_datasets.append(tbl)
                else:
                    message = "Not able to fetch details from Okera table " + tbl_name
                    logger.warning(message)
                    self.status_obj.update_warn_message(self.input_object["sync_id"],message)


        else:
            logger.error("Not able to fetch any details from Okera!!")
            
        okera_data = {}
        mapped_namespaces = self.get_mapped_attributes()

        for tbl_details in all_datasets:
            data,tbl_name = self.create_table_object_okera(tbl_details,mapped_namespaces)
            okera_data[tbl_name] = data
            

        for i, val in okera_data.items():
            logger.debug('Okera Table %s Details %s',i, val)

        return okera_data
            
    
    def create_table_object_dgc(self,tbl_dgc_data):
        try:
            dgc_dict = {}
            
            for data in tbl_dgc_data:
                if self.dgc_loaded_from_edge.lower() == 'true':
                    tbl_name = data['Fullname']
                    if len(self.input_object['system_name'])!=0:
                        tbl_name = tbl_name.replace(self.input_object['system_name'] + self.dgc_delimiter, '')
                    
                    tbl_name = tbl_name.replace(self.input_object['schema_name'] + self.dgc_delimiter, '')
                    
                elif self.dgc_loaded_from_edge.lower() == 'false':
                    tbl_name = self.input_object['database_name'] + self.dgc_delimiter + data['Fullname']
                
                else:
                    tbl_name = data['Fullname']

                tbl_name = tbl_name.replace(self.dgc_delimiter,self.okera_delimiter)

                #format tags of catalog
                data['Table_Security_Classification'] = self.format_catalog_tags(data.get('Table_Security_Classification'),data.get('Status_Name'))
                
                if data.get('Columns'):
                    for col_name in data.get('Columns'):
                        if self.dgc_loaded_from_edge.lower() == 'true':
                            col_full_name = col_name['columnName']
                            if len(self.input_object['system_name'])!=0:
                                col_full_name = col_full_name.replace(self.input_object['system_name'] + self.dgc_delimiter, '')
                            col_full_name = col_full_name.replace(self.input_object['schema_name'] + self.dgc_delimiter, '')
                        
                        elif self.dgc_loaded_from_edge.lower() == 'false':
                            col_full_name = self.input_object['database_name'] + self.dgc_delimiter + col_name['columnName']

                        else:
                            col_full_name = col_name['columnName']

                        #if Security classification set as a relation, then fetch the details using below tvc
                        if self.dgc_tag_as_relation == 'true':
                            col_asset_tvc = self.tvc_obj.form_column_tvc(self.input_object,col_name['columnName'],data['Fullname'])
                            col_name['Column_Security_Classification'] = list(self.dgc_connector.fetch_asset_json(col_asset_tvc)[0].values())[0]
                        
                        #format column tags
                        col_name['Column_Security_Classification'] = self.format_catalog_tags(col_name.get('Column_Security_Classification'))
                        col_name['columnName'] = col_full_name.replace(self.dgc_delimiter,self.okera_delimiter).replace(" ",'')
                        
                dgc_dict[tbl_name] = data
            return dgc_dict
                
        except Exception as e:
            logger.error('Exception: %s ',str(e))
    
    def create_table_object_okera(self,tbl_data,mapped_namespaces,mapped_obj_id=None,):
        try:
            tbl_name = Name('.'.join([tbl_data.db[0], tbl_data.name]), tbl_data.db[0], tbl_data.name, None)
            
            tbl_type = ConfigConstants.asset_type_table
            if tbl_data.primary_storage == "View":
                tbl_type = ConfigConstants.asset_type_view
            
            table_dict = {
                'name': tbl_name,
                'display_name': tbl_name.table,
                'obj_type': {'obj_type': ConfigConstants.asset_type_table, 
                            'tbl_type': tbl_type},
                'obj_id' : mapped_obj_id,
                'relation': tbl_data.db[0],
                'Table_attributes': {
                        'Table_description': tbl_data.description,
                        'Table_tags': self.format_okera_tags(tbl_data.attribute_values,mapped_namespaces)}
                        
            }

            columns_list = []
            i = 0
            while i < len(tbl_data.schema.cols):
                col = tbl_data.schema.cols[i]
                full_col_name = '.'.join([tbl_data.db[0], tbl_data.name, col.name])
                data_type = TYPE_IDS[col.type.type_id]

                col_name = Name(full_col_name, tbl_data.db[0], tbl_data.name, col.name)

                column_dict = {
                    'name': col_name,
                    'col_display_name': col_name.column,
                    'col_obj_type':{
                            'col_obj_type': ConfigConstants.asset_type_column, 
                            'col_data_type': data_type},

                    'col_relation': tbl_data.db[0],
                    'Column_attributes':{
                        'Column_description': col.comment,
                        'Column_tags':self.format_okera_tags(col.attribute_values,mapped_namespaces),
                        'Column_position': str(i)}
                }
                
                columns_list.append(column_dict)

                i += 1

            table_dict['Columns'] = columns_list

            return table_dict,tbl_name.full_name
            
        except Exception as e:
            logger.error('Exception: %s ',str(e))

    def format_catalog_tags(self,catalog_tags,catalog_status = None):
        try:
            tags = set()
            namespace = self.system_property.get(ConfigConstants.okera_namespace)
            status_namespace = self.system_property.get(ConfigConstants.okera_namespace_status)
            
            if catalog_tags:
                if self.dgc_tag_as_relation == 'true':
                    for tag in catalog_tags:
                        tags.add(namespace + '.' + list( tag.values())[0])
                else:
                    tags.add(namespace + '.' + catalog_tags)
           
            if catalog_status:
                tags.add(status_namespace + '.' + catalog_status)

            return tags

        except Exception as e:
            logger.warning("Exception occured while converting catalog tags!! %s", e)

    def process_integration(self):
        logger.info("Begin")
        
        dgc_asset = self.fetch_dgc_assets()
        okera_asset = self.fetch_okera_assets()
        
        if dgc_asset and okera_asset:
            self.create_objects(dgc_asset,okera_asset)

        else:
            self.status = "ERROR"
            message = "Okera or Collibra details not able to fetched!! please refer logs"
            self.status_obj.update_error_message(self.input_object["sync_id"],message)
        
        logger.info('end')

    
    def set_opr_level(self):
        
        if self.input_object.get('table_list'):
            self.opr_level = ConfigConstants.asset_type_table
            
            for i in self.input_object.get('table_list'):
                self.tbl_list.append(i.table_name)

        else:
            self.opr_level = ConfigConstants.asset_type_database

    def process(self):
        logger.debug('Begin')
        try:
            logger.debug(self.input_object)
        
            self.status_obj.initialize_status(self.input_object["sync_id"])
            self.set_opr_level()
            self.process_integration()
            
        except Exception as e:
            logger.error('Exception e occured! %s',e) 

        if self.status == '':
            self.status = 'ERROR'

        self.status_obj.complete_status(self.input_object["sync_id"], self.status)
        logger.debug('end')
        