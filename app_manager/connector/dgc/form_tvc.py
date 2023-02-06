
from app_manager.app_config_manager import ConfigManager
from app_manager.utility.common.app_constants import ConfigConstants
from app_manager.connector.dgc.dgc_connector import DGCConnector

class TvcConstants:
    table_view_config = "TableViewConfig"
    display_length = "displayLength"
    full_name = "Fullname" 
    full_name_val = "Full name" 
    vocabulary = "Vocabulary"
    domain_name_value = "Domain_Name"
    domain_id_value = "Domain_Id"
    id_key = "Id"
    id_value = "id"
    status_id = "Status_Id"
    concept_type = "ConceptType"
    target = "TARGET"
    target_small = "Target"
    source = "Source"
    SOURCE = "SOURCE"
    column_id = "columnID"
    column_name = "columnName"
    tbl_id = "tableID"
    tbl_name = "tableName"
    db_id = "databaseID"
    db_name = "databaseName"
    schema_name = 'schemaName'
    schema_id = 'schemaID'
    tbl_Security_Classification_id = 'Table_Security_Classification_ID'
    col_Security_Classification_id = 'Column_Security_Classification_ID'
    column_name_key = "column"
    attribute = "Attribute"
    string_attribute ="StringAttribute"
    resources = "Resources"
    asset = "Asset"
    term = 'Term'
    signifier = "Signifier"
    name = "name"
    order_key = "Order"
    order_value = "order"
    display_name_key = "DisplayName"
    display_name_value = "displayName"
    
    status_key = "Status"
    status_value = "Status_Name"
    asset_type_key = "AssetType"
    asset_type_id_value = "AssetType_Id"
    asset_type_name_value = "AssetType_Name"
    asset_type_name_value_key = "Asset Type"
    
    community_key = "Community"
    community_id_value = "communityId"
    community_name_value = "communityName"
    name_key = "Name"
    domain_name_value = "Domain_Name"
    
    
    tag_key = "Tag"
    tag_value = "Tag"
    
    filter_key = "Filter"
    and_operator = "AND"    
    field_key = "Field"
    operator_key = "operator"
    in_operator = "IN"
    equals_operator = "EQUALS"
    not_in_operator = "NOT_IN"
    value_key = "value"
    Value_key = 'Value'
    values_key = "values"
    columns_key = "Columns"
    group_key = "Group"
    column_key = "Column"
    field_name_key = "fieldName"
    label_key = "label"
    col_security_classification = "Column_Security_Classification"
    security_classification_key = "Security Classification"
    col_description = "ColumnDescription"
    description_key = "Description"
    tbl_security_classification = "Table_Security_Classification"
    tbl_security_classification_list = "Table_Security_Classification_list"
    col_security_classification_list = "Column_Security_Classification_list"
    tbl_description = "TableDescription"

    single_value_list_attribute_value_key = "single_value_list_attribute_value"

    relation_key = "Relation"
    type_id_key = "typeId"
    type_key = "type"
    
    string_attribute_key = "StringAttribute"
    single_value_attribute_key = "SingleValueListAttribute"
    
    label_id_key = "labelId"
    
    dataset_table_relation = "dataset_table_relation"
    dataset_related_table_id = "dataset_table_id"
    dataset_related_table_name = "dataset_table_name"
    dataset_related_table_domain = "dataset_table_domain"
    dataset_related_table_community = "dataset_table_community"


class TableViewConfig:
    def __init__(self):        
        self.system_property = ConfigManager().get_system_property()
        self.dgc_connector = DGCConnector()
        
        self.attr_description_id = None
        self.attr_security_classification_id = None
        self.table_database_relation_id = None 
        self.col_table_relation_id = None
        self.table_schema_relation_id =None
        self.table_tag_relation_id =None
        self.column_tag_relation_id = None

    

    def form_asset_tvc(self,asset_input,database_name,tbl_name = None, schema_name =None):
        
        asset_tvc_json = {}
        asset_tvc_json[TvcConstants.table_view_config] = {}
        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.display_length] = -1

        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources] = {}
        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources][TvcConstants.term] = {}
        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources][TvcConstants.term][TvcConstants.signifier] = {}
        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources][TvcConstants.term][TvcConstants.signifier][TvcConstants.name] = TvcConstants.full_name

        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources][TvcConstants.term][TvcConstants.display_name_key] = {}
        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources][TvcConstants.term][TvcConstants.display_name_key][TvcConstants.name] = TvcConstants.display_name_key
        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources][TvcConstants.term][TvcConstants.id_key] = {}
        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources][TvcConstants.term][TvcConstants.id_key][TvcConstants.name] = TvcConstants.id_key
        
        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources][TvcConstants.term][TvcConstants.vocabulary] = {}
        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources][TvcConstants.term][TvcConstants.vocabulary][TvcConstants.name_key] = {}
        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources][TvcConstants.term][TvcConstants.vocabulary][TvcConstants.name_key][TvcConstants.name] = TvcConstants.domain_name_value
        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources][TvcConstants.term][TvcConstants.vocabulary][TvcConstants.id_key] = {}
        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources][TvcConstants.term][TvcConstants.vocabulary][TvcConstants.id_key][TvcConstants.name] = TvcConstants.domain_id_value
        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources][TvcConstants.term][TvcConstants.vocabulary][TvcConstants.community_key] = {}
        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources][TvcConstants.term][TvcConstants.vocabulary][TvcConstants.community_key][TvcConstants.id_key] = {}
        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources][TvcConstants.term][TvcConstants.vocabulary][TvcConstants.community_key][TvcConstants.id_key][TvcConstants.name] = TvcConstants.community_id_value
        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources][TvcConstants.term][TvcConstants.vocabulary][TvcConstants.community_key][TvcConstants.name_key] = {}
        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources][TvcConstants.term][TvcConstants.vocabulary][TvcConstants.community_key][TvcConstants.name_key][TvcConstants.name] = TvcConstants.community_name_value

        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources][TvcConstants.term][TvcConstants.status_key] = {}
        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources][TvcConstants.term][TvcConstants.status_key][TvcConstants.signifier] = {}
        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources][TvcConstants.term][TvcConstants.status_key][TvcConstants.signifier][TvcConstants.name] = TvcConstants.status_value
        

        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources][TvcConstants.term][TvcConstants.concept_type] = {}
        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources][TvcConstants.term][TvcConstants.concept_type][TvcConstants.signifier] = {}
        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources][TvcConstants.term][TvcConstants.concept_type][TvcConstants.signifier][TvcConstants.name] = TvcConstants.asset_type_name_value
        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources][TvcConstants.term][TvcConstants.concept_type][TvcConstants.id_key] = {}
        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources][TvcConstants.term][TvcConstants.concept_type][TvcConstants.id_key][TvcConstants.name_key] = TvcConstants.asset_type_id_value

        Relation_list = []
        col_relation_attr = {}

        source = self.system_property.get(ConfigConstants.dgc_column_table_relation_source)
        role = self.system_property.get(ConfigConstants.dgc_column_table_relation_role)
        corole = self.system_property.get(ConfigConstants.dgc_column_table_relation_corole)
        target = self.system_property.get(ConfigConstants.dgc_column_table_relation_target)
        if self.col_table_relation_id == None:
            self.col_table_relation_id = self.dgc_connector.fetch_relation_id(source, role, corole, target)

        col_relation_attr[TvcConstants.type_id_key] = self.col_table_relation_id
        col_relation_attr[TvcConstants.type_key] = TvcConstants.target
        col_relation_attr[TvcConstants.source] = {}
        col_relation_attr[TvcConstants.source][TvcConstants.id_key] = {}
        col_relation_attr[TvcConstants.source][TvcConstants.id_key][TvcConstants.name] = TvcConstants.column_id
        col_relation_attr[TvcConstants.source][TvcConstants.signifier]={}
        col_relation_attr[TvcConstants.source][TvcConstants.signifier][TvcConstants.name] = TvcConstants.column_name
       
        Attribute_list = []
        if self.system_property.get(ConfigConstants.dgc_security_classification_as_relation).lower() == 'false':
            
            security_cls_attribute = {}
            if self.attr_security_classification_id == None:
                self.attr_security_classification_id = self.dgc_connector.fetch_attribute_id(self.system_property.get(ConfigConstants.dgc_attr_security_classification))

            security_cls_attribute[TvcConstants.label_id_key] = self.attr_security_classification_id
            security_cls_attribute[TvcConstants.Value_key] ={}
            security_cls_attribute[TvcConstants.Value_key][TvcConstants.name]=TvcConstants.col_security_classification
            Attribute_list.append(security_cls_attribute)

        desc_attribute = {}
        if self.attr_description_id == None:
            self.attr_description_id = self.dgc_connector.fetch_attribute_id(self.system_property.get(ConfigConstants.dgc_attr_dataset_desc))

        desc_attribute[TvcConstants.label_id_key] = self.attr_description_id
        desc_attribute[TvcConstants.Value_key] ={}
        desc_attribute[TvcConstants.Value_key][TvcConstants.name]=TvcConstants.col_description

        Attribute_list.append(desc_attribute)
        col_relation_attr[TvcConstants.source][TvcConstants.attribute] = Attribute_list

        if schema_name is None:
            source = self.system_property.get(ConfigConstants.dgc_table_database_relation_source)
            role = self.system_property.get(ConfigConstants.dgc_table_database_relation_role)
            corole = self.system_property.get(ConfigConstants.dgc_table_database_relation_corole)
            target = self.system_property.get(ConfigConstants.dgc_table_database_relation_target)
            
            if self.table_database_relation_id == None:
                self.table_database_relation_id = self.dgc_connector.fetch_relation_id(source, role, corole, target)

            tbl_db_relation_attr = {}
            tbl_db_relation_attr[TvcConstants.type_id_key] = self.table_database_relation_id
            tbl_db_relation_attr[TvcConstants.type_key] = TvcConstants.SOURCE
            tbl_db_relation_attr[TvcConstants.target_small] = {}
            tbl_db_relation_attr[TvcConstants.target_small][TvcConstants.id_key] = {}
            tbl_db_relation_attr[TvcConstants.target_small][TvcConstants.id_key][TvcConstants.name] = TvcConstants.db_id
            tbl_db_relation_attr[TvcConstants.target_small][TvcConstants.signifier]={}
            tbl_db_relation_attr[TvcConstants.target_small][TvcConstants.signifier][TvcConstants.name] = TvcConstants.db_name
            Relation_list.append(tbl_db_relation_attr)
        else:
            source = self.system_property.get(ConfigConstants.dgc_table_schema_relation_source)
            role = self.system_property.get(ConfigConstants.dgc_table_schema_relation_role)
            corole = self.system_property.get(ConfigConstants.dgc_table_schema_relation_corole)
            target = self.system_property.get(ConfigConstants.dgc_table_schema_relation_target)
            
            if self.table_schema_relation_id == None:
                self.table_schema_relation_id = self.dgc_connector.fetch_relation_id(source, role, corole, target)

            tbl_schema_relation_attr = {}
            tbl_schema_relation_attr[TvcConstants.type_key] = TvcConstants.target
            tbl_schema_relation_attr[TvcConstants.type_id_key] = self.table_schema_relation_id
            tbl_schema_relation_attr[TvcConstants.source] = {}
            tbl_schema_relation_attr[TvcConstants.source][TvcConstants.id_key] = {}
            tbl_schema_relation_attr[TvcConstants.source][TvcConstants.id_key][TvcConstants.name] = TvcConstants.schema_id
            tbl_schema_relation_attr[TvcConstants.source][TvcConstants.signifier]={}
            tbl_schema_relation_attr[TvcConstants.source][TvcConstants.signifier][TvcConstants.name] = TvcConstants.schema_name
            Relation_list.append(tbl_schema_relation_attr)

        
        if self.system_property.get(ConfigConstants.dgc_security_classification_as_relation).lower() == 'true':
            source = self.system_property.get(ConfigConstants.dgc_table_security_classification_relation_source)
            role = self.system_property.get(ConfigConstants.dgc_table_security_classification_relation_role)
            corole = self.system_property.get(ConfigConstants.dgc_table_security_classification_relation_corole)
            target = self.system_property.get(ConfigConstants.dgc_table_security_classification_relation_target)
            
            if self.table_tag_relation_id == None:
                self.table_tag_relation_id = self.dgc_connector.fetch_relation_id(source, role, corole, target)

            tbl_sc_relation = {}
            tbl_sc_relation[TvcConstants.type_key] = TvcConstants.SOURCE
            tbl_sc_relation[TvcConstants.type_id_key] = self.table_tag_relation_id
            tbl_sc_relation[TvcConstants.target_small] = {}
            tbl_sc_relation[TvcConstants.target_small][TvcConstants.id_key] = {}
            tbl_sc_relation[TvcConstants.target_small][TvcConstants.id_key][TvcConstants.name] = TvcConstants.tbl_Security_Classification_id
            tbl_sc_relation[TvcConstants.target_small][TvcConstants.signifier]={}
            tbl_sc_relation[TvcConstants.target_small][TvcConstants.signifier][TvcConstants.name] = TvcConstants.tbl_security_classification
            Relation_list.append(tbl_sc_relation)

        Relation_list.append(col_relation_attr)         
        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources][TvcConstants.term][TvcConstants.relation_key] = Relation_list

        if self.system_property.get(ConfigConstants.dgc_security_classification_as_relation).lower() == 'false':
            SingleValueListAttribute = []
            tbl_security_cls_attribute = {}
            tbl_security_cls_attribute[TvcConstants.label_id_key] = self.attr_security_classification_id
            tbl_security_cls_attribute[TvcConstants.Value_key] ={}
            tbl_security_cls_attribute[TvcConstants.Value_key][TvcConstants.name]=TvcConstants.tbl_security_classification
            
            SingleValueListAttribute.append(tbl_security_cls_attribute)
            asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources][TvcConstants.term][TvcConstants.single_value_attribute_key] = SingleValueListAttribute


        StringAttribute = []
        tbl_desc_attribute = {}
        tbl_desc_attribute[TvcConstants.label_id_key] = self.attr_description_id
        tbl_desc_attribute[TvcConstants.Value_key] ={}
        tbl_desc_attribute[TvcConstants.Value_key][TvcConstants.name]=TvcConstants.tbl_description

        StringAttribute.append(tbl_desc_attribute)
        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources][TvcConstants.term][TvcConstants.string_attribute] = StringAttribute
        
        comm_name_filter = {}
        comm_name_filter[TvcConstants.field_key] = {}
        comm_name_filter[TvcConstants.field_key][TvcConstants.name] = TvcConstants.community_name_value
        comm_name_filter[TvcConstants.field_key][TvcConstants.operator_key] = TvcConstants.equals_operator
        comm_name_filter[TvcConstants.field_key][TvcConstants.value_key] = asset_input['community_name']
        domain_name_filter = {}
        domain_name_filter[TvcConstants.field_key] = {}
        domain_name_filter[TvcConstants.field_key][TvcConstants.name] = TvcConstants.domain_name_value
        domain_name_filter[TvcConstants.field_key][TvcConstants.operator_key] = TvcConstants.equals_operator
        domain_name_filter[TvcConstants.field_key][TvcConstants.value_key] = asset_input['domain_name'] 
        full_name_filter  = {}
        full_name_filter[TvcConstants.field_key] = {}
        full_name_filter[TvcConstants.field_key][TvcConstants.name] = TvcConstants.full_name
        full_name_filter[TvcConstants.field_key][TvcConstants.operator_key] = TvcConstants.equals_operator
        full_name_filter[TvcConstants.field_key][TvcConstants.value_key] = tbl_name 
        
        if schema_name is None:
            db_name_filter  = {}
            db_name_filter[TvcConstants.field_key] = {}
            db_name_filter[TvcConstants.field_key][TvcConstants.name] = TvcConstants.db_name
            db_name_filter[TvcConstants.field_key][TvcConstants.operator_key] = TvcConstants.equals_operator
            db_name_filter[TvcConstants.field_key][TvcConstants.value_key] = database_name
        
        else:
            schema_name_filter  = {}
            schema_name_filter[TvcConstants.field_key] = {}
            schema_name_filter[TvcConstants.field_key][TvcConstants.name] = TvcConstants.schema_name
            schema_name_filter[TvcConstants.field_key][TvcConstants.operator_key] = TvcConstants.equals_operator
            schema_name_filter[TvcConstants.field_key][TvcConstants.value_key] = schema_name

        filter_list = []
        filter_list.append(comm_name_filter)
        filter_list.append(domain_name_filter)
        if schema_name is None:
            filter_list.append(db_name_filter)
        else:
            #change: add db also?
            filter_list.append(schema_name_filter)
        
        if tbl_name:
            filter_list.append(full_name_filter)
        #filter_list.append(asset_type_filter)

        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources][TvcConstants.term][TvcConstants.filter_key] = {}
        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources][TvcConstants.term][TvcConstants.filter_key][TvcConstants.and_operator] = filter_list

        full_name_column = {}
        full_name_column[TvcConstants.column_key] = {}
        full_name_column[TvcConstants.column_key][TvcConstants.field_name_key] = TvcConstants.full_name
        full_name_column[TvcConstants.column_key][TvcConstants.label_key] = TvcConstants.full_name_val

        display_name_column = {}
        display_name_column[TvcConstants.column_key] = {}
        display_name_column[TvcConstants.column_key][TvcConstants.field_name_key] = TvcConstants.display_name_key
        display_name_column[TvcConstants.column_key][TvcConstants.label_key] = TvcConstants.name

        status_name_column = {}
        status_name_column[TvcConstants.column_key] = {}
        status_name_column[TvcConstants.column_key][TvcConstants.field_name_key] = TvcConstants.status_value
        status_name_column[TvcConstants.column_key][TvcConstants.label_key] = TvcConstants.status_key

        asset_type_name_column = {}
        asset_type_name_column[TvcConstants.column_key] = {}
        asset_type_name_column[TvcConstants.column_key][TvcConstants.field_name_key] = TvcConstants.asset_type_name_value
        asset_type_name_column[TvcConstants.column_key][TvcConstants.label_key] = TvcConstants.asset_type_name_value_key

        

        col_op_list = []
        column_name_op = {}
        column_name_op[TvcConstants.column_key] = {}
        column_name_op[TvcConstants.column_key][TvcConstants.field_name_key] = TvcConstants.column_name
        column_name_op[TvcConstants.column_key][TvcConstants.label_key] = TvcConstants.column_name_key

        col_sc_op = {}
        col_sc_op[TvcConstants.column_key] = {}
        col_sc_op[TvcConstants.column_key][TvcConstants.field_name_key] = TvcConstants.col_security_classification
        col_sc_op[TvcConstants.column_key][TvcConstants.label_key] = TvcConstants.security_classification_key

        col_desc_op = {}
        col_desc_op[TvcConstants.column_key] = {}
        col_desc_op[TvcConstants.column_key][TvcConstants.field_name_key] = TvcConstants.col_description
        col_desc_op[TvcConstants.column_key][TvcConstants.label_key] = TvcConstants.description_key

        col_op_list.append(column_name_op)
        if self.system_property.get(ConfigConstants.dgc_security_classification_as_relation).lower() == 'false':
            col_op_list.append(col_sc_op)
        col_op_list.append(col_desc_op)

        group_filter  = {}
        group_filter[TvcConstants.group_key]  = {}
        group_filter[TvcConstants.group_key][TvcConstants.name] = TvcConstants.columns_key
        group_filter[TvcConstants.group_key][TvcConstants.columns_key] = col_op_list

        if self.system_property.get(ConfigConstants.dgc_security_classification_as_relation).lower() == 'false':
            tbl_sc_column = {}
            tbl_sc_column[TvcConstants.column_key] = {}
            tbl_sc_column[TvcConstants.column_key][TvcConstants.field_name_key] = TvcConstants.tbl_security_classification
            tbl_sc_column[TvcConstants.column_key][TvcConstants.label_key] = TvcConstants.security_classification_key

        elif self.system_property.get(ConfigConstants.dgc_security_classification_as_relation).lower() == 'true':
            tbl_sc_list = []
            tbl_sc = {}
            tbl_sc[TvcConstants.column_key] = {}
            tbl_sc[TvcConstants.column_key][TvcConstants.field_name_key] = TvcConstants.tbl_security_classification
            tbl_sc[TvcConstants.column_key][TvcConstants.label_key] = TvcConstants.security_classification_key
            tbl_sc_list.append(tbl_sc)

            tbl_sc_column  = {}
            tbl_sc_column[TvcConstants.group_key]  = {}
            tbl_sc_column[TvcConstants.group_key][TvcConstants.name] = TvcConstants.tbl_security_classification
            tbl_sc_column[TvcConstants.group_key][TvcConstants.columns_key] = tbl_sc_list

        tbl_desc_column = {}
        tbl_desc_column[TvcConstants.column_key] = {}
        tbl_desc_column[TvcConstants.column_key][TvcConstants.field_name_key] = TvcConstants.tbl_description
        tbl_desc_column[TvcConstants.column_key][TvcConstants.label_key] = TvcConstants.description_key

        final_column_list = []
        final_column_list.append(full_name_column)
        final_column_list.append(display_name_column)
        final_column_list.append(status_name_column)
        final_column_list.append(asset_type_name_column)
        final_column_list.append(group_filter)
        final_column_list.append(tbl_sc_column)
        final_column_list.append(tbl_desc_column)

        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.columns_key] = final_column_list
        
        return asset_tvc_json

    def form_column_tvc(self,asset_input,col_name,table_name):
        asset_tvc_json = {}
        asset_tvc_json[TvcConstants.table_view_config] = {}
        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.display_length] = -1

        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources] = {}
        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources][TvcConstants.term] = {}
        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources][TvcConstants.term][TvcConstants.signifier] = {}
        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources][TvcConstants.term][TvcConstants.signifier][TvcConstants.name] = TvcConstants.full_name

        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources][TvcConstants.term][TvcConstants.display_name_key] = {}
        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources][TvcConstants.term][TvcConstants.display_name_key][TvcConstants.name] = TvcConstants.display_name_key
        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources][TvcConstants.term][TvcConstants.id_key] = {}
        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources][TvcConstants.term][TvcConstants.id_key][TvcConstants.name] = TvcConstants.id_key
        
        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources][TvcConstants.term][TvcConstants.vocabulary] = {}
        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources][TvcConstants.term][TvcConstants.vocabulary][TvcConstants.name_key] = {}
        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources][TvcConstants.term][TvcConstants.vocabulary][TvcConstants.name_key][TvcConstants.name] = TvcConstants.domain_name_value
        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources][TvcConstants.term][TvcConstants.vocabulary][TvcConstants.id_key] = {}
        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources][TvcConstants.term][TvcConstants.vocabulary][TvcConstants.id_key][TvcConstants.name] = TvcConstants.domain_id_value
        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources][TvcConstants.term][TvcConstants.vocabulary][TvcConstants.community_key] = {}
        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources][TvcConstants.term][TvcConstants.vocabulary][TvcConstants.community_key][TvcConstants.id_key] = {}
        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources][TvcConstants.term][TvcConstants.vocabulary][TvcConstants.community_key][TvcConstants.id_key][TvcConstants.name] = TvcConstants.community_id_value
        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources][TvcConstants.term][TvcConstants.vocabulary][TvcConstants.community_key][TvcConstants.name_key] = {}
        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources][TvcConstants.term][TvcConstants.vocabulary][TvcConstants.community_key][TvcConstants.name_key][TvcConstants.name] = TvcConstants.community_name_value

        Relation_list = []
        source = self.system_property.get(ConfigConstants.dgc_column_security_classification_relation_source)
        role = self.system_property.get(ConfigConstants.dgc_column_security_classification_relation_role)
        corole = self.system_property.get(ConfigConstants.dgc_column_security_classification_relation_corole)
        target = self.system_property.get(ConfigConstants.dgc_column_security_classification_relation_target)
        
        if self.column_tag_relation_id == None:
                self.column_tag_relation_id = self.dgc_connector.fetch_relation_id(source, role, corole, target)
            
        col_sc_relation = {}
        col_sc_relation[TvcConstants.type_key] = TvcConstants.SOURCE
        col_sc_relation[TvcConstants.type_id_key] = self.column_tag_relation_id
        col_sc_relation[TvcConstants.target_small] = {}
        col_sc_relation[TvcConstants.target_small][TvcConstants.id_key] = {}
        col_sc_relation[TvcConstants.target_small][TvcConstants.id_key][TvcConstants.name] = TvcConstants.col_Security_Classification_id
        col_sc_relation[TvcConstants.target_small][TvcConstants.signifier]={}
        col_sc_relation[TvcConstants.target_small][TvcConstants.signifier][TvcConstants.name] = TvcConstants.col_security_classification
        

        col_relation_attr = {}
        source = self.system_property.get(ConfigConstants.dgc_column_table_relation_source)
        role = self.system_property.get(ConfigConstants.dgc_column_table_relation_role)
        corole = self.system_property.get(ConfigConstants.dgc_column_table_relation_corole)
        target = self.system_property.get(ConfigConstants.dgc_column_table_relation_target)
        if self.col_table_relation_id == None:
            self.col_table_relation_id = self.dgc_connector.fetch_relation_id(source, role, corole, target)

        col_relation_attr[TvcConstants.type_id_key] = self.col_table_relation_id
        col_relation_attr[TvcConstants.type_key] = TvcConstants.SOURCE
        col_relation_attr[TvcConstants.target_small] = {}
        col_relation_attr[TvcConstants.target_small][TvcConstants.id_key] = {}
        col_relation_attr[TvcConstants.target_small][TvcConstants.id_key][TvcConstants.name] = TvcConstants.tbl_id
        col_relation_attr[TvcConstants.target_small][TvcConstants.signifier]={}
        col_relation_attr[TvcConstants.target_small][TvcConstants.signifier][TvcConstants.name] = TvcConstants.tbl_name
       
        Relation_list.append(col_sc_relation)
        Relation_list.append(col_relation_attr)

        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources][TvcConstants.term][TvcConstants.relation_key] = Relation_list

        comm_name_filter = {}
        comm_name_filter[TvcConstants.field_key] = {}
        comm_name_filter[TvcConstants.field_key][TvcConstants.name] = TvcConstants.community_name_value
        comm_name_filter[TvcConstants.field_key][TvcConstants.operator_key] = TvcConstants.equals_operator
        comm_name_filter[TvcConstants.field_key][TvcConstants.value_key] = asset_input['community_name']
        domain_name_filter = {}
        domain_name_filter[TvcConstants.field_key] = {}
        domain_name_filter[TvcConstants.field_key][TvcConstants.name] = TvcConstants.domain_name_value
        domain_name_filter[TvcConstants.field_key][TvcConstants.operator_key] = TvcConstants.equals_operator
        domain_name_filter[TvcConstants.field_key][TvcConstants.value_key] = asset_input['domain_name'] 
        tbl_name_filter  = {}
        tbl_name_filter[TvcConstants.field_key] = {}
        tbl_name_filter[TvcConstants.field_key][TvcConstants.name] = TvcConstants.tbl_name
        tbl_name_filter[TvcConstants.field_key][TvcConstants.operator_key] = TvcConstants.equals_operator
        tbl_name_filter[TvcConstants.field_key][TvcConstants.value_key] = table_name 
        full_name_filter  = {}
        full_name_filter[TvcConstants.field_key] = {}
        full_name_filter[TvcConstants.field_key][TvcConstants.name] = TvcConstants.full_name
        full_name_filter[TvcConstants.field_key][TvcConstants.operator_key] = TvcConstants.equals_operator
        full_name_filter[TvcConstants.field_key][TvcConstants.value_key] = col_name 
        
        filter_list = []
        filter_list.append(comm_name_filter)
        filter_list.append(domain_name_filter)
        filter_list.append(full_name_filter)
        
        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources][TvcConstants.term][TvcConstants.filter_key] = {}
        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.resources][TvcConstants.term][TvcConstants.filter_key][TvcConstants.and_operator] = filter_list

        col_sc_list = []
        col_sc = {}
        col_sc[TvcConstants.column_key] = {}
        col_sc[TvcConstants.column_key][TvcConstants.field_name_key] = TvcConstants.col_security_classification
        col_sc[TvcConstants.column_key][TvcConstants.label_key] = TvcConstants.security_classification_key
        col_sc_list.append(col_sc)

        col_sc_column  = {}
        col_sc_column[TvcConstants.group_key]  = {}
        col_sc_column[TvcConstants.group_key][TvcConstants.name] = TvcConstants.col_security_classification_list
        col_sc_column[TvcConstants.group_key][TvcConstants.columns_key] = col_sc_list

        final_column_list = []
        final_column_list.append(col_sc_column)

        asset_tvc_json[TvcConstants.table_view_config][TvcConstants.columns_key] = final_column_list
        
        return asset_tvc_json