from pydantic import BaseModel
from typing import List

 

class Tables(BaseModel):
    table_name: str

class DgcOkeraModel(BaseModel):
    database_name: str
    table_list: List[Tables] = None

class DgcOkeraConstants:
    process_code = 'collibra_okera'