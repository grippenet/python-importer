##
# Profile defines configuration loaded from a config file
# A profile define for each exported table the mapping between the source and the target table
##
from typing import Dict, Optional, List, Tuple
from collections import OrderedDict
from ..common import get_table_name
from ..utils import read_yaml

class TableRef:
    def __init__(self, table_name, schema):
        self.name = table_name
        self.schema = schema

    def __str__(self):
        return "%s.%s" % (self.schema, self.name)

def get_export_table(name):
    return TableRef("pollster_results_%s" % name, "epidb_fr")

class ColumnConf:
    """
    Describe how to handle a column
    By default 
    """
    def __init__(self, target, conf) -> None:
        self.target = target
        self.rename = None
        self.source = None
        if conf is None:
            self.null = True
            return
        self.null = False
        if isinstance(conf, bool) and conf:
            self.source = target
        self.source = conf
       
class TableConf:
    """
        Describe mapping for a table

        Mapping describe transformation to do on an input column
        The mapping can refer to a full column name (like 'Q10c_1') or a pattern 'Q10c_*'

    """
    def __init__(self, name, conf, global_conf):
        self.name = name
        self.source_table = conf['source']
        self.mapping: List[ColumnConf] = []
        for name, colDef in conf['mapping'].items():
            self.mapping.append(ColumnConf(name, colDef))

    def get_mapping(self)->List[ColumnConf]:
        return self.mapping
    
    def get_source_table(self)->TableRef:
        return TableRef(self.source_table, "public")

    def get_target_table(self)->TableRef:
        return get_export_table(self.name)

class ExportProfile:

    def __init__(self, data:Dict, extra_globals: Dict):
        # if '_config' in data:
        #    self.create_globals(data['_config'], extra_globals)
        #    del data['_config']
        #else:
        #    self.create_globals({}, extra_globals)
        self.global_conf = {}
        self.tables: Dict[str, TableConf] = {}
        for name, tb in data.items():
            self.tables[name] = TableConf(name, tb, self.global_conf)

    @staticmethod
    def from_yaml(file: str, extra_globals: Dict):
        r = read_yaml(file, must_exist=True)
        return ExportProfile(r, extra_globals)
 
    def get_table(self, table: str)-> Optional[TableConf]:
        return self.tables[table]
