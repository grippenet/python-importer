##
# Profile defines configuration loaded from a config file
# A profile define for each exported table the mapping between the source and the target table
##
from typing import Dict, Optional, List, Tuple
from collections import OrderedDict
from ..common import get_table_name
from ..db import quote_id
from ..utils import read_yaml

class TableRef:
    def __init__(self, table_name:str, schema:str):
        if '.' in table_name:
            schema, name = table_name.split('.')
        else:
            name = table_name
        self.name = name
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
        self.source = None
        if conf is None:
            self.source = quote_id(self.target)
        else:
            self.source = conf
    
    def __repr__(self) -> str:
        return "%s = %s" % (self.target, self.source)
            
class ConfigError(Exception):
    pass

class TableConf:
    """
        Describe mapping for a table

        Mapping describe transformation to do on an input column
        The mapping can refer to a full column name (like 'Q10c_1') or a pattern 'Q10c_*'

    """
    def __init__(self, name, conf, global_conf):
        self.name = name
        if 'source' not in conf:
            raise ConfigError("Missing source in %s profile" % name)
        self.source_table = conf['source']
        if self.source_table is None or self.source_table == '':
            raise ConfigError("Missing source in %s profile" % name)
        self.mapping: List[ColumnConf] = []
        if 'mapping' not in conf:
            raise ConfigError("Missing mapping in %s profile" % name)
        if not isinstance(conf['mapping'], dict):
            raise ConfigError("mapping must be a dictionnary in %s profile" % name)
        for name, colDef in conf['mapping'].items():
            self.mapping.append(ColumnConf(name, colDef))

    def get_mapping(self)->List[ColumnConf]:
        return self.mapping
    
    def get_source_table(self)->TableRef:
        return TableRef(self.source_table, "public")

    def get_target_table(self)->TableRef:
        return get_export_table(self.name)
    
    def __repr__(self) -> str:
        return "Export(%s)[ %s -> %s]:\n - %s" % (self.name, self.get_source_table(), self.get_target_table(), "\n - ".join(map(repr, self.mapping)))

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

    def __repr__(self) -> str:
        return "\n\n".join(map(repr, self.tables.values()))