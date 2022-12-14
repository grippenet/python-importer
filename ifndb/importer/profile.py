import fnmatch
from typing import Dict, Optional, List
from collections import OrderedDict
from ..common import get_table_name
from ..utils import read_yaml
from .types import CONVERTS
import numpy

from . preprocess import PREPROCESSORS

class ColumnConf:
    """
    Describe how to handle a column
    """
    def __init__(self, name, conf) -> None:
        self.name = name
        self.rename = None
        self.to = None
        if conf is None:
            self.ignore = True
            return
        
        self.ignore = False
        
        if isinstance(conf, str):
            self.to = conf
        else:
            if 'ignore' in conf:
                self.ignore = bool(conf['ignore'])
            
            if 'rename' in conf:
                self.rename = conf['rename']
            if 'to' in conf:
                    self.to = conf['to']
        if self.to is not None and not self.to in CONVERTS:
            raise Exception("Unknown conversion type '%s' for '%s'" % (self.to, name))
class TableConf:
    """
        Describe mapping for a table

        Mapping describe transformation to do on an input column
        The mapping can refer to a full column name (like 'Q10c_1') or a pattern 'Q10c_*'

    """
    def __init__(self, conf, global_conf):
        self.patterns = []
        self.mapping = OrderedDict()
        self.csv_types = None
        self.table = conf['table']
        self.preprocess = []
        
        if 'csv_types' in conf:
            # dtypes can be used to force type during csv loading
            self.csv_types = self.create_dtypes(conf['csv_types'])

        for name, colDef in conf['mapping'].items():
            self.mapping[name] = ColumnConf(name, colDef)
            if '*' in name:
                self.patterns.append(name)
        
        skip_prepare = 'skip_prepare' in global_conf and global_conf['skip_prepare']
        
        if 'prepare' in conf and not skip_prepare:
            for index, p in enumerate(conf['prepare']):
                try:
                    pc = self.create_preprocessor(p, global_conf)
                    self.preprocess.append(pc)
                except Exception as e:
                    raise Exception("Error in prepare %d : %s" % (index, e)) from e

    def get_mapping(self, name):
        if name in self.mapping:
            return self.mapping[name]
        if len(self.patterns) > 0:
            for pattern in self.patterns:
                if fnmatch.fnmatch(name, pattern):
                    return self.mapping[pattern]
        return None

    def create_dtypes(self, conf:Dict):
        o = {}

        def dtype_from_name(name):
            if value == 'int':
                return numpy.int32
            if value == 'int8':
                return numpy.int8
            if value == "int64":
                return numpy.int64
            if value == "str":
                return str
            if value == "bool":
                return numpy.bool8
            return value

        for name, value in conf.items():
            v = dtype_from_name(value)
            o[name] = v
        return o



    def create_preprocessor(self, conf, global_conf):
        if not isinstance(conf, dict):
            raise("Preproressor entry must be a dict")
        for name, params in conf.items():
            if not name in PREPROCESSORS:
                raise Exception("Unknown preprocessor '%s'" % name)
            klass = PREPROCESSORS[name]
            return klass(params, global_conf)

    def get_table_name(self)->str:
        return get_table_name(self.table)
        

class Profile:
    def __init__(self, data:Dict, extra_globals: Dict):
        if '_config' in data:
            self.create_globals(data['_config'], extra_globals)
            del data['_config']
        else:
            self.create_globals({}, extra_globals)
        
        self.tables: Dict[str, TableConf] = {}
        for name, tb in data.items():
            self.tables[name] = TableConf(tb, self.global_conf)

    @staticmethod
    def from_yaml(file: str, extra_globals: Dict):
        r = read_yaml(file, must_exist=True)
        return Profile(r, extra_globals)
    

    def create_globals(self, conf, extra:Dict):
        if 'key_separator' in conf:
            if not isinstance(conf['key_separator'], str):
                raise Exception("key_separator should be a string")
        self.global_conf = conf
        for name, value in extra.items():
            self.global_conf[name] = value
        
    def get_table(self, table: str)-> Optional[TableConf]:
        return self.tables[table]
