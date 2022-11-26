import fnmatch
from typing import Dict, Optional, List
from collections import OrderedDict

from .types import CONVERTS

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
            raise Exception("Unknown convesion type '%s' for '%s'" % (self.to, name))

class TableConf:
    """
        Describe mapping for a table

        Mapping describe transformation to do on an input column
        The mapping can refer to a full column name (like 'Q10c_1') or a pattern 'Q10c_*'

    """
    def __init__(self, conf):
        self.patterns = []
        self.mapping = OrderedDict()
        for name, colDef in conf['mapping'].items():
            self.mapping[name] = ColumnConf(name, colDef)
            if '*' in name:
                self.patterns.append(name)

    def get_mapping(self, name):
        if name in self.mapping:
            return self.mapping[name]
        if len(self.patterns) > 0:
            for pattern in self.patterns:
                if fnmatch.fnmatch(name, pattern):
                    return self.mapping[pattern]
        return None


class Profile:
    def __init__(self, data) -> None:
        
        self.tables = {}
        for name, tb in data['tables'].items():
            self.tables[name] = TableConf(tb)

    def get_table(self, table: str)-> Optional[TableConf]:
        return self.tables[table]
