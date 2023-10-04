from .profile import ExportProfile, TableConf
from typing import Dict, Optional, List
from ..db import get_table_struct

class OutputColumn:
    """
        Describe a target column populated by an expression
    """
    def __init__(self, target:str, expr:str):
        self.target = target # Target column name
        self.expr = expr # Expression used to populate the column

class UpdateQuery:
    """
        All info to build the update query
    """
    def __init__(self, target_table, source_table, columns: List[OutputColumn]):
        self.target_table = target_table # INSERT
        self.source_table = source_table
        self.columns = columns # Target_colum and expression to populate it from the source column

    def select(self):
        exprs = [o.expr for o in self.columns]
        return "select %s from %s" % (",".join(exprs), self.source_table)

    def query(self):
        cols = [o.target for o in self.columns]
        return "insert into %s (%s) %s" % (self.target_table, ",".join(cols),  self.select())        

class ExportResult:
    """
        Build result
    """
    def __init__(self, update: Optional[UpdateQuery], errors: List[str]):
        self.update = update
        self.errors = errors


class ExporterManager:

    def __init__(self, profile: ExportProfile):
        self.profile = profile

    def build_table(self, conf: TableConf)-> ExportResult:
        src_table = conf.get_source_table()
        target_table = conf.get_target_table()
        #src_struct = get_table_struct(src_table.name, src_table.schema)
        target_struct = get_table_struct(target_table.name, target_table.schema)

        exports = []
        errors = []
        for mapping in conf.get_mapping():
            target_column = mapping
            if target_column not  in target_struct:
                errors.append("Column '%d' doenst exists in target %s" % (target_column, str(target_table)))
                continue
            exports.append(OutputColumn(mapping.target, mapping.source))
        
        exported = [o.target for o in exports]
        for column, column_def in target_struct.column_definitions.items():
            if column in exported:
                continue
            if not column_def.is_nullable():
                errors.append("Column %s not provided but not nullable" % column)
        
        if len(errors) > 0:
            query = None
        else:
            query = UpdateQuery(target_table, src_table, exports)
        return ExportResult(query, errors)

    def build(self)->Dict[str, ExportResult]:
        rr = {}
        for name, conf in self.profile.tables.items():
            rr[name] = self.build_table(conf)
        return rr

