from .profile import ExportProfile, TableConf, TableRef
from typing import Dict, Optional, List
from ..db import get_table_struct, connection, DbQuery,quote_id
from .update import UpdateQuery, OutputColumn
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
        src_struct = get_table_struct(src_table.name, src_table.schema)
        target_struct = get_table_struct(target_table.name, target_table.schema)

        exports = []
        errors = []
        for mapping in conf.get_mapping():
            target_column = mapping.target
            if target_column not  in target_struct:
                errors.append("Column '%s' doenst exists in target %s (%s)" % (target_column, str(target_table), conf.name))
                continue
            expr = mapping.source
            if expr is None or expr == '':
                errors.append("Source expression for '%s' cannot be null for %s (%s)" % (target_column, str(target_table), conf.name))
            
            exports.append(OutputColumn(mapping.target, expr))
        
        exported = [o.target for o in exports]
        for column, column_def in target_struct.column_definitions().items():
            if column in exported:
                continue
            if not column_def.is_nullable():
                errors.append("Column %s not provided but not nullable" % column)
        
        if len(errors) > 0:
            query = None
        else:
            query = UpdateQuery(target_table, src_table, exports)
        return ExportResult(query, errors)

    def build(self, survey_name=None)->Dict[str, ExportResult]:
        connection.connect()
        rr = {}
        for name, conf in self.profile.tables.items():
            if survey_name is not None and name != survey_name:
                continue
            rr[name] = self.build_table(conf)
        return rr

