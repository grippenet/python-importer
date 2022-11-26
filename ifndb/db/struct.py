"""

Table structure utilities

"""
from .query import get_cursor
from typing import List, Optional, Dict

class UnknownTableException(Exception):
    pass

class ColumnDef:
    """
        Colummn Definition
    """

    def __init__(self, name, dbtype, nullable ):
        self.name = name
        self.dbtype = dbtype
        self.nullable = nullable

    def __getitem__(self, key):
        if key == "name":
            return self.name
        if key == "type":
            return self.dbtype
        if key == "null":
            return self.nullable

    def is_nullable(self):
        return self.nullable

    def get_type(self):
        return self.dbtype

    def __str__(self):
        if self.nullable:
            n = '?'
        else:
            n = ''
        return "%s:%s%s" % (self.name, n, self.dbtype)

    def __repr__(self):
        return self.__str__()    

class TableStruct(object):
    """
        Describes table structure in db
    """
    def __init__(self, table_name, schema_name, columns, defs: Dict[str, ColumnDef]):
        self.table_name = table_name
        self.schema_name = schema_name
        self.columns = columns
        self.defs = defs

    def __str__(self):
        return '<Table(' + ','.join(self.columns) + ')>'

    def __contains__(self, column):
        """
            Check if table contains a columns
        """
        return column in self.defs

    def __getitem__(self, key)->ColumnDef:
        return self.defs[key]

    def qualified_table(self):
        return "%s.%s" % (self.schema_name, self.table_name)

    def column_definitions(self)->Dict[str, ColumnDef]:
        return self.defs

class TableDataInfo:
    """
        Information about table data
    """

    def __init__(self, table: TableStruct):
        db = DbQuery()
        query = "select count(*), min(timestamp), max(timestamp) from %s" % (table.qualified_table(), )
        r = db.fetch(query, 'one')
        self.rows = r[0]
        self.min_time = r[1]
        self.max_time = r[2]

class TableStructDiff:
    """
        Compare two TableStruct
    """
    def __init__(self, left: TableStruct, right:TableStruct):
        self.common = []
        self.only_left = []
        self.only_right = []
        
        remains = right.columns
        for col in left.columns:
            if col in right.columns:
                self.common.append(col)
                remains.remove(col)
            else:
                self.only_left.append(col)
        self.only_right = remains

    def is_common(self, col):
        return col in self.commmon

    def is_only_left(self, col):
        return col in self.only_left

    def is_only_right(self, col):
        return col in self.only_right

def get_table_struct(table, schema="public", by_ordinal=True):
    query = "select column_name, data_type, is_nullable from information_schema.columns where table_name='%s' and table_schema='%s'" % (table, schema)
    if by_ordinal:
        query += " order by ordinal_position"
    cursor = get_cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    if len(rows) == 0:
        raise UnknownTableException("Unable to find table %s.%s" % (schema, table))
    d = {}
    columns = []
    for row in rows:
        name = row[0]
        d[name] = ColumnDef(name, row[1], row[2])
        columns.append(name)
    return TableStruct(table, schema, columns, d)    
