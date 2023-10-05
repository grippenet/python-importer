from .profile import TableRef
from typing import List
from ..db import connection, DbQuery,quote_id

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
    def __init__(self, target_table:TableRef, source_table:TableRef, columns: List[OutputColumn]):
        self.target_table:TableRef = target_table # INSERT
        self.source_table:TableRef = source_table
        self.columns = columns # Target_colum and expression to populate it from the source column

    def select(self):
        print(self.source_table)
        exprs = [o.expr for o in self.columns]
        return "select %s from %s" % (",".join(exprs), str(self.source_table))

    def query(self):
        cols = [quote_id(o.target) for o in self.columns]
        return "insert into %s (%s) %s" % (str(self.target_table), ",".join(cols),  self.select())

    def source_range(self):
        query = 'select min("timestamp") , max("timestamp") from %s' % str(self.source_table)
        q = DbQuery()
        return q.fetch(query, 'one')
