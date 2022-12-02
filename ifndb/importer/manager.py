
from typing import Dict, Optional, List

import os
import time
import pandas
import base64
from pathlib import Path

from ..db import get_cursor, connection
from ..config import settings
from pandas import isna
from ..db.query import DbBatch, DbFakeBatch, DbFakeQuery, DbQuery
from ..db.struct import ColumnDef, TableStruct, get_table_struct
from ..db.types import normalize_db_type
from ..db.utils import quote_id
from ..utils import int_to_base36, read_content, read_yaml

from .profile import Profile
from .export import ExportColumn, ExportConstant
from .types import TYPE_COMPAT, CONVERTS

class ImportError(Exception):
    pass

class Importer:

    def __init__(self, path:Path, opts:Optional[Dict]):
        """
        
        arguments:

        path: the path where the files are stored
        opts: Dictionnary
            - dry_run : only shows queries do not really run them on database
            - show_batch : will shown N batches query
            - debug = debug mode will be very verbose
        """
        
        self.dry_run = 'dry_run' in opts and opts['dry_run']
        self.show_batch_count = 0
        self.show_batch_row   = 0
        if 'show_batch' in opts:
            self.show_batch_count = opts['show_batch']    

        if 'show_batch_row' in opts:
            self.show_batch_row = opts['show_batch_row']

        self.debug = 'debug' in opts and opts['debug']

        self.path = path
        
        if not os.path.exists(self.path):
            raise ImportError("Input path doesnt exists '%s'" % (self.path))
        
        self.profile = None
        
        self.has_error = False
    
    def error(self, message):
        self.has_error = True
        print("[error] %s" % ( message))
        
    def load_profile(self, file):
        r = read_yaml(file, must_exist=True)
        self.profile = Profile(r, {'path': self.path, 'debug': self.debug})
    
    def import_table(self, name, csv_file):
        """
            Import a from a the csv file to the database
            ---
            params:
            dry_run -- if True dont do modification just show what will be done
        
        """
        connection.connect()
        rows = pandas.read_csv(csv_file)

        tb_conf = self.profile.get_table(name)
        if tb_conf is None:
            raise Exception("Unknow table profile '%s'" % (name))

        rows.info(verbose=True)
        if len(tb_conf.preprocess) > 0:
            for index, processor in enumerate(tb_conf.preprocess):
                try:
                    print(processor)
                    processor.apply(rows)
                except Exception as e:
                    raise ImportError("Error running preprocessor %d" % index ) from e
        if self.debug:
            rows.info(verbose=True)
        
        table = tb_conf.get_table_name()
        target = get_table_struct(table)
        
        if str(rows.columns.values[0]) == "Unnamed: 0":
            rows = rows.rename(columns={'Unnamed: 0':"_rowid"})

        auto_ignore = ['engineVersion','language', '_rowid']
        auto_add = ['global_id','timestamp']
        columns = rows.columns
        
        #rows.infer_objects()

        # Export schema
        export = [] # Columns to import
        
        for column in columns:
            if column in auto_ignore:
                continue

            # Default strategy
            name = column
            target_name = name
            convert_to  = None

            colDef = tb_conf.get_mapping(column)
            if not colDef is None:
            
                if colDef.ignore:
                    continue
                
                if not colDef.rename is None:
                    target_name = colDef.rename
                
                convert_to = colDef.to
            
            if convert_to is None:
                if name in target:
                    convert_to = self.auto_convert_type(target[name])

            if not convert_to is None:
                if self.debug:
                    print("Convert %s to %s" % (column, convert_to))
                rows[column] = self.convert_series(rows[column], convert_to)
            export.append(ExportColumn(name, target_name, convert_to))
        
        if self.dry_run or self.debug:
            rows.info(verbose=True)
        
        self.check_columns(target, rows, export)
        if self.has_error:
            print("Some errors occured. Unable to make import")
            return
        self.import_data(target, rows, export)

    def convert_series(self, data: pandas.Series, to:str):
        if to == 'int':
           return pandas.to_numeric(data, downcast='integer', errors='coerce')
        if to == 'timestamp':
            return pandas.to_datetime(data, unit='s')
        if to == 'date':
            return pandas.to_datetime(data, unit="s")
        if to == 'bool':
            return data.astype(bool)
        if to == 'str':
            return data.astype(str)
        if to == 'month-year':
            d = pandas.to_datetime(data, unit='s')
            return d.dt.strftime('%Y-%m')
        return data

    def check_columns(self, target:TableStruct, rows: pandas.DataFrame, exports: List[ExportColumn]):
        to_export = []
        for export in exports:

            if isinstance(export, ExportConstant):
                to_export.append(export.name)
                continue

            target_name = export.target
            data_name = export.name
            
            if target_name in to_export:
                self.error("Column '%s' exporting %s is already registered as a target column name for another column" % (target_name, data_name))
                continue

            if not target_name in target:
                self.error("column '%s' not in target table %s" % (target_name, target.qualified_table()))
                continue
            colDef = target[target_name]
            dtype = rows[data_name].dtype
            dbtype = colDef.dbtype
            ntype = normalize_db_type(dbtype)
            compat = TYPE_COMPAT[ntype]
            if compat is None:
                self.error("Unknown db type '%s' for column '%s'" % (dbtype, data_name))  
            if not str(dtype) in compat:
                self.error("Column '%s' : type %s (%s) is not registred as compatible with '%s'" % (data_name, dbtype, ntype, dtype)) 
        
    def auto_convert_type(self, colDef: ColumnDef):
        dbtype = normalize_db_type(colDef.get_type())
        if dbtype == 'int':
            return 'int'
        if dbtype == 'timestamp with time zone':
            return 'timestamp'
        if dbtype == 'date':
            return 'date'
        if dbtype == 'bool':
            return 'bool'
        if dbtype == 'text' or dbtype == 'varying':
            return 'str'
        return None
    
    def import_data(self, target:TableStruct, rows: pandas.DataFrame, columns: List[ExportColumn]):
        plan = 'import_' + int_to_base36(int(time.time()))

        if self.dry_run:
            # Use a logger runner 
            db = DbFakeQuery() 
        else:
            db = DbQuery()
       
        temp_table = target.qualified_table() + '_import'

        print("Creating %s " % temp_table)

        db.execute("DROP TABLE IF EXISTS %s" % temp_table)
        db.execute("CREATE TABLE %s (like %s)" % (temp_table, target.qualified_table()))
        
        vars = []
        cols = []
        params = [] # List of parameters to add
        p = '%' + 's'
        for idx, col in enumerate(columns):
            cols.append(quote_id(col.target))
            vars.append("$%d" % ( idx+1, ))
            params.append(p) # List of parameters will be used to create the query
        
        query = "PREPARE %s AS INSERT INTO %s(%s) VALUES (%s)" % (plan, temp_table , ",".join(cols), ",".join(vars))
        
        db.execute(query)

        execute_query = "EXECUTE %s (%s)" % (plan, ",".join(params))
       
        batch_size = 2000
        cursor = get_cursor()
        
        # Need to show rows values
        shown = 0  # Count shown in case of show_batch_count
        need_show = self.show_batch_row > 0 or self.show_batch_count > 0

        if self.dry_run:
            batch = DbFakeBatch(cursor, batch_size)
        else:
            batch = DbBatch(cursor, batch_size)
        for idx, row in rows.iterrows():
            vv = []
            for column in columns:
                if column.is_constant():
                    vv.append(column.value)
                    continue
                value = row[column.name]
                if isna(value):
                    value = None
                else:
                    if(column.type == "int"):
                        value = int(value)
                vv.append(value)
            qq = cursor.mogrify(execute_query, vv)
            batch.append(qq)

            # Show given rows (for debug purpose)
            if need_show:
                to_show = False
                if self.show_batch_count > 0 and shown < self.show_batch_count:
                    to_show = True
                    shown += 1
                if self.show_batch_row > 0 and row['_rowid'] == self.show_batch_row:
                    to_show = True
                if to_show:
                    print(row.to_dict())
                    print(qq)
            
        batch.run()
        cursor.close()

        min_time = rows['timestamp'].min().to_pydatetime()
        max_time = rows['timestamp'].max().to_pydatetime()

        target_table = target.qualified_table()
        db.execute("delete from %s where timestamp >= %%s and timestamp <= %%s" % target_table, (min_time, max_time))
        db.execute("insert into %s select * from %s" % (target_table, temp_table))
            