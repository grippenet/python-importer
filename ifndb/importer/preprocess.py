import pandas
import re
import json
from pandas import isna
import base64
from .columns import ColumnSelector

def encode_global_id(v):
    """
        global_id is provided as an 48bytes hexadecimal encoded string
        To keep compat with old (<36 chars) we reencode it ot base64 urlsafe

    """
    b = base64.b16decode(v, casefold=True)
    return base64.urlsafe_b64encode(b).decode('utf-8')


class BasePreprocessor:

    def apply(self, rows: pandas.DataFrame):
        pass

class RenamePreprocessor(BasePreprocessor):

    def __init__(self, conf, global_conf) -> None:
        if not isinstance(conf, dict):
            raise Exception("expected dict of rules 'pattern': 'replacemenet'")
        self.rules = conf
        
    def apply(self, rows: pandas.DataFrame):
        for pattern, target in self.rules.items():
            renamer = lambda n: re.sub(pattern, target, n)
            rows.rename(columns=renamer, inplace=True)

    def __str__(self) -> str:
        return '<rename>'

class McgPreprocessor(BasePreprocessor):
    """
        Rename columns with questionId + 'sep' + key   
    
    """
    def __init__(self, conf, global_conf):
        
        if not 'key_separator' in global_conf:
            raise Exception("key_separator must be provided in _config entry (global config)")
        
        self.separator = global_conf['key_separator']

        if not isinstance(conf, list):
            raise Exception("List of keys expected for mcg preprocessor")

        self.keys = conf

    def apply(self, rows: pandas.DataFrame):
        for key in self.keys:
            prefix = key + self.separator
            def renamer(column:str):
                if column is not None and column.startswith(prefix):
                    return column.replace(self.separator, '_')
                return column
            rows.rename(columns=renamer, inplace=True)

    def __str__(self):
        return "<mcg(%s):%s>" % (self.separator, ','.join(self.keys))

def extract_items_keys(data):
    d = []
    if 'items' in data:
        for item in data['items']:
            d.append(item['key'])
    return d

class UnJsonPreprocessor(BasePreprocessor):
    def __init__(self, conf, global_conf):
        if not 'columns' in conf:
            raise Exception("expected 'columns' entry")
        self.columns_selector = ColumnSelector(conf['columns'])
        self.parser = None
        parser = conf['parser']
        self.parser_name = parser
        if parser == 'items_keys':
            self.parser = extract_items_keys
        if self.parser is None:
            raise Exception("Unknown parser '%s'" % parser)

    def apply(self, rows: pandas.DataFrame):

        data_columns = list(rows.columns)

        columns = self.columns_selector.select(data_columns)

        def update_row(value):
            if isna(value):
                return value
            v = json.loads(value)
            v = self.parser(v)
            if len(v) > 0:
                v = ','.join(v)
            return v
        
        for column in columns:
            if not column in rows.columns:
                continue
            rows[column] = rows[column].apply(update_row)

    def __str__(self):
        return "<unjson(%s):%s>" % (self.parser_name, self.columns_selector)

class TimeElapsedProcessor(BasePreprocessor):
    """
        Compute Time Elapsed column
    """
    def __init__(self, conf, global_conf):
        pass

    def apply(self, rows: pandas.DataFrame):
        rows['timeelapsed'] = rows['submitted'] - rows['opened']

    def __str__(self):
        return "<timeelapsed>"

class ToBooleanProcessor(BasePreprocessor):
    """
        Transform colum to boolean
    """
    def __init__(self, conf, global_conf):
        if not 'columns' in conf:
            raise Exception("expected 'columns' entry")
        self.columns_selector = ColumnSelector(conf['columns'])
        self.na_false = False
        if 'na_false' in conf:
            self.na_false = bool(conf['na_false'])
       
   
    def apply(self, rows: pandas.DataFrame):
        data_columns = list(rows.columns)
        columns = self.columns_selector.select(data_columns)
        booleans = {'0': False, '1':True, 'true': True, 'false': False}
        if self.na_false:
            to_bool = lambda x:str(x).lower() if not pandas.isna(x) else '0'
        else:
            to_bool = lambda x:str(x).lower() if not pandas.isna(x) else None
        for column in columns:
            rows[column] = rows[column].map(to_bool).replace(booleans)

    def __str__(self):
        return "<boolean:%s>" % (self.columns_selector)

class IndicatorProcessor(BasePreprocessor):
    """
       Create a boolean variable (indicator) if another has non empty data
       
       parameters : dictionary with a set of rules (like rename) key = column where data are, value = pattern to create indicator columns

    """
    def __init__(self, conf, global_conf):
        if not isinstance(conf, dict):
            raise Exception("parameters must be a dictionary")
        self.rules = conf

    def apply(self, rows: pandas.DataFrame):
       data_columns = list(rows.columns)

       for source_pattern, target_pattern in self.rules.items():
            for column in data_columns:
                if re.match(source_pattern, column) is not None:
                    target_column = re.sub(source_pattern, target_pattern, column)
                    if target_column in data_columns:
                        raise Exception("Column '%s' already in the data, cannot use it for indicator (from %s)" % (target_column, column))
                    rows[target_column] = rows[column].map(lambda x: True if not isna(x) else False)

    def __str__(self):
        return "<indicator:%s>" % (str(self.rules))

class MigrationProcessor(BasePreprocessor):
    """
        Replace
    """
    def __init__(self, conf, global_conf):
        self.load(global_conf)
        self.encode = 'encode' in conf and conf['encode']
    
    def load(self, global_conf):
        if not 'migrations' in global_conf:
            raise Exception("migrations is not available in global config")
        conf = global_conf['migrations']
        data = None
        if 'file' in conf:
            path = global_conf['path']
            f = path + '/' + conf['file']
            data = json.load(open(f, 'r'))
        if data is None:
            raise Exception("Unable to load migrations")
        self.migrations = data
    
    def apply(self, rows: pandas.DataFrame):
        def migrate_id(value):
            if value in self.migrations:
                return self.migrations[value]
            if self.encode:
                return str(encode_global_id(value))
            return value
                
        rows['global_id'] = rows['global_id'].apply(migrate_id)

    def __str__(self):
        return "<migrations>"

PREPROCESSORS = {
    'rename': RenamePreprocessor,
    'unjson': UnJsonPreprocessor,
    'mcg': McgPreprocessor,
    'timeelapsed': TimeElapsedProcessor,
    'migration': MigrationProcessor,
    'boolean': ToBooleanProcessor,
    'indicator':IndicatorProcessor
}
