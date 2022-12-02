
import psycopg2
from ..config import settings

class Connection:

    def __init__(self) -> None:
        self.conn = None

    def connect(self):
        dsn = settings['dsn']
        self.conn = psycopg2.connect(**dsn)

    def cursor(self):
        if self.conn is None:
            raise Exception("Not connected")
        return self.conn.cursor()

connection = Connection()

class DbError(Exception):
    pass

def get_cursor():
    return connection.cursor()

class DbQuery:

    def fetch(self,query, mode='all'):
        cursor = get_cursor()
        cursor.execute(query)
        r = None
        if mode == 'all':
            r = cursor.fetchall()
        if mode == 'one':
            r = cursor.fetchone()
        cursor.close()
        return r

    def run(self, query):
        return self.execute(query)

    def execute(self, query, params=None, do_commit=True):
        cursor = get_cursor()
        cursor.execute(query, params)
        count = True
        if do_commit:
            conn.commit()
            count = cursor.rowcount
        cursor.close()
        return count

    def commit(self):
       return conn.commit()

    def rollback(self):
       return conn.commit()

class DbFakeQuery(DbQuery):
    
    def execute(self, query, params=None, do_commit=True):
        cursor = get_cursor()
        qq = cursor.mogrify(query, params)
        if params is not None:
            print("[Fake] template:", query, params)
        print("[fake] query: %s" % qq)
        return 0

    def commit(self):
        return True

    def rollback(self):
        return True

class DbBatch:

    def __init__(self, cursor, batch_size: int):
        self.cursor = cursor
        self.chunks = []
        self.batch_size = batch_size
        self.index = 0

    def append(self, query):
        self.chunks.append(query)
        if len(self.chunks) > self.batch_size:
            self.run()

    def run(self):
        q = None
        try:
            for q in self.chunks:
                self.index += 1
                self.cursor.execute(q)
            conn.commit()
        except psycopg2.Error as e:
            d = {}
            for v in ['column_name', 'constraint_name', 'context', 'internal_query', 'datatype_name', 'internal_position','message_detail','message_hint','statement_position']:
                if hasattr(e.diag, v):
                    x = getattr(e.diag, v)
                    if x is not None:
                        d[v] = x 
            raise DbError("Query error for query %d <<%s>> : %s" % (self.index,q, e)) from e
        self.chunks = []

class DbFakeBatch(DbBatch):

    def run(self):
        print("[fake] Run batch of %d queries" % (len(self.chunks), ) )
        self.chunks = []

class DbBatchValues:

    def __init__(self, cursor, batch_size: int, prefix):
        self.cursor = cursor
        self.chunks = []
        self.index = 0
        self.batch_size = batch_size
        self.prefix = prefix
        
    def append(self, query):
        self.chunks.append(query)
        if len(self.chunks) > self.batch_size:
            self.run()

    def run(self):
        try:
            for q in self.chunks:
                self.index += 1
                print(q)
                self.cursor.execute(q)
            conn.commit()
        except psycopg2.Error as e:
            raise DbError("Query error for query %d : e" % (self.index, e)) from e
        self.chunks = []
