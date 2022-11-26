
KNOWN_DB_TYPES = ['integer', 'int', 'boolean','bool', 'text', 'smallint', 'varying', 'date', 'timestamp']

DB_TYPES_ALIASES = {
    'integer':'int',
    'boolean': 'bool',
    'character varying': 'varying',
}

def normalize_db_type(name:str):
    type_name = name.lower()
    if type_name in DB_TYPES_ALIASES:
        return DB_TYPES_ALIASES[type_name]
    return type_name
