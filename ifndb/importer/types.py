CONVERTS = ['int','bool','date','timestamp','str','month-year']

# Database type
TYPE_COMPAT = {
    'int':['int','int32','int8','int64', 'float64','int16'],
    'text': ['str', 'object'],
    'varying': ['str', 'object'],
    'bool': ['bool', 'boolean'],
    'date': ['date', 'datetime64[ns]'],
    'timestamp with time zone': ['datetime64[ns]']
}
