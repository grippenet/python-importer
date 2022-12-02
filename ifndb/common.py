
TABLES = ['weekly', 'intake', 'vaccination']


def get_table_name(name):
    return "pollster_results_%s" % name