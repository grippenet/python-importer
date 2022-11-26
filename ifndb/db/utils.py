
def quote_id(s):
    """
    Quote identifier
    """
    return '"' + s +'"'

def quote_val(s):
    """
        Quote literal value
    """
    return "'%s'" % s