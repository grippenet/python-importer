import os
from typing import Dict
from ..utils import read_content
import re

def load_template(name:str, vars:Dict):
    path = os.path.dirname(__file__)
    sql = read_content(path + '/templates/{}.sql'.format(name), must_exist=True)

    for m in re.findall('\{\{([^}]*)\}\}', sql):
        var_name = m
        ok = True
        if var_name not in vars:
            print("Unknown var '%s' in template %s" % (var_name, name))
            ok = False
    if not ok:
        raise Exception("Error in template")
    
    for var_name, value in vars.items():
        placeholder = "{{%s}}" % (var_name,)
        sql = sql.replace(placeholder, value)

    return sql
