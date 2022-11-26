
import yaml
import os
import sys

class Output:
    """
        Simple output class
        If no path provided the print contents
    """
    def __init__(self, path=None):
        self.path = path
    
    def write(self, data):
        need_close = False
        if not self.path is None :
            output = open(self.path, 'w')
            need_close = True
        else:
            output = sys.stdout

        output.write(data)
        
        if need_close:
            output.close()

def read_content(path, must_exist=False, default=None):
    found = os.path.exists(path)
    if not found:
        if must_exist:
            raise IOError("File %s doesnt exist" % path)
        return default
    with open(path, 'r', encoding='UTF-8') as f:
        content = f.read()
        f.close()
    return content

def write_content(path, content):
    with open(path, 'w') as f:
        f.write(content)
        f.close()

def read_yaml(path, must_exist=False):
    obj = yaml.load(read_content(path, must_exist=must_exist))
    return obj

def write_yaml(path, data):
    if isinstance(path, Output):
       content = yaml.dump(data, indent=4, default_flow_style=False)
       path.write(content)
    else: 
        with open(path, 'w', encoding="UTF-8") as f:
            yaml.dump(data, f, indent=4, default_flow_style=False)
            f.close()

def int_to_base36(i):
    """
    Converts an integer to a base36 string
    """
    chars = '0123456789abcdefghijklmnopqrstuvwxyz'
    if i < 0:
        raise ValueError("Negative base36 conversion input.")
    if i < 36:
        return chars[i]
    b36 = ''
    while i != 0:
        i, n = divmod(i, 36)
        b36 = chars[n] + b36
    return b36