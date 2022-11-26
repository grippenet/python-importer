# Describe a column to be exported
class ExportColumn:

    def __init__(self, name, target, type):
        self.name = name
        self.target = target
        self.type = type
    
    def is_column(self)->bool:
        return True

    def is_constant(self)->bool:
        return False


class ExportConstant(ExportColumn):
    
    def __init__(self, name, value):
        super(ExportConstant, self).__init__(name, name, None)
        self.value = value

    def is_column(self)->bool:
        return False

    def is_constant(self)->bool:
        return True
