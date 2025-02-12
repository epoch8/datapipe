class Pipeline:
    def table(self, table_type):
        return Table(table_type)

    def generate(self, outputs, func):
        pass

    def transform(self, inputs, outputs, func):
        pass

class Table:
    def __init__(self, table_type):
        self.table_type = table_type
