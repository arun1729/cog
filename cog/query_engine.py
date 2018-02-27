import json
import operator
from parser import Select

ops = { "+": operator.add, "-": operator.sub}


class ScanFilter:

    def __init__(self, col_names):
        self.col_names = col_names

    def process(self, record):
        value = json.loads(record)
        row = []
        for col in self.col_names:
            row.append(value[col])
        return row


def execute_query(query, database):
    if isinstance(query.command, Select):
        scanner = database.scanner(ScanFilter(query.command.columns))
        return scanner


# def row_filter(rows, iex_filter):
#     filtered_rows = []
#     for row in rows:
#         if iex_filter(row):
#             filtered_rows.append(row)

#select col1, col2 from x where col1 > 1 and col2 = "abc"
#ops["+"](x,y)