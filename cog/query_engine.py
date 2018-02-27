import json
import operator
import itertools
from parser import Select


ops = { "+": operator.add, "-": operator.sub}


class ScanFilter:

    def __init__(self, col_names):
        self.col_names = col_names

    def get_column_names(self):
        yield self.col_names

    def process(self, record):
        value = json.loads(record)
        row = []

        for col in self.col_names:
            if col == '*':
                for key in value:
                    row.append(value[key])
            else:
                row.append(value[col])
        return row


def execute_query(query, database):
    if isinstance(query.command, Select):
        scan_filter = ScanFilter(query.command.columns)
        scanner = database.scanner(scan_filter)
        return itertools.chain(scan_filter.get_column_names(), scanner)


# def row_filter(rows, iex_filter):
#     filtered_rows = []
#     for row in rows:
#         if iex_filter(row):
#             filtered_rows.append(row)

#select col1, col2 from x where col1 > 1 and col2 = "abc"
#ops["+"](x,y)