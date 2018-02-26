import json
import operator
ops = { "+": operator.add, "-": operator.sub}

def column_filter(json_str, col_names):
    value = json.loads(json_str)
    row = []
    for col in col_names:
        row.append(value[col])
    return row

def row_filter(rows, iex_filter):
    filtered_rows = []
    for row in rows:
        if iex_filter(row):
            filtered_rows.append(row)


#select col1, col2 from x where col1 > 1 and col2 = "abc"
#ops["+"](x,y)