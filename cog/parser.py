import re

# SQL Grammar reference: https://forcedotcom.github.io/phoenix/

SELECT_COMMAND_LIST = ["SELECT", "FROM", "WHERE", "LIMIT"]

create_table_pattern = re.compile('CREATE\s+TABLE\s+(?P<tablename>(.*))', flags=re.IGNORECASE)
add_index_pattern = re.compile('ADD\s+INDEX\s+(?P<tablename>(.*))\s+KEYS(?P<keys>(.*))', flags=re.IGNORECASE)


class Query:
    def __init__(self, select, sub_command=None):
        self.command = select
        self.sub_command = sub_command


class Select:
    def __init__(self, columns, table_name, conditions=None, limit=None):
        self.columns = columns
        self.table_name = table_name
        self.conditions = conditions
        self.limit = limit


class Condition:
    def __init__(self, condition, prefix_op=None):
        self.operation = condition
        self.prefix_op = prefix_op


"""

Create table is 'IF NOT EXISTS' by default. Columns are not necessary.

"""


class Create:
    def __init__(self, tablename):
        self.tablename = tablename


class AddIndex:
    def __init__(self, tablename, keys):
        self.tablename = tablename
        self.keys = keys


def get_query_list(statement):
    query_list = statement.split(";")
    # empty string
    if len(query_list) == 1 and query_list[0] == '':
        return None

    if len(query_list) > 1 and query_list[-1] == '':
        query_list = query_list[0:-1]

    return map(str.strip, query_list)


def process_select_statement(select_statement):
    select_tokens = re.split(SELECT_COMMAND_LIST[0], select_statement, flags=re.IGNORECASE)
    assert select_tokens[0] is '', "Syntax error: a query must start with SELECT."
    assert len(select_tokens) == 2, "Syntax error: invalid SELECT statement."

    from_tokens = re.split(SELECT_COMMAND_LIST[1], select_tokens[1], flags=re.IGNORECASE)
    assert len(from_tokens) == 2, "Syntax error: invalid select statement at FROM command."

    columns_str = from_tokens[0]
    columns = []
    for c in columns_str.split(","):
        columns.append(c.strip())

    where_tokens = re.split(SELECT_COMMAND_LIST[2], from_tokens[1], flags=re.IGNORECASE)

    table_name = where_tokens[0].strip()
    assert len(table_name) > 0, "Syntax error: Table name cannot be empty."

    conditions = []
    operators = [None] # No prefix op if only one condition exists
    limit = None
    limit_token = []
    if len(where_tokens) > 1:
        limit_token = re.split(SELECT_COMMAND_LIST[3], where_tokens[1], flags=re.IGNORECASE)
        where_conditions_str = limit_token[0]
        l = 0
        for w in re.split('(AND|,|OR) ', where_conditions_str, flags=re.IGNORECASE):
            if l % 2 == 0:
                conditions.append(process_where_expression(w.strip()))
            else:
                operators.append(w.strip().upper())
            l += 1
    else:
        conditions = None
        limit_token = re.split(from_tokens[1], SELECT_COMMAND_LIST[3], flags=re.IGNORECASE)

    if type(limit_token) is list and len(limit_token) > 1: limit = limit_token[1].strip()

    return columns, table_name, conditions, operators, limit


def process_where_expression(exp):
    tokens = re.split("(IN|LIKE|BETWEEN|IS|=|<>|<=|>=|!=)", exp, flags=re.IGNORECASE)
    assert len(tokens) == 3, "Syntax error in where expression: " + str(exp)
    conditions = []
    for t in tokens:
        conditions.append(t.strip())
    return conditions


#CREATE TABLE namespace.tablename;
def process_create_statement(create_statement):
    m = re.match(create_table_pattern, create_statement)
    return Create(m.groupdict()["tablename"])


#ADD INDEX TABLE namespace.tablename KEYS key1,key2
def process_index_statement(index_statement):
    m = re.match(add_index_pattern, index_statement)
    meta = m.groupdict()
    print meta
    keys = []
    for k in meta["keys"].strip().split(","):
        keys.append(k.strip())
    return AddIndex(meta["tablename"], keys)


def parse(sql_statement):
    query_string_list = get_query_list(sql_statement)
    query_list = []
    for qs in query_string_list:
        qs = re.sub(r'\s+', ' ', qs) #remove multiple spaces
        if qs.upper().startswith(SELECT_COMMAND_LIST[0]) :
            columns, table_name, conditions, operators, limit = process_select_statement(qs)
            conditions_list = []
            op = 0
            if conditions:
                for c in conditions:
                    conditions_list.append(Condition(c,operators[op]))
                    op += 1
            select = Select(columns, table_name, conditions_list, limit)
            query = Query(select)
            query_list.append(query)
        elif qs.upper().startswith("CREATE"):
            query_list.append(process_create_statement(qs))
        elif qs.upper().startswith("ADD"):
            if "INDEX" in qs.upper():
                query_list.append(query_list.append(process_index_statement(qs)))

    return query_list





