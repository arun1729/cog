import re

# SQL Grammar reference: https://forcedotcom.github.io/phoenix/

COMMAND_LIST = ["SELECT", "FROM", "WHERE"]


class Planner:

    def __init__(self, index, store):
        self.index = index
        self.store = store

    def execute(self, statement):
        print self.get_query_list(statement)

    def get_query_list(self, statement):
        query_list = statement.split(";")
        if len(query_list) == 1 and query_list[0] == '':
            return None

        if len(query_list) > 1 and query_list[-1] == '':
            query_list = query_list[0:-1]

        return query_list

    def get_select_command(self, query):

        select_tokens = re.split(COMMAND_LIST[0], query, flags=re.IGNORECASE)
        assert select_tokens[0] is '', "Syntax error: a query must start with Select."
        assert len(select_tokens) == 2, "Syntax error: invalid select statement."

        from_tokens = re.split(COMMAND_LIST[1], select_tokens[1], flags=re.IGNORECASE)
        assert len(from_tokens) == 2, "Syntax error: invalid select statement at FROM command."

        columns_str = from_tokens[0]
        columns = []
        for c in columns_str.split(","):
            columns.append(c.strip())

        where_tokens = re.split(COMMAND_LIST[2], from_tokens[1], flags=re.IGNORECASE)

        where_conditions = []
        operators = []
        if len(where_tokens) > 1:
            where_conditions_str = where_tokens[1]
            l = 0
            for w in re.split('(AND | , | OR) ', where_conditions_str, flags=re.IGNORECASE):
                print w
                if l%2 == 0:
                    where_conditions.append(w.strip())
                else:
                    operators.append(w.strip())
                l += 1
        else:
            where_conditions = None

        return columns, where_conditions, operators
