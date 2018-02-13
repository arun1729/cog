import re

# SQL Grammar reference: https://forcedotcom.github.io/phoenix/

COMMAND_LIST = ["SELECT", "FROM", "WHERE"]

class Parser:

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

    def process_select_statement(self, query):

        select_tokens = re.split(COMMAND_LIST[0], query, flags=re.IGNORECASE)
        assert select_tokens[0] is '', "Syntax error: a query must start with SELECT."
        assert len(select_tokens) == 2, "Syntax error: invalid SELECT statement."

        from_tokens = re.split(COMMAND_LIST[1], select_tokens[1], flags=re.IGNORECASE)
        assert len(from_tokens) == 2, "Syntax error: invalid select statement at FROM command."

        columns_str = from_tokens[0]
        select_expression = []
        for c in columns_str.split(","):
            select_expression.append(c.strip())

        where_tokens = re.split(COMMAND_LIST[2], from_tokens[1], flags=re.IGNORECASE)

        where_expressions = []
        conditions = []
        if len(where_tokens) > 1:
            where_conditions_str = where_tokens[1]
            l = 0
            for w in re.split('(AND|,|OR) ', where_conditions_str, flags=re.IGNORECASE):
                if l%2 == 0:
                    where_expressions.append(w.strip())
                else:
                    conditions.append(w.strip().upper())
                l += 1
        else:
            where_expressions = None

        return select_expression, where_expressions, conditions

    def process_where_expression(self, where_expression):
        operations = []
        for exp in where_expression:
            tokens = re.split("(IN|LIKE|BETWEEN|IS|=|<>|<=|>=|!=)", exp, flags=re.IGNORECASE)
            assert len(tokens) == 3, "Syntax error in where expression: " + str(exp)
            cleaned = []
            for t in tokens:
                cleaned.append(t.strip())
            operations.append(cleaned)
        return operations

        return None
