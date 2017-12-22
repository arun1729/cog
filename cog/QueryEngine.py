
# SQL Grammar reference: https://forcedotcom.github.io/phoenix/
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

        if len(query_list) > 1 and query_list[-1] == '' :
            query_list = query_list[0:-1]

        return query_list






