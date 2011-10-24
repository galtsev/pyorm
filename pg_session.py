
class PgSession(object):
    def __init__(self, con, schema=''):
        self.con = con
        self.schema = schema
    def cursor(self):
        return self.con.cursor()
    def fix_sql(self, sql):
        schema = self.schema
        if schema:
            schema=schema+'.'
        return sql.replace('$(schema).', schema)
    def execute(self, sql, params):
        cur = self.cursor()
        cur.execute(self.fix_sql(sql), params)
        return cur
    def commit(self):
        self.con.commit()
