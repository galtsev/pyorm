
class PgSession(object):
    def __init__(self, con, schema=''):
        self.con = con
        self.schema = schema
    def cursor(self):
        return self.con.cursor()
    def fix_sql(self, sql):
        return sql.replace('$(schema)', self.schema)
