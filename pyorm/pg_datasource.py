from model import FilterError

class QueryIterator(object):
    def __init__(self, query, cur):
        self.query = query
        self.cur = cur
    def __iter__(self):
        return self
    def next(self):
        rec = self.cur.fetchone()
        if rec is None:
            raise StopIteration
        res = self.query.model(self.query.session, **dict(zip(self.query.props, rec)))
        res.saved = True
        return res

class Query(object):
    def __init__(self, model_class, session, props):
        self.model = model_class
        self.session = session
        self._from = model_class.get_from()
        self.conditions = []
        self.params = {}
        self.par_id = 1
        self._order = ''
        if props:
            self.props = props.split(',')
        elif model_class._default_props:
            self.props = model_class._default_props
        else:
            self.props = [p for p in model_class._properties.keys() if not model_class._properties[p].virtual]
    def filter(self, *args, **kwargs):
        if args:
            assert len(args)==2, "filter method require exactly 2 positional parameters"
            self.append_filter(*args)
        if kwargs:
            for prop, value in kwargs.items():
                self.append_filter(prop, value)
        return self
    def raw_filter(self, sql, params={}):
        self.conditions.append(sql)
        self.params.update(params)
        return self
    def append_filter(self, prop, value):
        arr = prop.strip().split(' ')
        if len(arr) == 1:
            prop_name, op = prop, '='
            if isinstance(value, list):
                op = 'in'
        elif len(arr) == 2:
            prop_name, op = arr
        else:
            raise FilterError("Wrong filter prop: %s" % repr(prop))
        fld = self.model._properties[prop_name].fieldname
        if value is None:
            if op == '=':
                op = 'is null'
            elif op in ('!=', '<>'):
                op = 'is not null'
            else:
                raise FilterError("Wrong filter operator for None: %s" % repr(op))
            self.conditions.append("%s %s" % (fld, op))
        else:
            par_name = prop_name+str(self.par_id)
            self.par_id += 1
            self.params[par_name] = value
            self.conditions.append("%s %s %%(%s)s" % (fld, op, par_name))
    def get_sql(self, limit=None, offset=None, head=None, ignore_order=False):
        m = self.model
        fields = ', '.join([m._properties[p].fieldname for p in self.props])
        sql = [head or 'select ' + fields]
        sql.append(self._from)
        if self.conditions:
            sql.append( 'where %s' % ' and '.join(self.conditions))
        if self._order and not ignore_order:
            sql.append( 'order by %s' % (', '.join(self._order)))
        if limit:
            sql.append('limit %d' % limit)
        if offset:
            sql.append('offset %d' % offset)
        return '\n'.join(sql)
    def __iter__(self):
        return QueryIterator(self, self.session.execute(self.get_sql(), self.params))
    def fetch(self, limit, offset=0):
        return list(QueryIterator(self, self.session.execute(self.get_sql(limit=limit, offset=offset), self.params)))
    def fetchone(self):
        res = self.fetch(limit=1)
        if res:
            return res[0]
        else:
            return None
    def order(self, fields):
        self._order = []
        for prop_name in  [f.strip() for f in fields.split(',')]:
            desc = ''
            if prop_name[0]=='-':
                desc = ' desc'
                prop_name = prop_name[1:]
            self._order.append('%s%s' % (self.model._properties[prop_name].fieldname, desc))
        return self
    def raw_order(self, fields):
        self._order = [fields]
        return self
    def count(self):
        return self.session.execute(self.get_sql(head = 'select count(1)', ignore_order=True), self.params).fetchone()[0]
    def delete(self):
        self.session.execute(self.get_sql(head = 'delete', ignore_order=True), self.params)
    def update(self, param_dict):
        prop_list = param_dict.keys()
        m = self.model
        params = {}
        fields = []
        for p in prop_list:
            param_name = 'par_' + p
            fields.append("%s=%%(%s)s" % (m._properties[p].fieldname, param_name))
            params[param_name] = param_dict[p]
        sql = ['update %s set\n' % m._table_name]
        sql.append(','.join(fields))
        if self.conditions:
            sql.append( 'where %s' % ' and '.join(self.conditions))
        params.update(self.params)
        self.session.execute('\n'.join(sql), params)

class DataSet(object):
    def __init__(self, con, schema=''):
        self.con = con
        self.schema = schema
    def cursor(self):
        return self.con.cursor()
    def build_pk_cond(self, model_cls, key_dict, params):
        cond = []
        for prop in model_cls._key.split(','):
            field_name = model_cls._properties[prop].fieldname
            params['pk_'+field_name] = key_dict[prop]
            cond.append(field_name+'=%%(pk_%s)s' % field_name)
        return ' and '.join(cond)
    def get(self, model_cls, key):
        prop_list = [p for p in model_cls._properties.keys() if not model_cls._properties[p].virtual]
        fields = ', '.join([model_cls._properties[p].fieldname for p in prop_list])
        params = {}
        if not isinstance(key, tuple):
            key = (key,)
        key_dict = dict(zip(model_cls._key.split(','),key))
        sql = 'select %s from %s where %s' %(fields, model_cls._table_name, self.build_pk_cond(model_cls, key_dict, params))
        rec = self.execute(sql, params).fetchone()
        if rec:
            res = model_cls(self, **dict(zip(prop_list, rec)))
            res.saved = True
        else:
            res = None
        return res
    def save(self, model):
        prop_list = [p for p in model._properties.keys() if not model._properties[p].virtual]
        if model.saved:
            # object already exist in db. update
            if model.changed:
                # object properties modified
                sql = ["update %s set" % model._table_name]
                upd_fields = []
                params = {}
                for prop_name in prop_list:
                    if model.data.get(prop_name) != model.old.get(prop_name):
                        upd_fields.append("%s=%%(%s)s" % (model._properties[prop_name].fieldname, prop_name))
                        params[prop_name] = model[prop_name]
                if upd_fields:
                    sql.append(",\n".join(upd_fields))
                    sql.append("where")
                    sql.append(self.build_pk_cond(model, model.old, params))
                    self.execute('\n'.join(sql), params)
                del model.old
        else:
            # new object. insert
            fields = [model._properties[name].fieldname for name in prop_list]
            params = dict([(name, model[name]) for name in prop_list])
            sql = "insert into %s (%s)\n  values (%s)" % (model._table_name,
                    ','.join(fields),
                    ','.join(['%%(%s)s' % name for name in prop_list]))
            self.execute(sql, params)
    def delete(self, model):
        params = {}
        sql = "delete from %s where %s" % (model._table_name, self.build_pk_cond(model, model.data, params))
        self.execute(sql, params)
    def query(self, model_class, props):
        return Query(model_class, self, props)
    def fix_sql(self, sql):
        schema = self.schema
        if schema:
            schema=schema+'.'
        return sql.replace('$(schema).', schema)
    def execute(self, sql, params=None):
        cur = self.cursor()
        cur.execute(self.fix_sql(sql), params)
        return cur
    def commit(self):
        self.con.commit()

