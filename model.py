
import copy

class ORMError(Exception): pass
class FilterError(ORMError): pass

class Property(object):
    def __init__(self, fieldname=None):
        self.fieldname = fieldname
    def init_property(self, cls, name):
        self.name = name
        if not self.fieldname:
            self.fieldname = name
    def __get__(self, instance, owner):
        return instance.data[self.name]
    def __set__(self, instance, value):
        if not hasattr(instance, 'old'):
            instance.old = copy.copy(instance.data)
        instance.data[self.name] = value

class QueryIterator(object):
    def __init__(self, query, cur, props):
        self.query = query
        self.cur = cur
        self.props = props
    def __iter__(self):
        return self
    def next(self):
        rec = self.cur.fetchone()
        if rec is None:
            raise StopIteration
        res = self.query.model(self.query.session, **dict(zip(self.props, rec)))
        res._old_key = res.data[res._key]
        return res

class Query(object):
    def __init__(self, model_class, session):
        self.model = model_class
        self.session = session
        self.filters = []
        self._order = ''
    def get_sql(self, limit=None, offset=None):
        m = self.model
        props = m._properties.keys()
        fields = ', '.join([m._properties[p].fieldname for p in props])
        sql = ['select', fields, 'from $(schema).'+m.tablename()]
        if self.filters:
            filters = []
            par_id = 1
            params = {}
            for prop, value in self.filters:
                arr = prop.strip().split(' ')
                if len(arr) == 1:
                    prop_name, op = prop, '='
                elif len(arr) == 2:
                    prop_name, op = arr
                else:
                    raise FilterError("Wrong filter prop: %s" % repr(prop))
                fld = m._properties[prop_name].fieldname
                if value is None:
                    if op == '=':
                        op = 'is null'
                    elif op in ('!=', '<>'):
                        op = 'is not null'
                    else:
                        raise FilterError("Wrong filter operator for None: %s" % repr(op))
                    filters.append("%s %s" % (fld, op))
                else:
                    par_name = prop_name+str(par_id)
                    par_id += 1
                    params[par_name] = value
                    filters.append("%s %s %%(%s)s" % (fld, op, par_name))
            sql.append( 'where %s' % ' and '.join(filters))
        if self._order:
            order = []
            for prop_name in  [f.strip() for f in self._order.split(',')]:
                desc = ''
                if prop_name[0]=='-':
                    desc = ' desc'
                    prop_name = prop_name[1:]
                order.append('%s%s' % (m._properties[prop_name].fieldname, desc))
            sql.append( 'order by %s' % (', '.join(order)))
        if limit:
            sql.append('limit %s' % limit)
        if offset:
            sql.append('offset %s' % offset)
        sql = self.session.fix_sql('\n'.join(sql))
        return sql, params, props
    def __iter__(self):
        sql, params, props = self.get_sql()
        cur = self.session.cursor()
        cur.execute(sql, params)
        return QueryIterator(self, cur, props)
    def fetch(self, limit, offset=0):
        sql, params, props = self.get_sql(limit, offset)
        cur = self.session.cursor()
        cur.execute(sql, params)
        return list(QueryIterator(self, cur, props))
    def filter(self, prop, value):
        self.filters.append((prop, value))
        return self
    def order(self, fields):
        self._order = fields.strip()
        return self

class Model(object):
    def __init__(self, session, **kwargs):
        self.session = session
        self.data = kwargs
    @classmethod
    def tablename(cls):
        return cls._table_name
    @classmethod
    def key_field(cls):
        return cls._properties[cls._key].fieldname
    @classmethod
    def get(cls, session, key):
        prop_list = cls._properties.keys()
        fields = ', '.join([cls._properties[p].fieldname for p in prop_list])
        sql = 'select %s from $(schema).%s where %s=%%s' % (fields, cls.tablename(), cls.key_field())
        sql = session.fix_sql(sql)
        cur = session.cursor()
        cur.execute(sql, (key,))
        rec = cur.fetchone()
        if rec:
            res = cls(session, **dict(zip(prop_list, rec)))
            res._old_key = res[cls._key]
        else:
            res = None
        return res
    @classmethod
    def query(cls, session):
        return Query(cls, session)
    def save(self):
        prop_list = self._properties.keys()
        if hasattr(self, '_old_key'):
            # object already exist in db. update
            if hasattr(self, 'old'):
                # object properties modified
                sql = ["update $(schema).%s set" % self.tablename()]
                upd_fields = []
                params = {}
                for prop_name in prop_list:
                    if self.data.get(prop_name) != self.old.get(prop_name):
                        upd_fields.append("%s=%%(%s)s" % (self._properties[prop_name].fieldname, prop_name))
                        params[prop_name] = self.data.get(prop_name)
                if upd_fields:
                    sql.append(",\n".join(upd_fields))
                    sql.append("where %s=%%(old_key)s" % self.key_field())
                    params['old_key'] = self._old_key
                    sql = self.session.fix_sql('\n'.join(sql))
                    self.session.cursor().execute(sql, params)
                del self.old
        else:
            # new object. insert
            fields = [self._properties[name].fieldname for name in prop_list]
            params = dict([(name, self.data[name]) for name in prop_list])
            sql = "insert into $(schema).%s (%s)\n  values (%s)" % (self.tablename(),
                    ','.join(fields),
                    ','.join(['%%(%s)s' % name for name in param_names]))
            sql = self.session.fix_sql(sql)
            self.session.cursor().execute(sql, params)
        self._old_key = self[self._key]
    def delete(self):
        if self._old_key:
            sql = self.session.fix_sql("delete from $(schema).%s where %s=%%s" % (self.tablename(), self.key_field()))
            self.session.cursor().execute(sql, (self._old_key,))
    def __getitem__(self, key):
        return self.data[key]
    def __str__(self):
        key_prop = self._key
        prop_names = self._properties.keys()
        vals = ["%s:%s" % (name,repr(getattr(self, name))) for name in prop_names if name!=self._key]
        return "<Model %s: key(%s)=%s; %s>" % (self.__class__.__name__, self._key, self[self._key], '; '.join(vals))

def init_model(cls):
    if not hasattr(cls, '_properties'):
        cls._properties = {}
    for prop_name, prop in cls.__dict__.iteritems():
        if not prop_name.startswith('__') and isinstance(prop, Property):
            prop.init_property(cls, prop_name)
            cls._properties[prop_name] = prop
