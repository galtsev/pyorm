
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
        instance[self.name] = value

class QueryIterator(object):
    def __init__(self, query, cur, props):
        self.query = query
        self.cur = cur
        self.props = props
        #sql, params, self.props = query.get_sql()
        #cur.execute(sql, params)
    def __iter__(self):
        return self
    def next(self):
        rec = self.cur.fetchone()
        if rec is None:
            raise StopIteration
        return self.query.model(**dict(zip(self.props, rec)))

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
    def __init__(self, **kwargs):
        self.data = kwargs
    @classmethod
    def tablename(cls):
        return cls._table_name
    @classmethod
    def get(cls, session, key):
        prop_list = cls._properties.keys()
        fields = ', '.join([cls._properties[p].fieldname for p in prop_list])
        sql = 'select %s from $(schema).%s where %s=%%s' % (fields, cls.tablename(), cls._properties[cls._key_prop].fieldname)
        sql = session.fix_sql(sql)
        cur = session.cursor()
        cur.execute(sql, (key,))
        rec = cur.fetchone()
        return cls(**dict(zip(prop_list, rec)))
    @classmethod
    def query(cls, session):
        return Query(cls, session)
    def __str__(self):
        key_prop = self._key_prop
        prop_names = self._properties.keys()
        vals = ["%s:%s" % (name,repr(getattr(self, name))) for name in prop_names if name!=key_prop]
        return "<Model %s: key(%s)=%s; %s>" % (self.__class__.__name__, self._key_prop, getattr(self, key_prop), '; '.join(vals))

def init_model(cls):
    if not hasattr(cls, '_properties'):
        cls._properties = {}
    for prop_name, prop in cls.__dict__.iteritems():
        if not prop_name.startswith('__') and isinstance(prop, Property):
            prop.init_property(cls, prop_name)
            cls._properties[prop_name] = prop
