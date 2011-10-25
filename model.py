
import copy

class ORMError(Exception): pass
class FilterError(ORMError): pass

class Property(object):
    def __init__(self, fieldname=None, default=None):
        self.fieldname = fieldname
        self.default = default
    def init_property(self, cls, name):
        self.name = name
        if not self.fieldname:
            self.fieldname = name
    def __get__(self, instance, owner):
        return instance.data.get(self.name, self.default)
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
        res.saved = True
        return res

class Query(object):
    def __init__(self, model_class, session, props):
        self.model = model_class
        self.session = session
        self.filters = []
        self._order = ''
        self.props = props and props.split(',') or []
    def get_sql(self, limit=None, offset=None, head=None):
        m = self.model
        props = self.props or m._properties.keys()
        fields = ', '.join([m._properties[p].fieldname for p in props])
        if head:
            sql = [head]
        else:
            sql = ['select %s from %s' % (fields, m._table_name)]
        params = {}
        if self.filters:
            filters = []
            par_id = 1
            for prop, value in self.filters:
                arr = prop.strip().split(' ')
                if len(arr) == 1:
                    prop_name, op = prop, '='
                    if isinstance(value, list):
                        op = 'in'
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
            sql.append('limit %d' % limit)
        if offset:
            sql.append('offset %d' % offset)
        return '\n'.join(sql), params, props
    def __iter__(self):
        sql, params, props = self.get_sql()
        return QueryIterator(self, self.session.execute(sql, params), props)
    def fetch(self, limit, offset=0):
        sql, params, props = self.get_sql(limit=limit, offset=offset)
        return list(QueryIterator(self, self.session.execute(sql, params), props))
    def fetchone(self):
        res = self.fetch(limit=1)
        if res:
            return res[0]
        else:
            return None
    def filter(self, prop, value):
        self.filters.append((prop, value))
        return self
    def order(self, fields):
        self._order = fields.strip()
        return self
    def count(self):
        sql, params, props = self.get_sql(head = 'select count(1) from '+self.model._table_name)
        cur = self.session.execute(sql, params)
        return cur.fetchone()[0]
    def delete(self):
        sql, params, props = self.get_sql(head = 'delete from '+self.model._table_name)
        self.session.execute(sql, params)
    def update(self, param_dict):
        prop_list = param_dict.keys()
        m = self.model
        sql = []
        params = {}
        for p in prop_list:
            param_name = 'par_' + p
            sql.append("%s=%%(%s)s" % (m._properties[p].fieldname, param_name))
            params[param_name] = param_dict[p]
        head = 'update %s set\n' % m._table_name + ',\n'.join(sql) + '\n'
        sql, select_params, props = self.get_sql(head = head)
        params.update(select_params)
        self.session.execute(sql, params)

class Model(object):
    def __init__(self, session, **kwargs):
        self.session = session
        self.data = kwargs
        self.saved = False
    @classmethod
    def build_pk_cond(cls, key_dict, params):
        cond = []
        for prop in cls._key.split(','):
            field_name = cls._properties[prop].fieldname
            params['pk_'+field_name] = key_dict[prop]
            cond.append(field_name+'=%%(pk_%s)s' % field_name)
        return ' and '.join(cond)
    @classmethod
    def get(cls, session, key):
        prop_list = cls._properties.keys()
        fields = ', '.join([cls._properties[p].fieldname for p in prop_list])
        params = {}
        if not isinstance(key, tuple):
            key = (key,)
        key_dict = dict(zip(cls._key.split(','),key))
        sql = 'select %s from %s where %s' %(fields, cls._table_name, cls.build_pk_cond(key_dict, params))
        rec = session.execute(sql, params).fetchone()
        if rec:
            res = cls(session, **dict(zip(prop_list, rec)))
            res.saved = True
        else:
            res = None
        return res
    @classmethod
    def query(cls, session, props=''):
        return Query(cls, session, props)
    def before_save(self):
        pass
    def save(self):
        self.before_save()
        prop_list = self._properties.keys()
        #if hasattr(self, '_old_key'):
        if self.saved:
            # object already exist in db. update
            if hasattr(self, 'old'):
                # object properties modified
                sql = ["update %s set" % self._table_name]
                upd_fields = []
                params = {}
                for prop_name in prop_list:
                    if self.data.get(prop_name) != self.old.get(prop_name):
                        upd_fields.append("%s=%%(%s)s" % (self._properties[prop_name].fieldname, prop_name))
                        params[prop_name] = self[prop_name]
                if upd_fields:
                    sql.append(",\n".join(upd_fields))
                    sql.append("where")
                    sql.append(self.build_pk_cond(self.old, params))
                    self.session.execute('\n'.join(sql), params)
                del self.old
        else:
            # new object. insert
            fields = [self._properties[name].fieldname for name in prop_list]
            params = dict([(name, self[name]) for name in prop_list])
            sql = "insert into %s (%s)\n  values (%s)" % (self._table_name,
                    ','.join(fields),
                    ','.join(['%%(%s)s' % name for name in prop_list]))
            self.session.execute(sql, params)
        self.saved = True
    def cancel(self):
        if hasattr(self, 'old'):
            self.data = self.old
            del self.old
    def delete(self):
        if self.saved:
            params = {}
            sql = "delete from %s where %s" % (self._table_name, self.build_pk_cond(self.data, params))
            self.session.execute(sql, params)
    def __getitem__(self, key):
        return getattr(self, key)
    def __str__(self):
        key_prop = self._key
        prop_names = self._properties.keys()
        vals = ["%s:%s" % (name,repr(getattr(self, name))) for name in prop_names if name!=self._key]
        return "<Model %s: key(%s)=%s; %s>" % (self.__class__.__name__, self._key, self[self._key], '; '.join(vals))

def init_models(*classes):
    for cls in classes:
        if not hasattr(cls, '_properties'):
            cls._properties = {}
        for prop_name, prop in cls.__dict__.iteritems():
            if not prop_name.startswith('__') and isinstance(prop, Property):
                prop.init_property(cls, prop_name)
                cls._properties[prop_name] = prop
