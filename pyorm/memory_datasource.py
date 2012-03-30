
from os.path import isfile
import cPickle as pc

def eq(x, y): return x == y
def neq(x, y): return x != y
def lt(x, y): return x < y
def gt(x, y): return x > y
def le(x, y): return x <= y
def ge(x, y): return x >= y
def in_(x,y): return x in y
def is_null(x): return x is None
def is_not_null(x): return x is not None

cmp_map = {
    '=' : eq,
    '!=': neq,
    '<>': neq,
    '<' : lt,
    '<=': le,
    '>' : gt,
    '>=': ge,
    'in': in_,
}

def make_cond_1(ind, fn):
    def cmp(rec):
        return fn(rec[ind])
    return cmp

def make_cond_2(ind, op, val):
    fn = cmp_map[op]
    def cmp(rec):
        return fn(rec[ind], val)
    return cmp

def desc(a, b):
    return cmp(b,a)

class QueryIterator(object):
    def __init__(self, query):
        self.query = query
        self.cur = query.table.data.itervalues()
        self.props = [(p,i) for i, p in enumerate(query.table.props) if p in query.props]
    def __iter__(self):
        return self
    def next(self):
        rec = self.cur.next()
        while rec and not self.query.check(rec):
            rec = self.cur.next()
        if rec is None:
            raise StopIteration()
        res = self.query.model_cls(self.query.ds, **dict((p, rec[i]) for p,i in self.props))
        res.saved = True
        return res

class Query(object):
    def __init__(self, model_cls, ds, props):
        self.model_cls = model_cls
        self.ds = ds
        self.table = ds.get_table(model_cls)
        self.conditions = []
        self._order = ''
        if props:
            self.props = props.split(',')
        elif model_cls._default_props:
            self.props = model_cls._default_props
        else:
            self.props = [p for p in model_cls._properties.keys() if not model_cls._properties[p].virtual]
    def filter(self, *args, **kwargs):
        if args:
            assert len(args)==2, "filter method require exactly 2 positional parameters"
            self.append_filter(*args)
        if kwargs:
            for prop, value in kwargs.items():
                self.append_filter(prop, value)
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
        ind = self.props.index(prop)
        if value is None:
            if op == '=':
                fn = is_null
            elif op in ('!=', '<>'):
                fn = is_not_null
            else:
                raise FilterError("Wrong filter operator for None: %s" % repr(op))
            self.conditions.append(make_cond_1(ind, fn))
        else:
            self.conditions.append(make_cond_2(ind, op, value))
    def check(self, rec):
        for cond in self.conditions:
            if not cond(rec):
                return False
        return True
    def get_rec_list(self):
        return [r for r in self.table.data.itervalues() if self.check(r)]
    def __iter__(self):
        #return QueryIterator(self)
        props = [(p,i) for i, p in enumerate(self.table.props) if p in self.props]
        def map_fnc(rec):
            res = self.model_cls(self.ds, **dict((p, rec[i]) for p,i in props))
            res.saved = True
            return res
        l = map(map_fnc, self.get_rec_list())
        if self._order:
            l.sort(cmp = self._order)
        return l.__iter__()
    def order(self, props):
        props = [p.strip() for p in props.split(',')]
        sort_list = []
        for p in props:
            if p.startswith('-'):
                sort_list.append((p[1:], desc))
            else:
                sort_list.append((p, cmp))
        def cmp_fnc(a,b):
            for p, fn in sort_list:
                r = fn(a[p],b[p])
                if r:
                    return r
            return r
        self._order = cmp_fnc
        return self
    def count(self):
        return len(self.get_rec_list())
    def delete(self):
        map(self.ds.delete, self)
    def fetch(self, limit, offset=0):
        return list(self.__iter__())[offset, limit+offset]
    def fetchone(self):
        try:
            return self.__iter__().next()
        except StopIteration:
            return None
    def update(self, param_dict):
        for row in self:
            for k, v in param_dict.items():
                row[k] = v
            row.save()

class Table(object):
    def __init__(self, name, key, props):
        self.name = name
        self.props = props
        self.key = key
        self.data = {}

class DataSet(object):
    def __init__(self, filename=''):
        self.filename = filename
        self.load(filename)
    def load(self, filename):
        if filename and isfile(filename):
            f = open(filename, 'r')
            self.data = pc.load(f)
            f.close()
        else:
            self.data = {}
    def commit(self):
        if self.filename:
            f = open(self.filename, 'w')
            pc.dump(self.data, f)
            f.close()
    def rollback(self):
        self.load(self.filename)
    def get_table(self, model_cls):
        table_name = model_cls._table_name
        if table_name in self.data:
            return self.data[table_name]
        props = [p.name for p in model_cls._properties.values() if not p.virtual]
        table = Table(table_name, model_cls._key, props)
        self.data[table_name] = table
        return table
    def get(self, model_cls, key):
        table = self.get_table(model_cls)
        rec = table.data.get(key)
        if rec is None:
            return None
        vals = dict(zip(table.props, rec))
        m =  model_cls(self, **vals)
        m.saved = True
        return m
    def save(self, model):
        table = self.get_table(model)
        if ',' in model._key:
            key = tuple(model[k] for k in model._key.split(','))
        else:
            key = model[model._key]
        if key in table.data:
            rec = list(table.data[key])
            for i, p in enumerate(table.props):
                if model.data.get(p) != rec[i]:
                    rec[i] = model.data.get(p)
            rec = tuple(rec)
            if hasattr(model, 'old'):
                del model.old
        else:
            rec = tuple(model[p] for p in table.props)
        table.data[key] = rec
    def delete(self, model):
        table = self.get_table(model)
        if ',' in model._key:
            key = tuple(model[k] for k in model._key.split(','))
        else:
            key = model[model._key]
        if key in table.data:
            del table.data[key]
    def query(self, model_class, props):
        return Query(model_class, self, props)


