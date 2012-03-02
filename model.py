
import copy

class ORMError(Exception): pass
class FilterError(ORMError): pass

class Property(object):
    virtual = False
    def __init__(self, fieldname=None, default=None, virtual=False):
        self.fieldname = fieldname
        self.default = default
        if virtual:
            self.virtual = virtual
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

class ModelMetaclass(type):
    def __new__(model_metaclass, model_class_name, model_class_base, model_attrs):
        model_class = type.__new__(model_metaclass, model_class_name, model_class_base, model_attrs)
        properties = {}
        if hasattr(model_class, '_properties'):
            properties.update(model_class._properties)
        model_class._properties = properties
        for attr_name, attr in model_attrs.iteritems():
            if isinstance(attr, Property):
                attr.init_property(model_class, attr_name)
                model_class._properties[attr_name] = attr
        return model_class

class Model(object):
    __metaclass__ = ModelMetaclass
    __allow_access_to_unprotected_subobjects__=1
    _default_props = None
    def __init__(self, session, **kwargs):
        self.session = session
        self.data = kwargs
        self.saved = False
    @property
    def ds(self):
        return self.session
    @property
    def changed(self):
        return hasattr(self, 'old')
    @classmethod
    def get_from(cls):
        return "from " + cls._table_name
    @classmethod
    def get(cls, dataset, key):
        return dataset.get(cls, key)
    @classmethod
    def query(cls, dataset, props=''):
        return dataset.query(cls, props)
    def before_save(self):
        pass
    def save(self):
        self.before_save()
        self.session.save(self)
        self.saved = True
    def cancel(self):
        if hasattr(self, 'old'):
            self.data = self.old
            del self.old
    def delete(self):
        if self.saved:
            self.session.delete(self)
    def __getitem__(self, key):
        return getattr(self, key)
    def __setitem__(self, key, value):
        if key in self._properties:
            setattr(self, key, value)
        else:
            raise ORMError("Wrong property name %s of instance %s" % (key, repr(type(self))))

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
