from collections import defaultdict
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm.exc import DetachedInstanceError
from sqlalchemy.sql.schema import Column
from sqlalchemy.sql.sqltypes import Integer

_sa_meta = ('__abstract__', '__mapper__', '__table__', '__table_args__',
            '__tablename__', '__weakref__', '_sa_class_manager',
            '_sa_instance_state', '_sa_registry', 'metadata', 'registry',
            '_column_attr_update', '_sa_instances', '_pure_instance',
            '_pure_dict')


class _base(object):
    __table_args__ = ({
        'mysql_collate': 'utf8mb4_unicode_ci',
    }, )

    def _column_attr_update(self, attr: str, value, add_only=True):
        _attr = getattr(self, attr)
        if isinstance(_attr, list):
            if set(_attr) != set(value):
                if add_only is False:
                    attr_to_pop = set(_attr).difference(value)
                    _to_pop = []
                    for _obj in _attr:
                        if _obj in attr_to_pop:
                            _to_pop.append(_obj)
                    for _ in _to_pop:
                        _attr.remove(_)
                _attr.extend([item for item in value if item not in _attr])
        elif _attr != value:
            setattr(self, attr, value)

    def __repr__(self) -> str:
        li = []
        for k, v in self.__dict__.items():
            if k.startswith('_') is False and k.endswith('_id') is False:
                try:
                    if v:
                        if isinstance(v, (tuple, list, set)):
                            if type(v[0]).__repr__ is _base.__repr__:
                                _v = ', '.join([
                                    getattr(item, 'name',
                                            object.__repr__(item))
                                    for item in v
                                ])
                            else:
                                _v = ', '.join([str(item) for item in v])
                        elif isinstance(v, _base):
                            if type(v).__repr__ is _base.__repr__:
                                _v = str(getattr(v, 'name',
                                                 object.__repr__(v)))
                            else:
                                _v = str(v)
                        else:
                            _v = str(v)
                        li.append(f'{k}: {_v}')
                except DetachedInstanceError:
                    pass
        li.sort()
        return '\n'.join(li)

    @property
    def _sa_instances(self) -> dict:
        return _collect_sa_instances(self)

    @property
    def _pure_instance(self) -> object:
        return pure_instance(self)

    @property
    def _pure_dict(self) -> dict:
        return pure_dict(self)


Base = declarative_base(cls=_base)


class BaseMixin(object):
    id = Column(Integer, primary_key=True, autoincrement=True)


def _collect_sa_instances(instance):
    sa_instances_collection = {}

    def inner(instance):
        collection = {id(instance): instance}
        for name in dir(instance):
            if name not in _sa_meta and (name.startswith('__') is False
                                         and name.endswith('__') is False):
                try:
                    attr = getattr(instance, name)
                    if isinstance(attr, list):
                        for obj in attr:
                            if isinstance(obj, Base):
                                collection[id(obj)] = obj
                    elif isinstance(attr, Base):
                        collection[id(attr)] = attr
                except DetachedInstanceError:
                    pass
        return collection

    need_to_search = [instance]
    while need_to_search:
        new_collection = inner(need_to_search.pop(0))
        for k, v in new_collection.items():
            if k not in sa_instances_collection.keys():
                need_to_search.append(v)
        sa_instances_collection.update(new_collection)
    return sa_instances_collection


def pure_instance(instance) -> object:
    retains = ('__bool__', '__len__', '__eq__', '__repr__', '__str__')
    instances = {}
    new_types_collection = defaultdict(dict)
    new_types_prefix = 'TempType_'

    def type_name(obj: type):
        return obj.__module__ + '.' + obj.__name__

    # collect sa instances
    sa_instances_collection = _collect_sa_instances(instance)

    # collect new types data
    for k, v in sa_instances_collection.items():
        for name in dir(v):
            if name in retains:
                new_types_collection[type_name(type(v))][name] = getattr(
                    type(v), name)
    # generate new types dynamically
    for k, v in new_types_collection.items():
        new_types_collection[k] = type(new_types_prefix + k, (object, ), v)
    default_type = type(new_types_prefix + 'default', (object, ), {})
    # filter sa meta attributes
    for k, v in sa_instances_collection.items():
        _typename = type_name(type(v))
        instances[k] = new_types_collection.get(_typename, default_type)()
    for v in sa_instances_collection.values():
        for name in dir(v):
            if name not in _sa_meta and (name.startswith('__') is False
                                         and name.endswith('__') is False):
                try:
                    attr = getattr(v, name)
                    if isinstance(attr, list):
                        _ = []
                        for i in range(len(attr)):
                            if isinstance(attr[i], _base):
                                _.append(instances[id(attr[i])])
                            else:
                                _.append(attr[i])
                        setattr(instances[id(v)], name, _)
                    elif isinstance(attr, _base):
                        setattr(instances[id(v)], name, instances[id(attr)])
                    else:
                        setattr(instances[id(v)], name, attr)
                except DetachedInstanceError:
                    setattr(instances[id(v)], name, None)

    main = instances[id(instance)]
    # accessibility to original sa instance
    main.__sa_origin__ = instance
    # accessibility to all associations
    main.__associations__ = instances
    return main


def pure_dict(instance) -> dict:
    sa_instances_collection = _collect_sa_instances(instance)
    dicts = defaultdict(dict)
    for v in sa_instances_collection.values():
        for name in dir(v):
            if name not in _sa_meta and (name.startswith('__') is False
                                         and name.endswith('__') is False):
                try:
                    attr = getattr(v, name)
                    if isinstance(attr, list):
                        _ = []
                        for i in range(len(attr)):
                            if isinstance(attr[i], _base):
                                _.append(dicts[id(attr[i])])
                            else:
                                _.append(attr[i])
                        dicts[id(v)][name] = _
                    elif isinstance(attr, _base):
                        dicts[id(v)][name] = dicts[id(attr)]
                    else:
                        dicts[id(v)][name] = attr
                except DetachedInstanceError:
                    dicts[id(v)][name] = None
    main = dicts[id(instance)]
    main['__sa_origin__'] = instance
    main['__associations__'] = dicts
    return main


__all__ = ('Base', 'BaseMixin', 'pure_instance', 'pure_dict')
