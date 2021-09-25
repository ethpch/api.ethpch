from typing import Any, Dict, Union, Iterable, Literal
from sqlalchemy import orm
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.sql.expression import insert as _insert
from sqlalchemy.sql.expression import select as _select
from sqlalchemy.sql.expression import update as _update
from sqlalchemy.sql.expression import delete as _delete
from sqlalchemy.sql.dml import Insert, Update, Delete
from sqlalchemy.sql.selectable import Select, FromClause, Selectable
from sqlalchemy.sql.visitors import Visitable
from sqlalchemy.sql.expression import or_


def insert(table: Union[str, Selectable],
           values: Dict[str, Any] = {}) -> Insert:
    statement = _insert(table)
    if values:
        statement = statement.values(**values)
    return statement


def select(
    *table_or_column: Iterable[Union[ColumnElement, FromClause, int]],
    eagerloads: Iterable[str] = [],
    eagerload_strategy: Literal['joinedload', 'subqueryload',
                                'selectinload'] = None,
    joins: Iterable[FromClause] = [],
    whereclauses: Iterable[Union[str, bool, Visitable,
                                 Iterable[Union[str, bool, Visitable]]]] = [],
    select_from: FromClause = None,
    order_by: Union[str, bool, Visitable, None] = None,
    limit: Union[int, str, Visitable, None] = None,
    offset: Union[int, str, Visitable, None] = None,
) -> Select:
    statement = _select(table_or_column)
    eagerloads = list(set(eagerloads))
    if eagerloads:
        eagerload_strategy = eagerload_strategy or 'joinedload'
        eagerload_func = getattr(orm, eagerload_strategy)
        for eagerload in eagerloads:
            if '.' in eagerload:
                main = eagerload.split('.')
                eager = eagerload_func(main[0])
                for item in main[1:]:
                    eager = getattr(eager, eagerload_strategy)(item)
                statement = statement.options(eagerload_func(eager))
            else:
                statement = statement.options(eagerload_func(eagerload))
    for join in joins:
        statement = statement.join(join)
    for i in range(len(whereclauses)):
        if isinstance(whereclauses[i], (list, tuple)):
            whereclauses[i] = or_(*whereclauses[i])
    statement = statement.where(*whereclauses)
    if select_from is not None:
        statement = statement.select_from(select_from)
    if order_by is not None:
        statement = statement.order_by(order_by)
    if limit is not None:
        statement = statement.limit(limit)
    if offset is not None:
        statement = statement.offset(offset)
    return statement


def update(
    table: Union[str, Selectable],
    whereclauses: Iterable[Union[str, bool, Visitable,
                                 Iterable[Union[str, bool, Visitable]]]] = [],
    values: Dict[str, Any] = ...,
) -> Update:
    statement = _update(table)
    for i in range(len(whereclauses)):
        if isinstance(whereclauses[i], (list, tuple)):
            whereclauses[i] = or_(*whereclauses[i])
    statement = statement.where(*whereclauses)
    statement.values(**values)
    return statement


def delete(
    table: Union[str, Selectable],
    whereclauses: Iterable[Union[str, bool, Visitable,
                                 Iterable[Union[str, bool, Visitable]]]] = [],
) -> Delete:
    statement = _delete(table)
    for i in range(len(whereclauses)):
        if isinstance(whereclauses[i], (list, tuple)):
            whereclauses[i] = or_(*whereclauses[i])
    statement = statement.where(*whereclauses)
    return statement
