import typing as t
from contextlib import contextmanager
from sqlalchemy.orm.session import Session
from src.defs.utils import sessionMaker
from copy import copy
from collections import ChainMap
from collections.abc import Iterable
from decimal import Decimal
from sqlalchemy.sql.selectable import Alias, CTE, Select

@contextmanager
def session_scope():
    """Provide a transactional scope for series of operations"""
    session = sessionMaker()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise Exception('PostgreSQL query failed.')
    finally:
        session.close()

def result_to_dict(result) -> dict:
    def _parse_value(x):
        if type(x) == Decimal:
            return int(x) if x == x.to_integral_value() else float(x)
        else:
            return x
    return { key: _parse_value(value) for key, value in result.items() }

def run_query(q: Select) -> t.List[dict]:
    with session_scope() as session:
        results = session.execute(q).mappings().all()
        parsed_res = [ result_to_dict(result) for result in results ]
    return parsed_res

def get_first(q: Select) -> t.Optional[dict]:
    with session_scope() as session:
        result = session.execute(q).mappings().first()
        parsed_res = result_to_dict(result) if result else None
    return parsed_res
