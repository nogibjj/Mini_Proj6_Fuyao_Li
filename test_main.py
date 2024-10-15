"""
Test goes here

"""
from mylib.extract import extract
from mylib.transform_load import load
from mylib.query import (
    join_sql,
    agg_order_sql,
)

def test_extract():
    results = extract()
    assert results is not None


def test_load():
    results = load()
    assert results is not None


def test_join_sql():
    data = join_sql()
    assert data is not None 


def test_agg_order_sql():
    assert agg_order_sql() is not None 
