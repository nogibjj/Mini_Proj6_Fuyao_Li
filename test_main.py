"""
Test goes here

"""
from mylib.extract import extract
from mylib.transform_load import load
from mylib.query import (
    update_subject,
    delete_subject,
    create_subject,
    read_data,
)

def test_extract():
    results = extract()
    assert results is not None


def test_load():
    results = load()
    assert results is not None


def test_read_data():
    data = read_data()
    assert data is not None 


def test_update_subject():
    update_query = (101, "8/29/2018", "Durham South", "Durham", "NC", 35.99, 78.90)
    assert update_subject(update_query) is True


def test_delete_subject():
    id = 100
    assert delete_subject(id) is True


def test_create_subject():
    query = ("6/27/2018", "Durham South", "Durham", "NC", 35.99, 78.90)
    assert create_subject(query) is True
