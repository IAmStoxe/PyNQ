import pytest
from pynq import Queryable, Grouping

@pytest.fixture
def data():
    return [
        {"id": 1, "name": "Alice", "age": 25, "city": "New York"},
        {"id": 2, "name": "Bob", "age": 30, "city": "London"},
        {"id": 3, "name": "Charlie", "age": 35, "city": "Paris"},
        {"id": 4, "name": "David", "age": 40, "city": "New York"},
        {"id": 5, "name": "Eve", "age": 45, "city": "London"}
    ]

@pytest.fixture
def queryable(data):
    return Queryable(data)

def test_where_select(queryable):
    result = queryable \
        .where(lambda x: x["age"] > 30) \
        .select(lambda x: x["name"]) \
        .to_list()
    assert result == ["Charlie", "David", "Eve"]

def test_group_by_having(queryable):
    result = queryable \
        .group_by(lambda x: x["city"]) \
        .where(lambda g: g.count() > 1) \
        .select(lambda g: (g.key, g.count())) \
        .to_list()
    assert result == [("New York", 2), ("London", 2)]

def test_distinct_count(queryable):
    result = queryable \
        .select(lambda x: x["city"]) \
        .distinct() \
        .count()
    assert result == 3

def test_first_single_last(queryable):
    result = queryable \
        .where(lambda x: x["city"] == "London") \
        .order_by(lambda x: x["age"]) \
        .first()
    assert result["name"] == "Bob"

    result = queryable \
        .where(lambda x: x["city"] == "Paris") \
        .single()
    assert result["name"] == "Charlie"

    result = queryable \
        .where(lambda x: x["city"] == "New York") \
        .order_by(lambda x: x["age"]) \
        .last()
    assert result["name"] == "David"

def test_skip_take(queryable):
    result = queryable \
        .order_by(lambda x: x["age"]) \
        .skip(2) \
        .take(2) \
        .select(lambda x: x["name"]) \
        .to_list()
    assert result == ["Charlie", "David"]

def test_aggregate(queryable):
    result = queryable \
        .select(lambda x: x["age"]) \
        .aggregate(0, lambda acc, x: acc + x)
    assert result == 175

def test_join(queryable):
    orders = [
        {"id": 1, "customer_id": 1, "total": 100},
        {"id": 2, "customer_id": 1, "total": 200},
        {"id": 3, "customer_id": 2, "total": 300},
        {"id": 4, "customer_id": 3, "total": 400},
        {"id": 5, "customer_id": 4, "total": 500}
    ]
    result = queryable \
        .join(
            Queryable(orders),
            lambda c: c["id"],
            lambda o: o["customer_id"],
            lambda c, o: (c["name"], o["total"])
        ) \
        .to_list()
    expected_result = [
        ("Alice", 100),
        ("Alice", 200),
        ("Bob", 300),
        ("Charlie", 400),
        ("David", 500)
    ]
    assert sorted(result) == sorted(expected_result)

def test_group_join(queryable):
    orders = [
        {"id": 1, "customer_id": 1, "total": 100},
        {"id": 2, "customer_id": 1, "total": 200},
        {"id": 3, "customer_id": 2, "total": 300},
        {"id": 4, "customer_id": 3, "total": 400},
        {"id": 5, "customer_id": 4, "total": 500}
    ]
    result = queryable \
        .group_join(
            Queryable(orders),
            lambda c: c["id"],
            lambda o: o["customer_id"],
            lambda c, os: (c["name"], os.select(lambda o: o["total"]).sum())
        ) \
        .to_list()
    assert result == [
        ("Alice", 300),
        ("Bob", 300),
        ("Charlie", 400),
        ("David", 500),
        ("Eve", 0)
    ]

def test_zip(queryable):
    ages = [25, 30, 35, 40, 45]
    result = queryable \
        .zip(
            Queryable(ages),
            lambda c, a: (c["name"], a)
        ) \
        .to_list()
    assert result == [
        ("Alice", 25),
        ("Bob", 30),
        ("Charlie", 35),
        ("David", 40),
        ("Eve", 45)
    ]

def test_concat_union_intersect_except(queryable):
    other_data = [
        {"id": 4, "name": "David", "age": 40, "city": "New York"},
        {"id": 5, "name": "Eve", "age": 45, "city": "London"},
        {"id": 6, "name": "Frank", "age": 50, "city": "Paris"}
    ]
    other_queryable = Queryable(other_data)

    result = queryable \
        .concat(other_queryable) \
        .select(lambda x: x["name"]) \
        .to_list()
    assert result == ["Alice", "Bob", "Charlie", "David", "Eve", "David", "Eve", "Frank"]

    result = queryable \
        .union(other_queryable) \
        .select(lambda x: x["name"]) \
        .to_list()
    assert result == ["Alice", "Bob", "Charlie", "David", "Eve", "Frank"]

    result = queryable \
        .intersect(other_queryable) \
        .select(lambda x: x["name"]) \
        .to_list()
    assert result == ["David", "Eve"]

    result = queryable \
        .except_(other_queryable) \
        .select(lambda x: x["name"]) \
        .to_list()
    assert result == ["Alice", "Bob", "Charlie"]

def test_sequence_equal(queryable):
    other_data = [
        {"id": 1, "name": "Alice", "age": 25, "city": "New York"},
        {"id": 2, "name": "Bob", "age": 30, "city": "London"},
        {"id": 3, "name": "Charlie", "age": 35, "city": "Paris"},
        {"id": 4, "name": "David", "age": 40, "city": "New York"},
        {"id": 5, "name": "Eve", "age": 45, "city": "London"}
    ]
    other_queryable = Queryable(other_data)

    assert queryable.sequence_equal(other_queryable)

    other_data[0]["age"] = 26
    other_queryable = Queryable(other_data)

    assert not queryable.sequence_equal(other_queryable)

def test_any_all_contains(queryable, data):
    assert queryable.any(lambda x: x["age"] > 40)
    assert not queryable.all(lambda x: x["age"] > 40)
    assert queryable.contains(data[0])

def test_min_max_sum_average(queryable):
    assert queryable.min(lambda x: x["age"]) == 25
    assert queryable.max(lambda x: x["age"]) == 45
    assert queryable.sum(lambda x: x["age"]) == 175
    assert queryable.average(lambda x: x["age"]) == 35

def test_to_dict_set_tuple(queryable):
    result = queryable \
        .to_dict(lambda x: x["id"], lambda x: x["name"])
    assert result == {
        1: "Alice",
        2: "Bob",
        3: "Charlie",
        4: "David",
        5: "Eve"
    }

    result = queryable \
        .select(lambda x: x["city"]) \
        .to_set()
    assert result == {"New York", "London", "Paris"}

    result = queryable \
        .select(lambda x: x["name"]) \
        .to_tuple()
    assert result == ("Alice", "Bob", "Charlie", "David", "Eve")

def test_grouping_methods():
    data = [1, 2, 3, 4, 5]
    grouping = Grouping("key", data)

    assert grouping.count() == 5
    assert grouping.sum() == 15
    assert grouping.average() == 3
    assert grouping.min() == 1
    assert grouping.max() == 5
    assert grouping.select(lambda x: x * 2).to_list() == [2, 4, 6, 8, 10]
    assert grouping.any(lambda x: x > 3)
    assert not grouping.all(lambda x: x > 3)
    assert grouping.contains(3)