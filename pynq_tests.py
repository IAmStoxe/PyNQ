import unittest
from pynq import Queryable, Grouping

class TestQueryable(unittest.TestCase):
    def setUp(self):
        self.data = [
            {"id": 1, "name": "Alice", "age": 25, "city": "New York"},
            {"id": 2, "name": "Bob", "age": 30, "city": "London"},
            {"id": 3, "name": "Charlie", "age": 35, "city": "Paris"},
            {"id": 4, "name": "David", "age": 40, "city": "New York"},
            {"id": 5, "name": "Eve", "age": 45, "city": "London"}
        ]
        self.queryable = Queryable(self.data)

    def test_where_select(self):
        result = self.queryable \
            .where(lambda x: x["age"] > 30) \
            .select(lambda x: x["name"]) \
            .to_list()
        self.assertEqual(result, ["Charlie", "David", "Eve"])


    def test_group_by_having(self):
        result = self.queryable \
            .group_by(lambda x: x["city"]) \
            .where(lambda g: g.count() > 1) \
            .select(lambda g: (g.key, g.count())) \
            .to_list()
        self.assertEqual(result, [("New York", 2), ("London", 2)])

    def test_distinct_count(self):
        result = self.queryable \
            .select(lambda x: x["city"]) \
            .distinct() \
            .count()
        self.assertEqual(result, 3)

    def test_first_single_last(self):
        result = self.queryable \
            .where(lambda x: x["city"] == "London") \
            .order_by(lambda x: x["age"]) \
            .first()
        self.assertEqual(result["name"], "Bob")

        result = self.queryable \
            .where(lambda x: x["city"] == "Paris") \
            .single()
        self.assertEqual(result["name"], "Charlie")

        result = self.queryable \
            .where(lambda x: x["city"] == "New York") \
            .order_by(lambda x: x["age"]) \
            .last()
        self.assertEqual(result["name"], "David")

    def test_skip_take(self):
        result = self.queryable \
            .order_by(lambda x: x["age"]) \
            .skip(2) \
            .take(2) \
            .select(lambda x: x["name"]) \
            .to_list()
        self.assertEqual(result, ["Charlie", "David"])

    def test_aggregate(self):
        result = self.queryable \
            .select(lambda x: x["age"]) \
            .aggregate(0, lambda acc, x: acc + x)
        self.assertEqual(result, 175)

    def test_join(self):
        orders = [
            {"id": 1, "customer_id": 1, "total": 100},
            {"id": 2, "customer_id": 1, "total": 200},
            {"id": 3, "customer_id": 2, "total": 300},
            {"id": 4, "customer_id": 3, "total": 400},
            {"id": 5, "customer_id": 4, "total": 500}
        ]
        result = self.queryable \
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
        self.assertEqual(sorted(result), sorted(expected_result))

    def test_group_join(self):
        orders = [
            {"id": 1, "customer_id": 1, "total": 100},
            {"id": 2, "customer_id": 1, "total": 200},
            {"id": 3, "customer_id": 2, "total": 300},
            {"id": 4, "customer_id": 3, "total": 400},
            {"id": 5, "customer_id": 4, "total": 500}
        ]
        result = self.queryable \
            .group_join(
                Queryable(orders),
                lambda c: c["id"],
                lambda o: o["customer_id"],
                lambda c, os: (c["name"], os.select(lambda o: o["total"]).sum())
            ) \
            .to_list()
        self.assertEqual(result, [
            ("Alice", 300),
            ("Bob", 300),
            ("Charlie", 400),
            ("David", 500),
            ("Eve", 0)
        ])

    def test_zip(self):
        ages = [25, 30, 35, 40, 45]
        result = self.queryable \
            .zip(
                Queryable(ages),
                lambda c, a: (c["name"], a)
            ) \
            .to_list()
        self.assertEqual(result, [
            ("Alice", 25),
            ("Bob", 30),
            ("Charlie", 35),
            ("David", 40),
            ("Eve", 45)
        ])

    def test_concat_union_intersect_except(self):
        other_data = [
            {"id": 4, "name": "David", "age": 40, "city": "New York"},
            {"id": 5, "name": "Eve", "age": 45, "city": "London"},
            {"id": 6, "name": "Frank", "age": 50, "city": "Paris"}
        ]
        other_queryable = Queryable(other_data)

        result = self.queryable \
            .concat(other_queryable) \
            .select(lambda x: x["name"]) \
            .to_list()
        self.assertEqual(result, ["Alice", "Bob", "Charlie", "David", "Eve", "David", "Eve", "Frank"])

        result = self.queryable \
            .union(other_queryable) \
            .select(lambda x: x["name"]) \
            .to_list()
        self.assertEqual(result, ["Alice", "Bob", "Charlie", "David", "Eve", "Frank"])

        result = self.queryable \
            .intersect(other_queryable) \
            .select(lambda x: x["name"]) \
            .to_list()
        self.assertEqual(result, ["David", "Eve"])

        result = self.queryable \
            .except_(other_queryable) \
            .select(lambda x: x["name"]) \
            .to_list()
        self.assertEqual(result, ["Alice", "Bob", "Charlie"])

    def test_sequence_equal(self):
        other_data = [
            {"id": 1, "name": "Alice", "age": 25, "city": "New York"},
            {"id": 2, "name": "Bob", "age": 30, "city": "London"},
            {"id": 3, "name": "Charlie", "age": 35, "city": "Paris"},
            {"id": 4, "name": "David", "age": 40, "city": "New York"},
            {"id": 5, "name": "Eve", "age": 45, "city": "London"}
        ]
        other_queryable = Queryable(other_data)

        self.assertTrue(self.queryable.sequence_equal(other_queryable))

        other_data[0]["age"] = 26
        other_queryable = Queryable(other_data)

        self.assertFalse(self.queryable.sequence_equal(other_queryable))

    def test_any_all_contains(self):
        self.assertTrue(self.queryable.any(lambda x: x["age"] > 40))
        self.assertFalse(self.queryable.all(lambda x: x["age"] > 40))
        self.assertTrue(self.queryable.contains(self.data[0]))

    def test_min_max_sum_average(self):
        self.assertEqual(self.queryable.min(lambda x: x["age"]), 25)
        self.assertEqual(self.queryable.max(lambda x: x["age"]), 45)
        self.assertEqual(self.queryable.sum(lambda x: x["age"]), 175)
        self.assertEqual(self.queryable.average(lambda x: x["age"]), 35)

    def test_to_dict_set_tuple(self):
        result = self.queryable \
            .to_dict(lambda x: x["id"], lambda x: x["name"])
        self.assertEqual(result, {
            1: "Alice",
            2: "Bob",
            3: "Charlie",
            4: "David",
            5: "Eve"
        })

        result = self.queryable \
            .select(lambda x: x["city"]) \
            .to_set()
        self.assertEqual(result, {"New York", "London", "Paris"})

        result = self.queryable \
            .select(lambda x: x["name"]) \
            .to_tuple()
        self.assertEqual(result, ("Alice", "Bob", "Charlie", "David", "Eve"))

class TestGrouping(unittest.TestCase):
    def test_grouping_methods(self):
        data = [1, 2, 3, 4, 5]
        grouping = Grouping("key", data)

        self.assertEqual(grouping.count(), 5)
        self.assertEqual(grouping.sum(), 15)
        self.assertEqual(grouping.average(), 3)
        self.assertEqual(grouping.min(), 1)
        self.assertEqual(grouping.max(), 5)
        self.assertEqual(grouping.select(lambda x: x * 2).to_list(), [2, 4, 6, 8, 10])
        self.assertTrue(grouping.any(lambda x: x > 3))
        self.assertFalse(grouping.all(lambda x: x > 3))
        self.assertTrue(grouping.contains(3))

if __name__ == '__main__':
    unittest.main()