# PyNQ: Python LINQ for the Masochistic and Deranged

[![PyNQ](https://github.com/IAmStoxe/PyNQ/actions/workflows/python-app.yml/badge.svg)](https://github.com/IAmStoxe/PyNQ/actions/workflows/python-app.yml)

Attention all code junkies and complexity addicts! Are you tired of writing code that's readable, maintainable, and easy to understand? Do you crave the adrenaline rush of staring at a screen full of cryptic symbols and wondering what the hell you were thinking? Well, buckle up, buttercup, because PyNQ is here to satisfy all your wildest coding fantasies! 🎢

## Features

- 🧙‍♂️ Unleash your inner wizard with PyNQ's `group_join` method! It's like a regular join, but with a sprinkle of dark magic and a dash of chaos. Watch in awe as your data morphs into a tangled web of mystery!

```python
result = self.queryable \
    .group_join(
        Queryable(orders),
        lambda c: c["id"],
        lambda o: o["customer_id"],
        lambda c, os: (c["name"], os.select(lambda o: o["total"]).sum())
    ) \
    .to_list()
```

Good luck trying to explain this to your rubber duck! 🦆

- 🚀 Blast off into the stratosphere of abstraction with PyNQ's `zip` method! It's like a zip line, but instead of a thrilling ride, you get a one-way ticket to confusion town.

```python
result = self.queryable \
    .zip(
        Queryable(ages),
        lambda c, a: (c["name"], a)
    ) \
    .to_list()
```

Who needs clarity when you can have obscurity? 😎

- 🌀 Get lost in the vortex of nested lambdas with PyNQ's `aggregate` method! It's like a black hole of code, sucking in all your sanity and spitting out pure chaos.

```python
result = self.queryable \
    .select(lambda x: x["age"]) \
    .aggregate(0, lambda acc, x: acc + x)
```

I hope you brought a map, because you're gonna need it! 🗺️

- 🧩 Put your skills to the test with PyNQ's mind-bending `group_by` and `having` methods! It's like a Rubik's cube, but instead of colors, you have data, and instead of solving it, you just make it worse.

```python
result = self.queryable \
    .group_by(lambda x: x["city"]) \
    .where(lambda g: g.count() > 1) \
    .select(lambda g: (g.key, g.count())) \
    .to_list()
```

If you can decipher this, you're either a genius or a masochist. Or both. 🤓

Oh, and if you thought the code was mind-bending, just wait until you see the unit tests! We've got tests for days, covering every nook and cranny of PyNQ's intricate web of chaos. It's like a treasure hunt, but instead of treasure, you find more questions than answers. 💎

```python
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
```

With tests like these, who needs documentation? 📚

So what are you waiting for? Dive headfirst into the rabbit hole of PyNQ and discover a world of coding possibilities you never knew existed! Just don't blame us if you start speaking in lambdas and dreaming in LINQ. 😴

Happy querying, you magnificent mad hatter! And remember, if the code doesn't make you question your sanity, you're not doing it right. 😜

---

Disclaimer: PyNQ is not responsible for any brain damage, existential crises, or spontaneous fits of maniacal laughter that may occur while using this library. Proceed at your own risk, and may the lambdas be ever in your favor! 🙏

It's a joke... Laugh a little. ❤️