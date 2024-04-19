from collections import defaultdict
from itertools import islice
from typing import Callable, Iterable, TypeVar, Generic, Dict, List, Optional, Any, Union

T = TypeVar('T')
TKey = TypeVar('TKey')
TResult = TypeVar('TResult')

class Grouping(Generic[TKey, T]):
    def __init__(self, key: TKey, items: List[T]):
        self.key = key
        self._items = items

    def __iter__(self):
        return iter(self._items)

    def __len__(self):
        return len(self._items)

    def __getitem__(self, index):
        return self._items[index]

    def count(self):
        return len(self._items)

    def sum(self, selector: Optional[Callable[[T], Union[int, float]]] = None):
        if selector is None:
            return sum(self._items)
        return sum(selector(item) for item in self._items)

    def average(self, selector: Optional[Callable[[T], Union[int, float]]] = None):
        if selector is None:
            return sum(self._items) / len(self._items)
        return sum(selector(item) for item in self._items) / len(self._items)

    def min(self, selector: Optional[Callable[[T], Any]] = None):
        if selector is None:
            return min(self._items)
        return min(selector(item) for item in self._items)

    def max(self, selector: Optional[Callable[[T], Any]] = None):
        if selector is None:
            return max(self._items)
        return max(selector(item) for item in self._items)

    def select(self, selector: Callable[[T], TResult]) -> 'Queryable[TResult]':
        return Queryable(selector(item) for item in self._items)

    def any(self, predicate: Optional[Callable[[T], bool]] = None) -> bool:
        if predicate is None:
            return any(self._items)
        return any(predicate(item) for item in self._items)

    def all(self, predicate: Callable[[T], bool]) -> bool:
        return all(predicate(item) for item in self._items)

    def contains(self, value: T) -> bool:
        return value in self._items

class Queryable(Generic[T]):
    def __init__(self, iterable: Iterable[T]):
        self._iterable = iterable

    def where(self, predicate: Callable[[T], bool]) -> 'Queryable[T]':
        return Queryable(item for item in self._iterable if predicate(item))

    def select(self, selector: Callable[[T], TResult]) -> 'Queryable[TResult]':
        return Queryable(selector(item) for item in self._iterable)

    def order_by(self, key_selector: Callable[[T], TResult]) -> 'Queryable[T]':
        return Queryable(sorted(self._iterable, key=key_selector))

    def order_by_descending(self, key_selector: Callable[[T], TResult]) -> 'Queryable[T]':
        return Queryable(sorted(self._iterable, key=key_selector, reverse=True))

    
    def first(self) -> T:
        return next(iter(self._iterable))

    def first_or_default(self, default: T) -> T:
        return next(iter(self._iterable), default)

    def last(self) -> T:
        return next(reversed(list(self._iterable)))

    def last_or_default(self, default: T) -> T:
        iterable = list(self._iterable)
        return iterable[-1] if iterable else default

    def element_at(self, index: int) -> T:
        return list(self._iterable)[index]

    def element_at_or_default(self, index: int, default: T) -> T:
        iterable = list(self._iterable)
        return iterable[index] if 0 <= index < len(iterable) else default

    def count(self) -> int:
        return sum(1 for _ in self._iterable)

    def distinct(self) -> 'Queryable[T]':
        seen = set()
        return Queryable(item for item in self._iterable if repr(item) not in seen and not seen.add(repr(item)))

    def group_by(self, key_selector: Callable[[T], TKey]) -> 'Queryable[Grouping[TKey, T]]':
        groups: Dict[TKey, List[T]] = {}
        for item in self._iterable:
            key = key_selector(item)
            if key not in groups:
                groups[key] = []
            groups[key].append(item)
        return Queryable(Grouping(key, items) for key, items in groups.items())

    def any(self, predicate: Optional[Callable[[T], bool]] = None) -> bool:
        if predicate is None:
            return any(self._iterable)
        return any(predicate(item) for item in self._iterable)

    def all(self, predicate: Callable[[T], bool]) -> bool:
        return all(predicate(item) for item in self._iterable)

    def contains(self, value: T) -> bool:
        return value in self._iterable

    def skip(self, count: int) -> 'Queryable[T]':
        return Queryable(islice(self._iterable, count, None))

    def take(self, count: int) -> 'Queryable[T]':
        return Queryable(islice(self._iterable, count))

    def to_set(self) -> set[T]:
        return set(self._iterable)

    def skip_while(self, predicate: Callable[[T], bool]) -> 'Queryable[T]':
        return Queryable(item for item in self._iterable if not predicate(item))

    def take_while(self, predicate: Callable[[T], bool]) -> 'Queryable[T]':
        return Queryable(item for item in self._iterable if predicate(item))

    def aggregate(self, seed: TResult, func: Callable[[TResult, T], TResult]) -> TResult:
        result = seed
        for item in self._iterable:
            result = func(result, item)
        return result

    def select_many(self, selector: Callable[[T], Iterable[TResult]]) -> 'Queryable[TResult]':
        return Queryable(item for sublist in self._iterable for item in selector(sublist))

    def join(self, other: 'Queryable[TResult]', key_selector: Callable[[T], TKey], other_key_selector: Callable[[TResult], TKey], result_selector: Callable[[T, TResult], Any]) -> 'Queryable[Any]':
        lookup = defaultdict(list)
        for item in other._iterable:
            lookup[other_key_selector(item)].append(item)
        return Queryable(result_selector(item, match) for item in self._iterable for match in lookup[key_selector(item)])


    def group_join(self, other: 'Queryable[TResult]', key_selector: Callable[[T], TKey], other_key_selector: Callable[[TResult], TKey], result_selector: Callable[[T, 'Queryable[TResult]'], Any]) -> 'Queryable[Any]':
        lookup: Dict[TKey, List[TResult]] = {}
        for item in other._iterable:
            key = other_key_selector(item)
            if key not in lookup:
                lookup[key] = []
            lookup[key].append(item)
        return Queryable(result_selector(item, Queryable(lookup.get(key_selector(item), []))) for item in self._iterable)

    def zip(self, other: 'Queryable[TResult]', result_selector: Callable[[T, TResult], Any]) -> 'Queryable[Any]':
        return Queryable(result_selector(item1, item2) for item1, item2 in zip(self._iterable, other._iterable))

    def concat(self, other: 'Queryable[T]') -> 'Queryable[T]':
        return Queryable(item for iterable in (self._iterable, other._iterable) for item in iterable)

    def union(self, other: 'Queryable[T]') -> 'Queryable[T]':
        return self.concat(other).distinct()

    def intersect(self, other: 'Queryable[T]') -> 'Queryable[T]':
        return Queryable(item for item in self._iterable if any(item == other_item for other_item in other._iterable))

    def except_(self, other: 'Queryable[T]') -> 'Queryable[T]':
        other_set = set(tuple(item.items()) for item in other._iterable)
        return Queryable(item for item in self._iterable if tuple(item.items()) not in other_set)

    def default_if_empty(self, default: T) -> 'Queryable[T]':
        iterable = list(self._iterable)
        return Queryable(iterable if iterable else [default])

    def reverse(self) -> 'Queryable[T]':
        return Queryable(reversed(list(self._iterable)))

    def sequence_equal(self, other: 'Queryable[T]') -> bool:
        return list(self._iterable) == list(other._iterable)

    def single(self) -> T:
        iterable = list(self._iterable)
        if len(iterable) != 1:
            raise ValueError("Sequence contains more than one element")
        return iterable[0]

    def single_or_default(self, default: T) -> T:
        iterable = list(self._iterable)
        if len(iterable) == 1:
            return iterable[0]
        if not iterable:
            return default
        raise ValueError("Sequence contains more than one element")

    def sum(self, selector: Optional[Callable[[T], Union[int, float]]] = None) -> Union[int, float]:
        if selector is None:
            return sum(self._iterable)
        return sum(selector(item) for item in self._iterable)

    def average(self, selector: Optional[Callable[[T], Union[int, float]]] = None) -> float:
        if selector is None:
            return sum(self._iterable) / self.count()
        return sum(selector(item) for item in self._iterable) / self.count()

    def min(self, selector: Optional[Callable[[T], Any]] = None) -> T:
        if selector is None:
            return min(self._iterable)
        return min(selector(item) for item in self._iterable)

    def max(self, selector: Optional[Callable[[T], Any]] = None) -> T:
        if selector is None:
            return max(self._iterable)
        return max(selector(item) for item in self._iterable)

    def to_list(self) -> List[T]:
        return list(self._iterable)

    def to_dict(self, key_selector: Callable[[T], TKey], value_selector: Optional[Callable[[T], TResult]] = None) -> Dict[TKey, TResult]:
        if value_selector is None:
            return {key_selector(item): item for item in self._iterable}
        return {key_selector(item): value_selector(item) for item in self._iterable}

    def to_tuple(self) -> tuple[T, ...]:
        return tuple(self._iterable)