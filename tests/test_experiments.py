from dataclasses import dataclass

import pytest


def test_eq_and_frozen_generates_hash() -> None:
    @dataclass(frozen=True)
    class Frozen:
        x: int

    assert Frozen.__hash__ is not None
    assert hash(Frozen(1)) == hash(Frozen(1))


def test_eq_without_frozen_removes_hash() -> None:
    @dataclass
    class NotFrozen:
        x: int

    assert NotFrozen.__hash__ is None


def test_eq_false_keeps_object_hash() -> None:
    @dataclass(eq=False)
    class NoEq:
        x: int

    assert NoEq.__hash__ is object.__hash__
    assert NoEq(1) != NoEq(1)


def test_plain_class_with_eq_but_no_hash_is_unhashable() -> None:
    class Plain:
        def __init__(self, x: int) -> None:
            self.x = x

        def __eq__(self, other: object) -> bool:
            return isinstance(other, Plain) and self.x == other.x

    assert Plain.__hash__ is None
    with pytest.raises(TypeError):
        hash(Plain(1))


def test_unsafe_hash_breaks_lookup_after_change() -> None:
    @dataclass(unsafe_hash=True)
    class Box:
        x: int

    box = Box(1)
    seen = {box}
    box.x = 2
    assert box not in seen
    assert box in list(seen)


def test_unsafe_hash_cannot_hash_a_list_field() -> None:
    @dataclass(unsafe_hash=True)
    class WithList:
        items: list[int]

    with pytest.raises(TypeError):
        hash(WithList([1]))
