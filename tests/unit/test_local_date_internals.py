"""Unit tests for LocalDate internal helper classes."""

import pytest

from dbt.adapters.hologres.local_date import _CallableInt, DualAccessor


class TestCallableInt:
    """Test _CallableInt class."""

    def test_callable_int_value(self):
        """Test _CallableInt stores integer value."""
        ci = _CallableInt(42)
        assert int(ci) == 42
        assert ci == 42

    def test_callable_int_call(self):
        """Test _CallableInt is callable and returns value."""
        ci = _CallableInt(42)
        result = ci()
        assert result == 42
        assert isinstance(result, int)

    def test_callable_int_arithmetic(self):
        """Test _CallableInt can participate in arithmetic."""
        ci = _CallableInt(10)

        assert ci + 5 == 15
        assert ci - 3 == 7
        assert ci * 2 == 20
        assert ci / 2 == 5

    def test_callable_int_comparison(self):
        """Test _CallableInt can be compared."""
        ci = _CallableInt(10)

        assert ci > 5
        assert ci < 20
        assert ci == 10
        assert ci >= 10
        assert ci <= 10

    def test_callable_int_str(self):
        """Test _CallableInt string representation."""
        ci = _CallableInt(42)
        assert str(ci) == "42"
        assert int(ci) == 42

    def test_callable_int_repr(self):
        """Test _CallableInt repr."""
        ci = _CallableInt(42)
        assert repr(ci) == "42"

    def test_callable_int_zero(self):
        """Test _CallableInt with zero value."""
        ci = _CallableInt(0)
        assert ci == 0
        assert ci() == 0

    def test_callable_int_negative(self):
        """Test _CallableInt with negative value."""
        ci = _CallableInt(-5)
        assert ci == -5
        assert ci() == -5

    def test_callable_int_large_value(self):
        """Test _CallableInt with large value."""
        ci = _CallableInt(1000000)
        assert ci == 1000000
        assert ci() == 1000000


class TestDualAccessor:
    """Test DualAccessor descriptor class."""

    def test_attribute_access(self):
        """Test DualAccessor allows attribute access."""
        class TestClass:
            @DualAccessor
            def value(self):
                return 42

        obj = TestClass()
        # Access as attribute
        assert obj.value == 42

    def test_method_call(self):
        """Test DualAccessor allows method call."""
        class TestClass:
            @DualAccessor
            def value(self):
                return 42

        obj = TestClass()
        # Access as method
        assert obj.value() == 42

    def test_both_access_same_result(self):
        """Test both access methods return same value."""
        class TestClass:
            @DualAccessor
            def value(self):
                return 42

        obj = TestClass()
        assert obj.value == obj.value()

    def test_docstring_preserved(self):
        """Test DualAccessor preserves docstring."""
        class TestClass:
            @DualAccessor
            def value(self):
                """This is the value."""
                return 42

        obj = TestClass()
        assert obj.value.__doc__ == "This is the value."

    def test_name_preserved(self):
        """Test DualAccessor preserves __name__."""
        class TestClass:
            @DualAccessor
            def my_property(self):
                """A property."""
                return 100

        obj = TestClass()
        assert obj.my_property.__name__ == "my_property"

    def test_class_access_returns_descriptor(self):
        """Test accessing from class returns descriptor instance."""
        class TestClass:
            @DualAccessor
            def value(self):
                return 42

        # Access from class (not instance) should return descriptor
        descriptor = TestClass.value
        assert isinstance(descriptor, DualAccessor)

    def test_returns_callable_int(self):
        """Test DualAccessor returns _CallableInt instance."""
        class TestClass:
            @DualAccessor
            def value(self):
                return 42

        obj = TestClass()
        result = obj.value
        assert isinstance(result, _CallableInt)

    def test_with_calculated_value(self):
        """Test DualAccessor with calculated value."""
        class TestClass:
            def __init__(self, x):
                self._x = x

            @DualAccessor
            def doubled(self):
                return self._x * 2

        obj = TestClass(10)
        assert obj.doubled == 20
        assert obj.doubled() == 20

    def test_dynamic_value(self):
        """Test DualAccessor with dynamic value."""
        class Counter:
            def __init__(self):
                self._count = 0

            @DualAccessor
            def count(self):
                return self._count

        counter = Counter()
        assert counter.count == 0

        counter._count = 5
        assert counter.count == 5
        assert counter.count() == 5

    def test_with_different_types(self):
        """Test DualAccessor with different return types."""
        class TestClass:
            @DualAccessor
            def int_value(self):
                return 42

        obj = TestClass()
        result = obj.int_value

        # Should be int (via _CallableInt which inherits from int)
        assert isinstance(result, int)
        assert isinstance(result, _CallableInt)
