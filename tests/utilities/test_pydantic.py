import json

import pydantic
import pytest
from typing_extensions import Literal

from prefect.utilities.dispatch import register_type
from prefect.utilities.pydantic import PartialModel, add_type_dispatch


class SimplePydantic(pydantic.BaseModel):
    x: int
    y: int


class TestPartialModel:
    def test_init(self):
        p = PartialModel(SimplePydantic)
        assert p.model_cls == SimplePydantic
        assert p.fields == {}

    def test_init_with_fields(self):
        p = PartialModel(SimplePydantic, x=1, y=2)
        assert p.fields == {"x": 1, "y": 2}
        m = p.finalize()
        assert isinstance(m, SimplePydantic)
        assert m == SimplePydantic(x=1, y=2)

    def test_init_with_invalid_field(self):
        with pytest.raises(ValueError, match="Field 'z' is not present in the model"):
            PartialModel(SimplePydantic, x=1, z=2)

    def test_set_attribute(self):
        p = PartialModel(SimplePydantic)
        p.x = 1
        p.y = 2
        assert p.finalize() == SimplePydantic(x=1, y=2)

    def test_set_invalid_attribute(self):
        p = PartialModel(SimplePydantic)
        with pytest.raises(ValueError, match="Field 'z' is not present in the model"):
            p.z = 1

    def test_set_already_set_attribute(self):
        p = PartialModel(SimplePydantic, x=1)
        with pytest.raises(ValueError, match="Field 'x' has already been set"):
            p.x = 2

    def test_finalize_with_fields(self):
        p = PartialModel(SimplePydantic)
        m = p.finalize(x=1, y=2)
        assert isinstance(m, SimplePydantic)
        assert m == SimplePydantic(x=1, y=2)

    def test_finalize_with_invalid_field(self):
        p = PartialModel(SimplePydantic)
        with pytest.raises(ValueError, match="Field 'z' is not present in the model"):
            p.finalize(z=1)

    def test_finalize_with_already_set_field(self):
        p = PartialModel(SimplePydantic, x=1)
        with pytest.raises(ValueError, match="Field 'x' has already been set"):
            p.finalize(x=1)

    def test_finalize_with_missing_field(self):
        p = PartialModel(SimplePydantic, x=1)
        with pytest.raises(ValueError, match="validation error"):
            p.finalize()


class TestTypeDispatchField:
    def test_base_can_be_resolved_to_self(self):
        @add_type_dispatch
        class Base(pydantic.BaseModel):
            __dispatch_key__ = "__base__"

        instance = Base()
        post_instance = Base.parse_raw(instance.json())
        assert type(post_instance) == Base

    def test_subtype_dict_contains_dispatch_key(self):
        @add_type_dispatch
        class Base(pydantic.BaseModel):
            __dispatch_key__ = "base"

        @register_type
        class Foo(Base):
            __dispatch_key__ = "foo"

        assert Foo().dict().get("type") == "foo"

    def test_subtype_json_contains_dispatch_key(self):
        @add_type_dispatch
        class Base(pydantic.BaseModel):
            __dispatch_key__ = "base"

        @register_type
        class Foo(Base):
            __dispatch_key__ = "foo"

        assert json.loads(Foo().json()).get("type") == "foo"

    def test_subtype_can_be_resolved_with_base_parse(self):
        @add_type_dispatch
        class Base(pydantic.BaseModel):
            __dispatch_key__ = "base"

        @register_type
        class Foo(Base):
            __dispatch_key__ = "foo"

        instance = Foo()
        post_instance = Base.parse_raw(instance.json())
        assert type(post_instance) == Foo
        assert isinstance(post_instance, Foo)
        assert isinstance(post_instance, Base)

    def test_subtype_can_be_resolved_with_subtype_parse(self):
        @add_type_dispatch
        class Base(pydantic.BaseModel):
            __dispatch_key__ = "base"

        @register_type
        class Foo(Base):
            __dispatch_key__ = "foo"

        instance = Foo()
        post_instance = Foo.parse_raw(instance.json())
        assert type(post_instance) == Foo
        assert isinstance(post_instance, Foo)
        assert isinstance(post_instance, Base)

    def test_subtype_with_callable_dispatch_key_can_be_resolved_with_base_parse(self):
        @add_type_dispatch
        class Base(pydantic.BaseModel):
            @classmethod
            def __dispatch_key__(cls):
                return cls.__name__.lower()

        @register_type
        class Foo(Base):
            pass

        instance = Foo()
        post_instance = Foo.parse_raw(instance.json())
        assert type(post_instance) == Foo
        assert isinstance(post_instance, Foo)
        assert isinstance(post_instance, Base)

    def test_subtypes_are_unique(self):
        @add_type_dispatch
        class Base(pydantic.BaseModel):
            @classmethod
            def __dispatch_key__(cls):
                return cls.__name__.lower()

        @register_type
        class Foo(Base):
            pass

        @register_type
        class Bar(Base):
            pass

        foo = Foo()
        bar = Bar()
        post_foo = Base.parse_raw(foo.json())
        post_bar = Base.parse_raw(bar.json())
        assert type(post_foo) != type(post_bar)

    def test_registries_are_unique_per_base(self):
        @add_type_dispatch
        class Base(pydantic.BaseModel):
            @classmethod
            def __dispatch_key__(cls):
                return cls.__name__.lower()

        @register_type
        class Foo(Base):
            pass

        @add_type_dispatch
        class SecondBase(pydantic.BaseModel):
            @classmethod
            def __dispatch_key__(cls):
                return cls.__name__.lower()

        @register_type
        class SecondFoo(SecondBase):
            pass

        foo = Foo()
        second_foo = SecondFoo()
        post_foo = Base.parse_raw(foo.json())
        post_second_foo = SecondBase.parse_raw(second_foo.json())
        assert type(post_foo) != type(post_second_foo)
        assert isinstance(post_foo, Foo)
        assert isinstance(post_second_foo, SecondFoo)

    def test_unregistered_subtype_cannot_be_resolved_with_base_parse(self):
        @add_type_dispatch
        class Base(pydantic.BaseModel):
            __dispatch_key__: str

        class Foo(Base):
            __dispatch_key__ = "foo"

        instance = Foo()
        with pytest.raises(
            KeyError, match="No class found in registry for dispatch key 'foo'."
        ):
            Base.parse_raw(instance.json())

    def test_register_type_without_known_base(self):
        class DispatchlessBase(pydantic.BaseModel):
            pass

        with pytest.raises(ValueError, match="No registry found for type 'Foo'"):

            @register_type
            class Foo(DispatchlessBase):
                pass

    def test_register_type_without_dispatch_key(self):
        @add_type_dispatch
        class Base(pydantic.BaseModel):
            __dispatch_key__: str

        with pytest.raises(
            ValueError,
            match="Type 'Foo' does not define a value for '__dispatch_key__' which is required for registry lookup",
        ):

            @register_type
            class Foo(Base):
                pass

    def test_type_field_can_be_used_directly(self):
        @add_type_dispatch
        class Base(pydantic.BaseModel):
            type: str

        @register_type
        class Foo(Base):
            type: Literal["foo"] = "foo"

        assert Foo.__dispatch_key__() == "foo"

        instance = Foo()
        post_instance = Base.parse_raw(instance.json())
        assert type(post_instance) == Foo
        assert isinstance(post_instance, Foo)
        assert isinstance(post_instance, Base)

    def test_both_type_field_and_dispatch_key_cannot_be_set(self):

        with pytest.raises(
            ValueError,
            match="Model class 'Base' defines a `__dispatch_key__` and a type field. Only one of these may be defined for dispatch",
        ):

            @add_type_dispatch
            class Base(pydantic.BaseModel):
                type: str
                __dispatch_key__ = "base"

    def test_base_type_field_must_be_string_type(self):

        with pytest.raises(
            TypeError,
            match="Model class 'Base' defines a 'type' field with type 'int' but it must be 'str'",
        ):

            @add_type_dispatch
            class Base(pydantic.BaseModel):
                type: int