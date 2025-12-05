from abc import abstractmethod
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Protocol

"""`Service[Input, Output]`: `Input` -> `Output`.

A Service is an abstraction that represents an awaitable function that takes
one argument and returns one result.

A `Service[A, B]` takes an input of type `A` and returns an `Awaitable` of type
`B`.
"""
type Service[Input, Output] = Callable[[Input], Awaitable[Output]]


class Filter[FilterIn, FilterOut, ServiceIn, ServiceOut](Protocol):
    """`Filter` is a Decorator/Transformer on a `Service`.

    `Filter[FilterIn, FilterOut, ServiceIn, ServiceOut]` =
        FilterIn -> (ServiceIn -> ServiceOut) -> FilterOut

    Put differently, a Filter[A, B, C, D] transforms a `Service[C, D]`
    into a `Service[A, B]`.
    """

    @abstractmethod
    async def __call__(self, inp: FilterIn, service: Service[ServiceIn, ServiceOut]) -> FilterOut:
        """`Filter` implementation.

        Args:
            inp (FilterIn): The input to the `Filter`.
            service: (Service[ServiceIn, Out]): The service the `Filter`
                should call.

        Returns:
            Result of a call composing the filter with the service.
        """

    def and_then[NextIn, NextOut](
        self,
        next_filter: "Filter[ServiceIn, ServiceOut, NextIn, NextOut]",
    ) -> "Filter[FilterIn, FilterOut, NextIn, NextOut]":
        """Compose a filter with another filter.

        A `Filter[A, B, C, D]` composed with a `Filter[C, D, E,
        F]`, results in a `Filter[A, B, E, F]`.
        """

        def build(
            service: Service[NextIn, NextOut],
        ) -> Service[FilterIn, FilterOut]:
            """Build the eventual composed service when it's provided."""
            return self.and_then_service(next_filter.and_then_service(service))

        return _AndThen(self, next_filter, build)

    def and_then_service(
        self, service: Service[ServiceIn, ServiceOut]
    ) -> Service[FilterIn, FilterOut]:
        """Compose a service filter with a service to build a new service.

        Composing a `ServiceFilter[A, B, C, D]` with a `Service[C, D]` results
        in a new `Service[A, B]`.

        Args:
            service (Service[ServiceIn, ServiceOut]): The service to compose
                with the filter

        Returns:
            (Service[FilterIn, FilterOut]): The service composed with the
                filter
        """
        return _Adapter(self, service)


class SimpleFilter[In, Out](Filter[In, Out, In, Out]):
    """SimpleFilter is a Decorator.

    A SimpleFilter adds logic around a `Service[A, B]` to produce a new
    `Service[A, B]`.
    """


@dataclass()
class _AndThen[FilterIn, FilterOut, ServiceIn, ServiceOut](
    Filter[FilterIn, FilterOut, ServiceIn, ServiceOut]
):
    """Private class to build filter composition.

    This class is, itself, a filter. It's used to create a filter by composing
    2 filters.

    Library users should expect to use the `Filter` type rather than this type.
    """

    first: Filter[Any, Any, Any, Any]
    next: Filter[Any, Any, Any, Any]
    build: Callable[[Service[ServiceIn, ServiceOut]], Service[FilterIn, FilterOut]]

    async def __call__(self, inp: FilterIn, service: Service[ServiceIn, ServiceOut]) -> FilterOut:
        transformed_service = self.build(service)
        return await transformed_service(inp)


class _Adapter[FilterIn, FilterOut, ServiceIn, ServiceOut]:
    """Private class to compose a filter with a service.

    This class is, itself, a `Service[FilterIn, FilterOut]`. It's used to
    create a service by composing a filter with a service.

    Library users should expect to use the `Service` type rather than this
    type.
    """

    filter: Filter[FilterIn, FilterOut, ServiceIn, ServiceOut]
    service: Service[ServiceIn, ServiceOut]

    def __init__(
        self,
        filter: Filter[FilterIn, FilterOut, ServiceIn, ServiceOut],
        service: Service[ServiceIn, ServiceOut],
    ) -> None:
        """Initializer."""
        self.filter = filter
        self.service = service

    async def __call__(self, inp: FilterIn) -> FilterOut:
        """Execute a `ServiceFilter` composed with a `Service`."""
        return await self.filter(inp, self.service)
