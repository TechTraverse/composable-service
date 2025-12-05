# Introduction

This library is a Python implementation of
[Finagle](https://twitter.github.io/finagle/)'s [`Service` and `Filter`
abstractions](https://twitter.github.io/finagle/guide/ServicesAndFilters.html).

# Purpose

The ability to compose methods is generically useful, and it's nice to have a
common dependency that creates this abstraction.

# Usage

## Create a `Service`

A `Service[A, B]` is a `Callabe[[A], Awaitable[B]]`, generally implemented
like:

```python
async def my_function(inp: A) -> B:
    # Do some awaitable work
    result = await ...
    return result
```

A `Filter[A, B, C, D]` composes logic with a service. A `Filter` is a
decorator/transformer on a service. Composing a `Filter[A, B, C, D]` with a
`Service[C, D]` returns a new `Service[A, B].`

A `SimpleFilter[A, B]` is represents the common case of a decorator that does
not transform the input or output in anyway. A `SimpleFilter[A, B]` is a
`Filter[A, B, A, B]`.

A `Filter` might implement timeout logic on a service, add observability to it,
map between domain types, etc.

A `Filter` is additionally composable with another `Filter`. Composing a
`Filter[A, B, C, D]` with a `Filter[C, D, E, F]` returns a new `Filter[A, B, E,
F]`. That `Filter[A, B, E, F]` could then be composed with another with a
`Service[E, F]` to produce a new `Service[A, B]`. (Of course, the `Filter[A, B,
E, F]` could also be composed with another `Filter[E, F, _, _]`.)

A `Filter` gets composed with another `Filter` using the `.and_then` method. A
`Filter` gets composed with a `Service` with the `.and_then_service` method.

For concrete examples of:
* building services,
* building filters,
* composing filters with services, and
* composing filters with other filters

see: `tests/simple_service_compose/test_service.py`.

# Known Limitations

* In an idealized version (and in the Finagle implementation), filters would
compose with servcie using the same `.and_then` method name that is used to
compose with other filters. Pull requests and issues are welcome.
* The original Finagle implementaion is careful to declare the generic types on
`Filter` as contravariant or covariant, as applicable. We haven't found this to
be necessary for our use cases yet. Pull requests and issues are welcome.
