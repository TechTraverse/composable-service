from dataclasses import dataclass
import pytest

from simple_service_compose.service import Service, Filter, SimpleFilter


@pytest.mark.asyncio
async def test_can_call_service_as_a_function():
    async def int_to_awaitable_string(inp: int) -> str:
        return str(inp)

    int_to_string_service: Service[int, str] = int_to_awaitable_string

    assert await int_to_string_service(4) == "4"


@pytest.mark.asyncio
async def test_can_compose_simple_filter_with_service():
    async def awaitable_double_int(inp: int) -> int:
        return inp * 2

    class AddOneFilter(SimpleFilter[int, int]):
        async def __call__(self, inp: int, service: Service[int, int]) -> int:
            return await service(inp + 1)

    doubler_service: Service[int, int] = awaitable_double_int
    add_one_filter = AddOneFilter()

    add_one_and_then_double_service = add_one_filter.and_then_service(doubler_service)

    assert await add_one_and_then_double_service(2) == 6


@pytest.mark.asyncio
async def test_can_compose_filter_with_service():
    async def awaitable_double_int(inp: int) -> int:
        return inp * 2

    class AddOneBeforeAndToStringResultFilter(Filter[int, str, int, int]):
        async def __call__(self, inp: int, service: Service[int, int]) -> str:
            return str(await service(inp + 1))

    doubler_service: Service[int, int] = awaitable_double_int
    add_one_and_to_string_result_filter = AddOneBeforeAndToStringResultFilter()

    composed_service = add_one_and_to_string_result_filter.and_then_service(doubler_service)

    assert await composed_service(2) == "6"


@pytest.mark.asyncio
async def test_can_compose_filter_with_function_without_casting():
    async def awaitable_double_int(inp: int) -> int:
        return inp * 2

    class AddOneBeforeAndToStringResultFilter(Filter[int, str, int, int]):
        async def __call__(self, inp: int, service: Service[int, int]) -> str:
            return str(await service(inp + 1))

    add_one_and_to_string_result_filter = AddOneBeforeAndToStringResultFilter()

    composed_service = add_one_and_to_string_result_filter.and_then_service(awaitable_double_int)

    assert await composed_service(2) == "6"


@pytest.mark.asyncio
async def test_can_compose_two_filters_and_then_service():
    async def awaitable_double_int(inp: int) -> int:
        return inp * 2

    class AddOneFilter(SimpleFilter[int, int]):
        async def __call__(self, inp: int, service: Service[int, int]) -> int:
            return await service(inp + 1)

    class OutputToStringfilter(Filter[int, str, int, int]):
        async def __call__(self, inp: int, service: Service[int, int]) -> str:
            return str(await service(inp))

    doubler_service: Service[int, int] = awaitable_double_int

    add_one_filter = AddOneFilter()
    output_to_string_filter = OutputToStringfilter()

    composed_filter = output_to_string_filter.and_then(add_one_filter)
    composed_service = composed_filter.and_then_service(doubler_service)

    assert await composed_service(2) == "6"


@pytest.mark.asyncio
async def test_can_compose_filter_with_service_and_then_compose_with_filter():
    async def awaitable_double_int(inp: int) -> int:
        return inp * 2

    class AddOneFilter(SimpleFilter[int, int]):
        async def __call__(self, inp: int, service: Service[int, int]) -> int:
            return await service(inp + 1)

    class OutputToStringFilter(Filter[int, str, int, int]):
        async def __call__(self, inp: int, service: Service[int, int]) -> str:
            return str(await service(inp))

    doubler_service: Service[int, int] = awaitable_double_int

    add_one_filter = AddOneFilter()
    output_to_string_filter = OutputToStringFilter()

    add_one_and_double_service = add_one_filter.and_then_service(doubler_service)

    composed_service = output_to_string_filter.and_then_service(add_one_and_double_service)

    assert await composed_service(2) == "6"


@pytest.mark.asyncio
async def test_service_propagates_errors():
    async def awaitable_thrower(_: None) -> None:
        raise NotImplementedError

    thrower_service: Service[None, None] = awaitable_thrower

    with pytest.raises(NotImplementedError):
        await thrower_service(None)


@pytest.mark.asyncio
async def test_simple_filter_can_modify_both_input_and_output():
    async def awaitable_double_int(inp: int) -> int:
        return inp * 2

    class AddOneBeforeAndAfterFilter(SimpleFilter[int, int]):
        async def __call__(self, inp: int, service: Service[int, int]) -> int:
            return (await service(inp + 1)) + 1

    doubler_service: Service[int, int] = awaitable_double_int
    add_one_before_and_after_filter = AddOneBeforeAndAfterFilter()

    assert await add_one_before_and_after_filter.and_then_service(doubler_service)(2) == 7


@pytest.mark.asyncio
async def test_response_shaping_filter():
    async def awaitable_double_int(inp: int) -> int:
        return inp * 2

    @dataclass()
    class InputType:
        value: int

    @dataclass()
    class OutputType:
        value: int

    class ConvertTypesFilter(Filter[InputType, OutputType, int, int]):
        async def __call__(self, inp: InputType, service: Service[int, int]) -> OutputType:
            result = await service(inp.value)
            return OutputType(value=result)

    doubler_service: Service[int, int] = awaitable_double_int
    convert_types_filter = ConvertTypesFilter()

    service_with_wrapper_types = convert_types_filter.and_then_service(doubler_service)

    assert await service_with_wrapper_types(InputType(value=2)) == OutputType(value=4)
