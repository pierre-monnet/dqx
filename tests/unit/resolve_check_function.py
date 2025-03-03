import pytest
from databricks.labs.dqx.engine import DQEngineCore


def test_resolve_predefined_function():
    result = DQEngineCore.resolve_check_function('is_not_null')
    assert result


def custom_check_func():
    pass


def test_resolve_custom_check_function():
    result = DQEngineCore.resolve_check_function('custom_check_func', {"custom_check_func": custom_check_func})
    assert result

    # or for simplicity use globals
    result = DQEngineCore.resolve_check_function('custom_check_func', globals())
    assert result


def test_resolve_function_fail_on_missing():
    with pytest.raises(AttributeError):
        DQEngineCore.resolve_check_function('missing_func', fail_on_missing=True)


def test_resolve_function_not_fail_on_missing():
    result = DQEngineCore.resolve_check_function('missing_func', fail_on_missing=False)
    assert not result
