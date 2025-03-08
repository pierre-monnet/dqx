from datetime import datetime

import yaml
import pyspark.sql.functions as F
import pytest
from pyspark.sql import Column
from chispa.dataframe_comparer import assert_df_equality  # type: ignore
from databricks.labs.dqx.col_functions import is_not_null_and_not_empty, make_condition
from databricks.labs.dqx.engine import (
    DQEngine,
    ExtraParams,
)
from databricks.labs.dqx.rule import DQRule, DQRuleColSet, ColumnArguments
from databricks.labs.dqx.schema import validation_result_schema


SCHEMA = "a: int, b: int, c: int"
REPORTING_COLUMNS = (
    f", _errors: {validation_result_schema.simpleString()}, _warnings: {validation_result_schema.simpleString()}"
)
EXPECTED_SCHEMA = SCHEMA + REPORTING_COLUMNS
EXPECTED_SCHEMA_WITH_CUSTOM_NAMES = (
    SCHEMA
    + f", dq_errors: {validation_result_schema.simpleString()}, dq_warnings: {validation_result_schema.simpleString()}"
)


def test_apply_checks_on_empty_checks(ws, spark):
    dq_engine = DQEngine(ws)
    test_df = spark.createDataFrame([[1, 3, None], [2, 4, None]], SCHEMA)

    good = dq_engine.apply_checks(test_df, [])

    expected_df = spark.createDataFrame([[1, 3, None, None, None], [2, 4, None, None, None]], EXPECTED_SCHEMA)
    assert_df_equality(good, expected_df)


def test_apply_checks_and_split_on_empty_checks(ws, spark):
    dq_engine = DQEngine(ws)
    test_df = spark.createDataFrame([[1, 3, None], [2, 4, None]], SCHEMA)

    good, bad = dq_engine.apply_checks_and_split(test_df, [])

    expected_df = spark.createDataFrame([], EXPECTED_SCHEMA)

    assert_df_equality(good, test_df)
    assert_df_equality(bad, expected_df)


def test_apply_checks_passed(ws, spark):
    dq_engine = DQEngine(ws)
    test_df = spark.createDataFrame([[1, 3, 3]], SCHEMA)

    checks = [
        DQRule(
            name="col_a_is_null_or_empty",
            criticality="warn",
            check_func=is_not_null_and_not_empty,
            col_name="a",
        ),
        DQRule(
            name="col_b_is_null_or_empty",
            criticality="error",
            check_func=is_not_null_and_not_empty,
            col_name="b",
        ),
    ]

    checked = dq_engine.apply_checks(test_df, checks)

    expected = spark.createDataFrame([[1, 3, 3, None, None]], EXPECTED_SCHEMA)
    assert_df_equality(checked, expected, ignore_nullable=True)


def test_apply_checks(ws, spark, run_time_date):
    dq_engine = DQEngine(ws)
    test_df = spark.createDataFrame([[1, 3, 3], [2, None, 4], [None, 4, None], [None, None, None]], SCHEMA)

    checks = [
        DQRule(
            name="col_a_is_null_or_empty",
            criticality="warn",
            check_func=is_not_null_and_not_empty,
            col_name="a",
        ),
        DQRule(
            name="col_b_is_null_or_empty",
            criticality="error",
            check_func=is_not_null_and_not_empty,
            col_name="b",
        ),
        DQRule(
            name="col_c_is_null_or_empty",
            criticality="error",
            check_func=is_not_null_and_not_empty,
            col_name="c",
        ),
    ]

    checked = dq_engine.apply_checks(test_df, checks)

    expected = spark.createDataFrame(
        [
            [1, 3, 3, None, None],
            [
                2,
                None,
                4,
                [
                    {
                        "name": "col_b_is_null_or_empty",
                        "rule": "Column b is null or empty",
                        "col_name": "b",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": run_time_date,
                    }
                ],
                None,
            ],
            [
                None,
                4,
                None,
                [
                    {
                        "name": "col_c_is_null_or_empty",
                        "rule": "Column c is null or empty",
                        "col_name": "c",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": run_time_date,
                    }
                ],
                [
                    {
                        "name": "col_a_is_null_or_empty",
                        "rule": "Column a is null or empty",
                        "col_name": "a",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": run_time_date,
                    }
                ],
            ],
            [
                None,
                None,
                None,
                [
                    {
                        "name": "col_b_is_null_or_empty",
                        "rule": "Column b is null or empty",
                        "col_name": "b",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": run_time_date,
                    },
                    {
                        "name": "col_c_is_null_or_empty",
                        "rule": "Column c is null or empty",
                        "col_name": "c",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": run_time_date,
                    },
                ],
                [
                    {
                        "name": "col_a_is_null_or_empty",
                        "rule": "Column a is null or empty",
                        "col_name": "a",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": run_time_date,
                    }
                ],
            ],
        ],
        EXPECTED_SCHEMA,
    )

    assert_df_equality(checked, expected, ignore_nullable=True)


def test_apply_checks_invalid_criticality(ws, spark):
    dq_engine = DQEngine(ws)
    test_df = spark.createDataFrame([[1, 3, 3], [2, None, 4], [None, 4, None], [None, None, None]], SCHEMA)

    checks = [
        DQRule(
            name="col_a_is_null_or_empty",
            criticality="warn",
            check_func=is_not_null_and_not_empty,
            col_name="a",
        ),
        DQRule(
            name="col_b_is_null_or_empty",
            criticality="error",
            check_func=is_not_null_and_not_empty,
            col_name="b",
        ),
        DQRule(
            name="col_c_is_null_or_empty",
            criticality="invalid",
            check_func=is_not_null_and_not_empty,
            col_name="c",
        ),
    ]

    with pytest.raises(ValueError, match="Invalid criticality value: invalid"):
        dq_engine.apply_checks(test_df, checks)


def test_apply_checks_with_autogenerated_col_names(ws, spark, run_time_date):
    dq_engine = DQEngine(ws)
    test_df = spark.createDataFrame([[1, 3, 3], [2, None, 4], [None, 4, None], [None, None, None]], SCHEMA)

    checks = [
        DQRule(criticality="warn", check_func=is_not_null_and_not_empty, col_name="a"),
        DQRule(criticality="error", check_func=is_not_null_and_not_empty, col_name="b"),
        DQRule(criticality="error", check_func=is_not_null_and_not_empty, col_name="c"),
    ]

    checked = dq_engine.apply_checks(test_df, checks)

    expected = spark.createDataFrame(
        [
            [1, 3, 3, None, None],
            [
                2,
                None,
                4,
                [
                    {
                        "name": "col_b_is_null_or_empty",
                        "rule": "Column b is null or empty",
                        "col_name": "b",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": run_time_date,
                    }
                ],
                None,
            ],
            [
                None,
                4,
                None,
                [
                    {
                        "name": "col_c_is_null_or_empty",
                        "rule": "Column c is null or empty",
                        "col_name": "c",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": run_time_date,
                    }
                ],
                [
                    {
                        "name": "col_a_is_null_or_empty",
                        "rule": "Column a is null or empty",
                        "col_name": "a",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": run_time_date,
                    }
                ],
            ],
            [
                None,
                None,
                None,
                [
                    {
                        "name": "col_b_is_null_or_empty",
                        "rule": "Column b is null or empty",
                        "col_name": "b",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": run_time_date,
                    },
                    {
                        "name": "col_c_is_null_or_empty",
                        "rule": "Column c is null or empty",
                        "col_name": "c",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": run_time_date,
                    },
                ],
                [
                    {
                        "name": "col_a_is_null_or_empty",
                        "rule": "Column a is null or empty",
                        "col_name": "a",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": run_time_date,
                    }
                ],
            ],
        ],
        EXPECTED_SCHEMA,
    )

    assert_df_equality(checked, expected, ignore_nullable=True)


def test_apply_checks_and_split(ws, spark, run_time_date):
    dq_engine = DQEngine(ws)
    test_df = spark.createDataFrame([[1, 3, 3], [2, None, 4], [None, 4, None], [None, None, None]], SCHEMA)

    checks = [
        DQRule(name="col_a_is_null_or_empty", criticality="warn", check_func=is_not_null_and_not_empty, col_name="a"),
        DQRule(name="col_b_is_null_or_empty", criticality="error", check_func=is_not_null_and_not_empty, col_name="b"),
        DQRule(name="col_c_is_null_or_empty", criticality="warn", check_func=is_not_null_and_not_empty, col_name="c"),
    ]

    good, bad = dq_engine.apply_checks_and_split(test_df, checks)

    expected_good = spark.createDataFrame([[1, 3, 3], [None, 4, None]], SCHEMA)
    assert_df_equality(good, expected_good)

    expected_bad = spark.createDataFrame(
        [
            [
                2,
                None,
                4,
                [
                    {
                        "name": "col_b_is_null_or_empty",
                        "rule": "Column b is null or empty",
                        "col_name": "b",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": run_time_date,
                    }
                ],
                None,
            ],
            [
                None,
                4,
                None,
                None,
                [
                    {
                        "name": "col_a_is_null_or_empty",
                        "rule": "Column a is null or empty",
                        "col_name": "a",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": run_time_date,
                    },
                    {
                        "name": "col_c_is_null_or_empty",
                        "rule": "Column c is null or empty",
                        "col_name": "c",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": run_time_date,
                    },
                ],
            ],
            [
                None,
                None,
                None,
                [
                    {
                        "name": "col_b_is_null_or_empty",
                        "rule": "Column b is null or empty",
                        "col_name": "b",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": run_time_date,
                    }
                ],
                [
                    {
                        "name": "col_a_is_null_or_empty",
                        "rule": "Column a is null or empty",
                        "col_name": "a",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": run_time_date,
                    },
                    {
                        "name": "col_c_is_null_or_empty",
                        "rule": "Column c is null or empty",
                        "col_name": "c",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": run_time_date,
                    },
                ],
            ],
        ],
        EXPECTED_SCHEMA,
    )

    assert_df_equality(bad, expected_bad, ignore_nullable=True)


def test_apply_checks_and_split_by_metadata(ws, spark, run_time_date):
    dq_engine = DQEngine(ws)
    test_df = spark.createDataFrame([[1, 3, 3], [2, None, 4], [None, 4, None], [None, None, None]], SCHEMA)

    checks = [
        {
            "name": "col_a_is_null_or_empty",
            "criticality": "warn",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"col_name": "a"}},
        },
        {
            "name": "col_b_is_null_or_empty",
            "criticality": "error",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"col_name": "b"}},
        },
        {
            "name": "col_c_is_null_or_empty",
            "criticality": "warn",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"col_name": "c"}},
        },
        {
            "name": "col_a_is_not_in_the_list",
            "criticality": "warn",
            "check": {"function": "is_in_list", "arguments": {"col_name": "a", "allowed": [1, 3, 4]}},
        },
        {
            "name": "col_c_is_not_in_the_list",
            "criticality": "warn",
            "check": {"function": "is_in_list", "arguments": {"col_name": "c", "allowed": [1, 3, 4]}},
        },
    ]

    good, bad = dq_engine.apply_checks_by_metadata_and_split(test_df, checks)

    expected_good = spark.createDataFrame([[1, 3, 3], [None, 4, None]], SCHEMA)
    assert_df_equality(good, expected_good)

    expected_bad = spark.createDataFrame(
        [
            [
                2,
                None,
                4,
                [
                    {
                        "name": "col_b_is_null_or_empty",
                        "rule": "Column b is null or empty",
                        "col_name": "b",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": run_time_date,
                    }
                ],
                [
                    {
                        "name": "col_a_is_not_in_the_list",
                        "rule": "Value 2 is not in the allowed list: [1, 3, 4]",
                        "col_name": "a",
                        "filter": None,
                        "function": "is_in_the_list",
                        "run_time": run_time_date,
                    }
                ],
            ],
            [
                None,
                4,
                None,
                None,
                [
                    {
                        "name": "col_a_is_null_or_empty",
                        "rule": "Column a is null or empty",
                        "col_name": "a",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": run_time_date,
                    },
                    {
                        "name": "col_c_is_null_or_empty",
                        "rule": "Column c is null or empty",
                        "col_name": "c",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": run_time_date,
                    },
                ],
            ],
            [
                None,
                None,
                None,
                [
                    {
                        "name": "col_b_is_null_or_empty",
                        "rule": "Column b is null or empty",
                        "col_name": "b",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": run_time_date,
                    }
                ],
                [
                    {
                        "name": "col_a_is_null_or_empty",
                        "rule": "Column a is null or empty",
                        "col_name": "a",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": run_time_date,
                    },
                    {
                        "name": "col_c_is_null_or_empty",
                        "rule": "Column c is null or empty",
                        "col_name": "c",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": run_time_date,
                    },
                ],
            ],
        ],
        EXPECTED_SCHEMA,
    )

    assert_df_equality(bad, expected_bad, ignore_nullable=True)


def test_apply_checks_and_split_by_metadata_with_autogenerated_col_names(ws, spark, run_time_date):
    dq_engine = DQEngine(ws)
    test_df = spark.createDataFrame([[1, 3, 3], [2, None, 4], [None, 4, None], [None, None, None]], SCHEMA)

    checks = [
        {
            "criticality": "warn",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"col_names": ["a", "c"]}},
        },
        {
            "criticality": "error",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"col_name": "b"}},
        },
        {
            "criticality": "warn",
            "check": {"function": "is_in_list", "arguments": {"col_names": ["a", "c"], "allowed": [1, 3, 4]}},
        },
    ]

    good, bad = dq_engine.apply_checks_by_metadata_and_split(test_df, checks)

    expected_good = spark.createDataFrame([[1, 3, 3], [None, 4, None]], SCHEMA)
    assert_df_equality(good, expected_good)

    expected_bad = spark.createDataFrame(
        [
            [
                2,
                None,
                4,
                [
                    {
                        "name": "col_b_is_null_or_empty",
                        "rule": "Column b is null or empty",
                        "col_name": "b",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": run_time_date,
                    }
                ],
                [
                    {
                        "name": "col_a_is_not_in_the_list",
                        "rule": "Value 2 is not in the allowed list: [1, 3, 4]",
                        "col_name": "a",
                        "filter": None,
                        "function": "is_in_the_list",
                        "run_time": run_time_date,
                    }
                ],
            ],
            [
                None,
                4,
                None,
                None,
                [
                    {
                        "name": "col_a_is_null_or_empty",
                        "rule": "Column a is null or empty",
                        "col_name": "a",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": run_time_date,
                    },
                    {
                        "name": "col_c_is_null_or_empty",
                        "rule": "Column c is null or empty",
                        "col_name": "c",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": run_time_date,
                    },
                ],
            ],
            [
                None,
                None,
                None,
                [
                    {
                        "name": "col_b_is_null_or_empty",
                        "rule": "Column b is null or empty",
                        "col_name": "b",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": run_time_date,
                    }
                ],
                [
                    {
                        "name": "col_a_is_null_or_empty",
                        "rule": "Column a is null or empty",
                        "col_name": "a",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": run_time_date,
                    },
                    {
                        "name": "col_c_is_null_or_empty",
                        "rule": "Column c is null or empty",
                        "col_name": "c",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": run_time_date,
                    },
                ],
            ],
        ],
        EXPECTED_SCHEMA,
    )

    assert_df_equality(bad, expected_bad, ignore_nullable=True)


def test_apply_checks_by_metadata(ws, spark, run_time_date):
    dq_engine = DQEngine(ws)
    test_df = spark.createDataFrame([[1, 3, 3], [2, None, 4], [None, 4, None], [None, None, None]], SCHEMA)

    checks = [
        {
            "criticality": "warn",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"col_names": ["a", "c"]}},
        },
        {
            "criticality": "error",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"col_name": "b"}},
        },
        {
            "criticality": "warn",
            "check": {"function": "is_in_list", "arguments": {"col_names": ["a", "c"], "allowed": [1, 3, 4]}},
        },
    ]

    checked = dq_engine.apply_checks_by_metadata(test_df, checks)

    expected = spark.createDataFrame(
        [
            [1, 3, 3, None, None],
            [
                2,
                None,
                4,
                [
                    {
                        "name": "col_b_is_null_or_empty",
                        "rule": "Column b is null or empty",
                        "col_name": "b",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": run_time_date,
                    }
                ],
                [
                    {
                        "name": "col_a_is_not_in_the_list",
                        "rule": "Value 2 is not in the allowed list: [1, 3, 4]",
                        "col_name": "a",
                        "filter": None,
                        "function": "is_in_the_list",
                        "run_time": run_time_date,
                    }
                ],
            ],
            [
                None,
                4,
                None,
                None,
                [
                    {
                        "name": "col_a_is_null_or_empty",
                        "rule": "Column a is null or empty",
                        "col_name": "a",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": run_time_date,
                    },
                    {
                        "name": "col_c_is_null_or_empty",
                        "rule": "Column c is null or empty",
                        "col_name": "c",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": run_time_date,
                    },
                ],
            ],
            [
                None,
                None,
                None,
                [
                    {
                        "name": "col_b_is_null_or_empty",
                        "rule": "Column b is null or empty",
                        "col_name": "b",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": run_time_date,
                    }
                ],
                [
                    {
                        "name": "col_a_is_null_or_empty",
                        "rule": "Column a is null or empty",
                        "col_name": "a",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": run_time_date,
                    },
                    {
                        "name": "col_c_is_null_or_empty",
                        "rule": "Column c is null or empty",
                        "col_name": "c",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": run_time_date,
                    },
                ],
            ],
        ],
        EXPECTED_SCHEMA,
    )

    assert_df_equality(checked, expected, ignore_nullable=True)


def test_apply_checks_with_filter(ws, spark, run_time_date):
    dq_engine = DQEngine(ws)
    test_df = spark.createDataFrame(
        [[1, 3, 3], [2, None, 4], [3, 4, None], [4, None, None], [None, None, None]], SCHEMA
    )

    checks = DQRuleColSet(
        check_func=is_not_null_and_not_empty, criticality="warn", filter="b>3", columns=["a", "c"]
    ).get_rules() + [
        DQRule(
            name="col_b_is_null_or_empty",
            criticality="error",
            check_func=is_not_null_and_not_empty,
            col_name="b",
            filter="a<3",
        )
    ]

    checked = dq_engine.apply_checks(test_df, checks)

    expected = spark.createDataFrame(
        [
            [1, 3, 3, None, None],
            [
                2,
                None,
                4,
                [
                    {
                        "name": "col_b_is_null_or_empty",
                        "rule": "Column b is null or empty",
                        "col_name": "b",
                        "filter": "a<3",
                        "function": "is_not_null_and_not_empty",
                        "run_time": run_time_date,
                    }
                ],
                None,
            ],
            [
                3,
                4,
                None,
                None,
                [
                    {
                        "name": "col_c_is_null_or_empty",
                        "rule": "Column c is null or empty",
                        "col_name": "c",
                        "filter": "b>3",
                        "function": "is_not_null_and_not_empty",
                        "run_time": run_time_date,
                    }
                ],
            ],
            [4, None, None, None, None],
            [None, None, None, None, None],
        ],
        EXPECTED_SCHEMA,
    )

    assert_df_equality(checked, expected, ignore_nullable=True)


def test_apply_checks_by_metadata_with_filter(ws, spark, run_time_date):
    dq_engine = DQEngine(ws)
    test_df = spark.createDataFrame(
        [[1, 3, 3], [2, None, 4], [3, 4, None], [4, None, None], [None, None, None]], SCHEMA
    )

    checks = [
        {
            "criticality": "warn",
            "filter": "b>3",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"col_names": ["b", "c"]}},
        },
        {
            "criticality": "error",
            "filter": "a<3",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"col_name": "b"}},
        },
    ]

    checked = dq_engine.apply_checks_by_metadata(test_df, checks)

    expected = spark.createDataFrame(
        [
            [1, 3, 3, None, None],
            [
                2,
                None,
                4,
                [
                    {
                        "name": "col_b_is_null_or_empty",
                        "rule": "Column b is null or empty",
                        "col_name": "b",
                        "filter": "a<3",
                        "function": "is_not_null_and_not_empty",
                        "run_time": run_time_date,
                    }
                ],
                None,
            ],
            [
                3,
                4,
                None,
                None,
                [
                    {
                        "name": "col_c_is_null_or_empty",
                        "rule": "Column c is null or empty",
                        "col_name": "c",
                        "filter": "b>3",
                        "function": "is_not_null_and_not_empty",
                        "run_time": run_time_date,
                    }
                ],
            ],
            [4, None, None, None, None],
            [None, None, None, None, None],
        ],
        EXPECTED_SCHEMA,
    )

    assert_df_equality(checked, expected, ignore_nullable=True)


def test_apply_checks_from_json_file_by_metadata(ws, spark, make_local_check_file_as_json, run_time_date):
    dq_engine = DQEngine(ws)
    schema = "col1: int, col2: int, col3: int, col4 int"
    test_df = spark.createDataFrame([[1, 3, 3, 1], [2, None, 4, 1]], schema)

    check_file = make_local_check_file_as_json
    checks = DQEngine.load_checks_from_local_file(check_file)

    actual = dq_engine.apply_checks_by_metadata(test_df, checks)

    expected = spark.createDataFrame(
        [
            [1, 3, 3, 1, None, None],
            [
                2,
                None,
                4,
                1,
                [
                    {
                        "name": "col_col2_is_null",
                        "rule": "Column col2 is null",
                        "col_name": "col2",
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": run_time_date,
                    }
                ],
                None,
            ],
        ],
        schema + REPORTING_COLUMNS,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_apply_checks_from_yml_file_by_metadata(ws, spark, make_local_check_file_as_yml, run_time_date):
    dq_engine = DQEngine(ws)
    schema = "col1: int, col2: int, col3: int, col4 int"
    test_df = spark.createDataFrame([[1, 3, 3, 1], [2, None, 4, 1]], schema)

    check_file = make_local_check_file_as_yml
    checks = DQEngine.load_checks_from_local_file(check_file)

    actual = dq_engine.apply_checks_by_metadata(test_df, checks)

    expected = spark.createDataFrame(
        [
            [1, 3, 3, 1, None, None],
            [
                2,
                None,
                4,
                1,
                [
                    {
                        "name": "col_col2_is_null",
                        "rule": "Column col2 is null",
                        "col_name": "col2",
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": run_time_date,
                    }
                ],
                None,
            ],
        ],
        schema + REPORTING_COLUMNS,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def custom_check_func_global(col_name: str) -> Column:
    column = F.col(col_name)
    return make_condition(column.isNull(), "custom check failed", f"{col_name}_is_null_custom")


def test_apply_checks_with_custom_check(ws, spark, run_time_date):
    dq_engine = DQEngine(ws)
    test_df = spark.createDataFrame([[1, 3, 3], [2, None, 4], [None, 4, None], [None, None, None]], SCHEMA)

    checks = [
        DQRule(criticality="warn", check_func=is_not_null_and_not_empty, col_name="a"),
        DQRule(criticality="warn", check_func=custom_check_func_global, col_name="a"),
    ]

    checked = dq_engine.apply_checks(test_df, checks)

    expected = spark.createDataFrame(
        [
            [1, 3, 3, None, None],
            [2, None, 4, None, None],
            [
                None,
                4,
                None,
                None,
                [
                    {
                        "name": "col_a_is_null_or_empty",
                        "rule": "Column a is null or empty",
                        "col_name": "a",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": run_time_date,
                    },
                    {
                        "name": "col_a_is_null_custom",
                        "rule": "custom check failed",
                        "col_name": "a",
                        "filter": None,
                        "function": "custom_check_func_global",
                        "run_time": run_time_date,
                    },
                ],
            ],
            [
                None,
                None,
                None,
                None,
                [
                    {
                        "name": "col_a_is_null_or_empty",
                        "rule": "Column a is null or empty",
                        "col_name": "a",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": run_time_date,
                    },
                    {
                        "name": "col_a_is_null_custom",
                        "rule": "custom check failed",
                        "col_name": "a",
                        "filter": None,
                        "function": "custom_check_func_global",
                        "run_time": run_time_date,
                    },
                ],
            ],
        ],
        EXPECTED_SCHEMA,
    )

    assert_df_equality(checked, expected, ignore_nullable=True)


def test_apply_checks_by_metadata_with_custom_check(ws, spark, run_time_date):
    dq_engine = DQEngine(ws)
    test_df = spark.createDataFrame([[1, 3, 3], [2, None, 4], [None, 4, None], [None, None, None]], SCHEMA)

    checks = [
        {"criticality": "warn", "check": {"function": "is_not_null_and_not_empty", "arguments": {"col_name": "a"}}},
        {"criticality": "warn", "check": {"function": "custom_check_func_global", "arguments": {"col_name": "a"}}},
    ]

    checked = dq_engine.apply_checks_by_metadata(
        test_df, checks, {"custom_check_func_global": custom_check_func_global}
    )
    # or for simplicity use globals
    checked2 = dq_engine.apply_checks_by_metadata(test_df, checks, globals())

    expected = spark.createDataFrame(
        [
            [1, 3, 3, None, None],
            [2, None, 4, None, None],
            [
                None,
                4,
                None,
                None,
                [
                    {
                        "name": "col_a_is_null_or_empty",
                        "rule": "Column a is null or empty",
                        "col_name": "a",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": run_time_date,
                    },
                    {
                        "name": "col_a_is_null_custom",
                        "rule": "custom check failed",
                        "col_name": "a",
                        "filter": None,
                        "function": "custom_check_func_global",
                        "run_time": run_time_date,
                    },
                ],
            ],
            [
                None,
                None,
                None,
                None,
                [
                    {
                        "name": "col_a_is_null_or_empty",
                        "rule": "Column a is null or empty",
                        "col_name": "a",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": run_time_date,
                    },
                    {
                        "name": "col_a_is_null_custom",
                        "rule": "custom check failed",
                        "col_name": "a",
                        "filter": None,
                        "function": "custom_check_func_global",
                        "run_time": run_time_date,
                    },
                ],
            ],
        ],
        EXPECTED_SCHEMA,
    )

    assert_df_equality(checked, expected, ignore_nullable=True)
    assert_df_equality(checked2, expected, ignore_nullable=True)


def test_get_valid_records(ws, spark, run_time_date):
    dq_engine = DQEngine(ws)

    test_df = spark.createDataFrame(
        [
            [1, 1, 1, None, None],
            [
                None,
                2,
                2,
                None,
                [
                    {
                        "name": "col_a_is_null_or_empty",
                        "rule": "check failed",
                        "col_name": "a",
                        "filter": None,
                        "function": "col_a_is_null_or_empty",
                        "run_time": run_time_date,
                    }
                ],
            ],
            [
                None,
                2,
                2,
                [
                    {
                        "name": "col_b_is_null_or_empty",
                        "rule": "check failed",
                        "col_name": "b",
                        "filter": None,
                        "function": "col_a_is_null_or_empty",
                        "run_time": run_time_date,
                    }
                ],
                None,
            ],
        ],
        EXPECTED_SCHEMA,
    )

    valid_df = dq_engine.get_valid(test_df)

    expected_valid_df = spark.createDataFrame(
        [
            [1, 1, 1],
            [None, 2, 2],
        ],
        SCHEMA,
    )

    assert_df_equality(valid_df, expected_valid_df)


def test_get_invalid_records(ws, spark, run_time_date):
    dq_engine = DQEngine(ws)

    test_df = spark.createDataFrame(
        [
            [1, 1, 1, None, None],
            [
                None,
                2,
                2,
                None,
                [
                    {
                        "name": "col_a_is_null_or_empty",
                        "rule": "check failed",
                        "col_name": "a",
                        "filter": None,
                        "function": "col_a_is_null_or_empty",
                        "run_time": run_time_date,
                    }
                ],
            ],
            [
                None,
                2,
                2,
                [
                    {
                        "name": "col_b_is_null_or_empty",
                        "rule": "check failed",
                        "col_name": "b",
                        "filter": None,
                        "function": "col_a_is_null_or_empty",
                        "run_time": run_time_date,
                    }
                ],
                None,
            ],
        ],
        EXPECTED_SCHEMA,
    )

    invalid_df = dq_engine.get_invalid(test_df)

    expected_invalid_df = spark.createDataFrame(
        [
            [
                None,
                2,
                2,
                None,
                [
                    {
                        "name": "col_a_is_null_or_empty",
                        "rule": "check failed",
                        "col_name": "a",
                        "filter": None,
                        "function": "col_a_is_null_or_empty",
                        "run_time": run_time_date,
                    }
                ],
            ],
            [
                None,
                2,
                2,
                [
                    {
                        "name": "col_b_is_null_or_empty",
                        "rule": "check failed",
                        "col_name": "b",
                        "filter": None,
                        "function": "col_a_is_null_or_empty",
                        "run_time": run_time_date,
                    }
                ],
                None,
            ],
        ],
        EXPECTED_SCHEMA,
    )

    assert_df_equality(invalid_df, expected_invalid_df)


def test_apply_checks_with_custom_column_naming(ws, spark, run_time_date):
    dq_engine = DQEngine(
        ws,
        extra_params=ExtraParams(
            column_names={ColumnArguments.ERRORS.value: "dq_errors", ColumnArguments.WARNINGS.value: "dq_warnings"}
        ),
    )
    test_df = spark.createDataFrame([[1, 3, 3], [2, None, 4], [None, 4, None], [None, None, None]], SCHEMA)

    checks = [
        {"criticality": "warn", "check": {"function": "is_not_null_and_not_empty", "arguments": {"col_name": "a"}}}
    ]
    checked = dq_engine.apply_checks_by_metadata(test_df, checks)

    expected = spark.createDataFrame(
        [
            [1, 3, 3, None, None],
            [2, None, 4, None, None],
            [
                None,
                4,
                None,
                None,
                [
                    {
                        "name": "col_a_is_null_or_empty",
                        "rule": "Column a is null or empty",
                        "col_name": "a",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": run_time_date,
                    }
                ],
            ],
            [
                None,
                None,
                None,
                None,
                [
                    {
                        "name": "col_a_is_null_or_empty",
                        "rule": "Column a is null or empty",
                        "col_name": "a",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": run_time_date,
                    }
                ],
            ],
        ],
        EXPECTED_SCHEMA_WITH_CUSTOM_NAMES,
    )

    assert_df_equality(checked, expected, ignore_nullable=True)


def test_apply_checks_by_metadata_with_custom_column_naming(ws, spark, run_time_date):
    dq_engine = DQEngine(
        ws,
        extra_params=ExtraParams(
            column_names={ColumnArguments.ERRORS.value: "dq_errors", ColumnArguments.WARNINGS.value: "dq_warnings"}
        ),
    )
    test_df = spark.createDataFrame([[1, 3, 3], [2, None, 4], [None, 4, None], [None, None, None]], SCHEMA)

    checks = [
        {"criticality": "warn", "check": {"function": "is_not_null_and_not_empty", "arguments": {"col_name": "a"}}},
        {"criticality": "error", "check": {"function": "is_not_null_and_not_empty", "arguments": {"col_name": "b"}}},
    ]
    good, bad = dq_engine.apply_checks_by_metadata_and_split(test_df, checks)

    assert_df_equality(good, spark.createDataFrame([[1, 3, 3], [None, 4, None]], SCHEMA), ignore_nullable=True)

    assert_df_equality(
        bad,
        spark.createDataFrame(
            [
                [
                    2,
                    None,
                    4,
                    [
                        {
                            "name": "col_b_is_null_or_empty",
                            "rule": "Column b is null or empty",
                            "col_name": "b",
                            "filter": None,
                            "function": "is_not_null_and_not_empty",
                            "run_time": run_time_date,
                        }
                    ],
                    None,
                ],
                [
                    None,
                    4,
                    None,
                    None,
                    [
                        {
                            "name": "col_a_is_null_or_empty",
                            "rule": "Column a is null or empty",
                            "col_name": "a",
                            "filter": None,
                            "function": "is_not_null_and_not_empty",
                            "run_time": run_time_date,
                        }
                    ],
                ],
                [
                    None,
                    None,
                    None,
                    [
                        {
                            "name": "col_b_is_null_or_empty",
                            "rule": "Column b is null or empty",
                            "col_name": "b",
                            "filter": None,
                            "function": "is_not_null_and_not_empty",
                            "run_time": run_time_date,
                        }
                    ],
                    [
                        {
                            "name": "col_a_is_null_or_empty",
                            "rule": "Column a is null or empty",
                            "col_name": "a",
                            "filter": None,
                            "function": "is_not_null_and_not_empty",
                            "run_time": run_time_date,
                        }
                    ],
                ],
            ],
            EXPECTED_SCHEMA_WITH_CUSTOM_NAMES,
        ),
    )


def test_apply_checks_by_metadata_with_custom_column_naming_fallback_to_default(ws, spark, run_time_date):
    dq_engine = DQEngine(
        ws,
        extra_params=ExtraParams(column_names={"errors_invalid": "dq_errors", "warnings_invalid": "dq_warnings"}),
    )
    test_df = spark.createDataFrame([[1, 3, 3], [2, None, 4], [None, 4, None], [None, None, None]], SCHEMA)

    checks = [
        {"criticality": "warn", "check": {"function": "is_not_null_and_not_empty", "arguments": {"col_name": "a"}}},
        {"criticality": "error", "check": {"function": "is_not_null_and_not_empty", "arguments": {"col_name": "b"}}},
    ]
    good, bad = dq_engine.apply_checks_by_metadata_and_split(test_df, checks)

    assert_df_equality(good, spark.createDataFrame([[1, 3, 3], [None, 4, None]], SCHEMA), ignore_nullable=True)

    assert_df_equality(
        bad,
        spark.createDataFrame(
            [
                [
                    2,
                    None,
                    4,
                    [
                        {
                            "name": "col_b_is_null_or_empty",
                            "rule": "Column b is null or empty",
                            "col_name": "b",
                            "filter": None,
                            "function": "is_not_null_and_not_empty",
                            "run_time": run_time_date,
                        }
                    ],
                    None,
                ],
                [
                    None,
                    4,
                    None,
                    None,
                    [
                        {
                            "name": "col_a_is_null_or_empty",
                            "rule": "Column a is null or empty",
                            "col_name": "a",
                            "filter": None,
                            "function": "is_not_null_and_not_empty",
                            "run_time": run_time_date,
                        }
                    ],
                ],
                [
                    None,
                    None,
                    None,
                    [
                        {
                            "name": "col_b_is_null_or_empty",
                            "rule": "Column b is null or empty",
                            "col_name": "b",
                            "filter": None,
                            "function": "is_not_null_and_not_empty",
                            "run_time": run_time_date,
                        }
                    ],
                    [
                        {
                            "name": "col_a_is_null_or_empty",
                            "rule": "Column a is null or empty",
                            "col_name": "a",
                            "filter": None,
                            "function": "is_not_null_and_not_empty",
                            "run_time": run_time_date,
                        }
                    ],
                ],
            ],
            EXPECTED_SCHEMA,
        ),
    )


def test_apply_checks_with_sql_expression(ws, spark, run_time_date):
    dq_engine = DQEngine(ws)
    schema = "col1: string, col2: string"
    test_df = spark.createDataFrame([["str1", "str2"], ["val1", "val2"]], schema)

    checks = [
        {
            "criticality": "error",
            "check": {"function": "sql_expression", "arguments": {"expression": 'col1 like "val%"'}},
        },
        {
            "criticality": "error",
            "check": {"function": "sql_expression", "arguments": {"expression": "col2 like 'val%'"}},
        },
    ]

    checked = dq_engine.apply_checks_by_metadata(test_df, checks)

    expected_schema = schema + REPORTING_COLUMNS
    expected = spark.createDataFrame(
        [
            ["str1", "str2", None, None],
            [
                "val1",
                "val2",
                [
                    {
                        "name": "col_col1_like_val_",
                        "rule": 'Value matches expression: col1 like "val%"',
                        "col_name": "",
                        "filter": None,
                        "function": "sql_expression",
                        "run_time": run_time_date,
                    },
                    {
                        "name": "col_col2_like_val_",
                        "rule": "Value matches expression: col2 like 'val%'",
                        "col_name": "",
                        "filter": None,
                        "function": "sql_expression",
                        "run_time": run_time_date,
                    },
                ],
                None,
            ],
        ],
        expected_schema,
    )
    assert_df_equality(checked, expected, ignore_nullable=True)


def test_apply_checks_with_is_unique(ws, spark, set_utc_timezone):
    schema = "col1: int, col2: timestamp"
    test_df = spark.createDataFrame([[1, datetime(2025, 1, 1)], [1, datetime(2025, 1, 2)], [None, None]], schema)

    checks = [
        {
            "criticality": "error",
            "check": {"function": "is_unique", "arguments": {"col_name": "col1"}},
        },
        {
            "criticality": "error",
            "name": "col_col1_is_not_unique2",
            "check": {
                "function": "is_unique",
                "arguments": {"col_name": "col1", "window_spec": "window(coalesce(col2, '1970-01-01'), '30 days')"},
            },
        },
    ]

    dq_engine = DQEngine(ws)
    checked = dq_engine.apply_checks_by_metadata(test_df, checks)

    expected_schema = schema + REPORTING_COLUMNS
    expected = spark.createDataFrame(
        [
            [None, None, None, None],
            [
                1,
                datetime(2025, 1, 1),
                {
                    "col_col1_is_not_unique": "Column col1 has duplicate values",
                    "col_col1_is_not_unique2": "Column col1 has duplicate values",
                },
                None,
            ],
            [
                1,
                datetime(2025, 1, 2),
                {
                    "col_col1_is_not_unique": "Column col1 has duplicate values",
                    "col_col1_is_not_unique2": "Column col1 has duplicate values",
                },
                None,
            ],
        ],
        expected_schema,
    )
    assert_df_equality(checked, expected, ignore_nullable=True)


def test_apply_checks_all_checks_as_yaml(ws, spark):
    checks = yaml.safe_load(
        """
    # is_not_null check
    - criticality: error
      check:
        function: is_not_null
        arguments:
          col_name: col1
    
    # is_not_empty check
    - criticality: error
      check:
        function: is_not_empty
        arguments:
          col_name: col1
    
    # is_not_null_and_not_empty check
    - criticality: error
      check:
        function: is_not_null_and_not_empty
        arguments:
          col_name: col1
          trim_strings: true
    
    # is_in_list check
    - criticality: error
      check:
        function: is_in_list
        arguments:
          col_name: col2
          allowed:
          - 1
          - 2
          - 3
    
    # is_not_null_and_is_in_list check
    - criticality: error
      check:
        function: is_not_null_and_is_in_list
        arguments:
          col_name: col2
          allowed:
          - 1
          - 2
          - 3
    
    # is_not_null_and_not_empty_array check
    - criticality: error
      check:
        function: is_not_null_and_not_empty_array
        arguments:
          col_name: col4
    
    # is_in_range check
    - criticality: error
      check:
        function: is_in_range
        arguments:
          col_name: col2
          min_limit: 1
          max_limit: 10
    - criticality: error
      check:
        function: is_in_range
        arguments:
          col_name: col5
          min_limit: 2025-01-01
          max_limit: 2025-02-24
    - criticality: error
      check:
        function: is_in_range
        arguments:
          col_name: col6
          min_limit: 2025-01-01 00:00:00
          max_limit: 2025-02-24 01:00:00
    - criticality: error
      check:
        function: is_in_range
        arguments:
          col_name: col3
          min_limit: col2
          max_limit: col2 * 2
    
    # is_not_in_range check
    - criticality: error
      check:
        function: is_not_in_range
        arguments:
          col_name: col2
          min_limit: 11
          max_limit: 20
    - criticality: error
      check:
        function: is_not_in_range
        arguments:
          col_name: col5
          min_limit: 2025-02-25
          max_limit: 2025-02-26
    - criticality: error
      check:
        function: is_not_in_range
        arguments:
          col_name: col6
          min_limit: 2025-02-25 00:00:00
          max_limit: 2025-02-26 01:00:00
    - criticality: error
      check:
        function: is_not_in_range
        arguments:
          col_name: col3
          min_limit: col2 + 10
          max_limit: col2 * 10
    
    # is_not_less_than check
    - criticality: error
      check:
        function: is_not_less_than
        arguments:
          col_name: col2
          limit: 0
    - criticality: error
      check:
        function: is_not_less_than
        arguments:
          col_name: col5
          limit: 2025-01-01
    - criticality: error
      check:
        function: is_not_less_than
        arguments:
          col_name: col6
          limit: 2025-01-01 01:00:00
    - criticality: error
      check:
        function: is_not_less_than
        arguments:
          col_name: col3
          limit: col2 - 10
    
    # is_not_greater_than check
    - criticality: error
      check:
        function: is_not_greater_than
        arguments:
          col_name: col2
          limit: 10
    - criticality: error
      check:
        function: is_not_greater_than
        arguments:
          col_name: col5
          limit: 2025-03-01
    - criticality: error
      check:
        function: is_not_greater_than
        arguments:
          col_name: col6
          limit: 2025-03-24 01:00:00
    - criticality: error
      check:
        function: is_not_greater_than
        arguments:
          col_name: col3
          limit: col2 + 10
    
    # is_valid_date check
    - criticality: error
      check:
        function: is_valid_date
        arguments:
          col_name: col5
    - criticality: error
      name: col5_is_not_valid_date2
      check:
        function: is_valid_date
        arguments:
          col_name: col5
          date_format: yyyy-MM-dd
    
    # is_valid_timestamp check
    - criticality: error
      check:
        function: is_valid_timestamp
        arguments:
          col_name: col6
          timestamp_format: yyyy-MM-dd HH:mm:ss
    - criticality: error
      name: col6_is_not_valid_timestamp2
      check:
        function: is_valid_timestamp
        arguments:
          col_name: col6
    
    # is_not_in_future check
    - criticality: error
      check:
        function: is_not_in_future
        arguments:
          col_name: col6
          offset: 86400
    
    # is_not_in_near_future check
    - criticality: error
      check:
        function: is_not_in_near_future
        arguments:
          col_name: col6
          offset: 36400
    
    # is_older_than_n_days check
    - criticality: error
      check:
        function: is_older_than_n_days
        arguments:
          col_name: col5
          days: 10000
    
    # is_older_than_col2_for_n_days check
    - criticality: error
      check:
        function: is_older_than_col2_for_n_days
        arguments:
          col_name1: col5
          col_name2: col6
          days: 2

    # is_unique check
    - criticality: error
      check:
        function: is_unique
        arguments:
          col_name: col1
    - criticality: error
      name: col1_is_not_unique2
      check:
        function: is_unique
        arguments:
          col_name: col1
          window_spec: window(coalesce(col6, '1970-01-01'), '10 minutes')

    # regex_match check
    - criticality: error
      check:
        function: regex_match
        arguments:
          col_name: col2
          regex: '[0-9]+'
          negate: false
    
    # sql_expression check
    - criticality: error
      check:
        function: sql_expression
        arguments:
          expression: col3 > col2 and col3 < 10
          msg: col3 is greater than col2 and col3 less than 10
          name: custom_output_name
          negate: false
    """
    )

    dq_engine = DQEngine(ws)
    status = dq_engine.validate_checks(checks)
    assert not status.has_errors

    schema = "col1: string, col2: int, col3: int, col4 array<int>, col5: date, col6: timestamp"
    test_df = spark.createDataFrame(
        [
            ["val1", 1, 1, [1], datetime(2025, 1, 2).date(), datetime(2025, 1, 2, 1, 0, 0)],
            ["val2", 2, 2, [2], datetime(2025, 1, 2).date(), datetime(2025, 1, 2, 2, 0, 0)],
            ["val3", 3, 3, [3], datetime(2025, 1, 2).date(), datetime(2025, 1, 2, 3, 0, 0)],
        ],
        schema,
    )

    checked = dq_engine.apply_checks_by_metadata(test_df, checks)

    expected_schema = schema + REPORTING_COLUMNS
    expected = spark.createDataFrame(
        [
            ["val1", 1, 1, [1], datetime(2025, 1, 2).date(), datetime(2025, 1, 2, 1, 0, 0), None, None],
            ["val2", 2, 2, [2], datetime(2025, 1, 2).date(), datetime(2025, 1, 2, 2, 0, 0), None, None],
            ["val3", 3, 3, [3], datetime(2025, 1, 2).date(), datetime(2025, 1, 2, 3, 0, 0), None, None],
        ],
        expected_schema,
    )
    assert_df_equality(checked, expected, ignore_nullable=True)
