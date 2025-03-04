from unittest.mock import MagicMock

from chispa.dataframe_comparer import assert_df_equality  # type: ignore
from databricks.labs.dqx.col_functions import is_not_null_and_not_empty
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQRule
from databricks.sdk import WorkspaceClient


def test_apply_checks(spark_local, run_time_date):
    ws = MagicMock(spec=WorkspaceClient, **{"catalogs.list.return_value": []})

    schema = "x: int, y: int, z: int"
    expected_schema = (
        schema
        + ", _errors: ARRAY<STRUCT<name: STRING NOT NULL, rule: STRING, col_name: STRING NOT NULL, filter: STRING, message: STRING NOT NULL, run_time: STRING NOT NULL>>, _warnings: ARRAY<STRUCT<name: STRING NOT NULL, rule: STRING, col_name: STRING NOT NULL, filter: STRING, message: STRING NOT NULL, run_time: STRING NOT NULL>>"
    )
    test_df = spark_local.createDataFrame([[1, None, 3]], schema)

    checks = [
        DQRule(
            name="col_x_is_null_or_empty",
            criticality="warn",
            check_func=is_not_null_and_not_empty,
            col_name="x",
        ),
        DQRule(
            name="col_y_is_null_or_empty",
            criticality="error",
            check_func=is_not_null_and_not_empty,
            col_name="y",
        ),
    ]

    dq_engine = DQEngine(ws)

    df = dq_engine.apply_checks(test_df, checks)
    expected_df = spark_local.createDataFrame(
        [
            [
                1,
                None,
                3,
                [
                    {
                        "name": "col_y_is_null_or_empty",
                        "rule": "Column y is null or empty",
                        "col_name": "y",
                        "filter": None,
                        "message": "col_y_is_null_or_empty",
                        "run_time": run_time_date.strftime("%Y-%m-%dT%H:%M:%S"),
                    }
                ],
                None,
            ]
        ],
        expected_schema,
    )
    assert_df_equality(df, expected_df, ignore_nullable=True)
