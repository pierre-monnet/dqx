from enum import Enum
from dataclasses import dataclass, field
import functools as ft
from typing import Any
from collections.abc import Callable
from datetime import datetime
from pyspark.sql import Column
import pyspark.sql.functions as F
from databricks.labs.dqx.utils import get_column_name


class Criticality(Enum):
    """Enum class to represent criticality of the check."""

    WARN = "warn"
    ERROR = "error"


class DefaultColumnNames(Enum):
    """Enum class to represent columns in the dataframe that will be used for error and warning reporting."""

    ERRORS = "_errors"
    WARNINGS = "_warnings"


class ColumnArguments(Enum):
    """Enum class that is used as input parsing for custom column naming."""

    ERRORS = "errors"
    WARNINGS = "warnings"


@dataclass(frozen=True)
class ExtraParams:
    """Class to represent extra parameters for DQEngine."""

    column_names: dict[str, str] = field(default_factory=dict)
    run_time: datetime = datetime.now()
    user_metadata: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class DQRule:
    """Class to represent a data quality rule consisting of following fields:
    * `check_func` - check function to be applied.
    * `col_name` - column name to which the given check function should be applied.
    * `name` - optional name that will be given to the resulting check name. Autogenerated if not provided.
    * `criticality` (optional) - possible values are `error` (critical problems), and `warn` (potential problems).
    * `filter` (optional) - filter expression to apply the check only to the rows that satisfy the condition.
    * `check_func_args` (optional) - non-keyword / positional arguments for the check function after the col_name.
    * `check_func_kwargs` (optional) - keyword /named arguments for the check function after the col_name.
    """

    check_func: Callable
    col_name: str | None = None
    name: str = ""
    criticality: str = Criticality.ERROR.value
    filter: str | None = None
    check_func_args: list[Any] = field(default_factory=list)
    check_func_kwargs: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        # validates correct args and kwargs are passed
        check = self._get_check()

        # take the name from the alias of the column expression if not provided
        object.__setattr__(self, "name", self.name if self.name else "col_" + get_column_name(check))

    @ft.cached_property
    def rule_criticality(self) -> str:
        """Returns criticality of the check.

        :return: string describing criticality - `warn` or `error`.
        :raises ValueError: if criticality is invalid.
        """
        criticality = self.criticality
        if criticality not in {Criticality.WARN.value, Criticality.ERROR.value}:
            raise ValueError(f"Invalid criticality value: {criticality}")

        return criticality

    def _get_check(self) -> Column:
        """Creates a Column object from the given check."""
        args = [self.col_name] if self.col_name else []
        args.extend(self.check_func_args)
        return self.check_func(*args, **self.check_func_kwargs)

    def check_column(self) -> Column:
        """Generates a Spark Column expression representing the check.

        This expression returns a string value if the check evaluates to `true`,
        which serves as an error or warning message. If the check evaluates to `false`,
        it returns `null`. If a filter condition is provided, the check is applied
        only to rows that satisfy the condition.

        :return: A Spark Column object representing the check condition.
        """
        # if filter is provided, apply the filter to the check
        filter_col = F.expr(self.filter) if self.filter else F.lit(True)
        return F.when(self._get_check().isNotNull(), F.when(filter_col, self._get_check())).otherwise(
            F.lit(None).cast("string")
        )


@dataclass(frozen=True)
class DQRuleColSet:
    """Class to represent a data quality col rule set which defines quality check function for a set of columns.
    The class consists of the following fields:
    * `columns` - list of column names to which the given check function should be applied
    * `criticality` - criticality level ('warn' or 'error')
    * `check_func` - check function to be applied
    * `check_func_args` - non-keyword / positional arguments for the check function after the col_name
    * `check_func_kwargs` - keyword /named arguments for the check function after the col_name
    """

    columns: list[str]
    check_func: Callable
    name: str = ""
    criticality: str = Criticality.ERROR.value
    filter: str | None = None
    check_func_args: list[Any] = field(default_factory=list)
    check_func_kwargs: dict[str, Any] = field(default_factory=dict)

    def get_rules(self) -> list[DQRule]:
        """Build a list of rules for a set of columns.

        :return: list of dq rules
        """
        rules = []
        for col_name in self.columns:
            rule = DQRule(
                col_name=col_name,
                name=self.name,
                criticality=self.criticality,
                check_func=self.check_func,
                check_func_args=self.check_func_args,
                check_func_kwargs=self.check_func_kwargs,
                filter=self.filter,
            )
            rules.append(rule)
        return rules


@dataclass(frozen=True)
class ChecksValidationStatus:
    """Class to represent the validation status."""

    _errors: list[str] = field(default_factory=list)

    def add_error(self, error: str):
        """Add an error to the validation status."""
        self._errors.append(error)

    def add_errors(self, errors: list[str]):
        """Add an error to the validation status."""
        self._errors.extend(errors)

    @property
    def has_errors(self) -> bool:
        """Check if there are any errors in the validation status."""
        return bool(self._errors)

    @property
    def errors(self) -> list[str]:
        """Get the list of errors in the validation status."""
        return self._errors

    def to_string(self) -> str:
        """Convert the validation status to a string."""
        if self.has_errors:
            return "\n".join(self._errors)
        return "No errors found"

    def __str__(self) -> str:
        """String representation of the ValidationStatus class."""
        return self.to_string()
