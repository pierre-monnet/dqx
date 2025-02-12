import logging
import os
import functools as ft
import inspect
import itertools
from pathlib import Path
from collections.abc import Callable
from typing import Any
import yaml
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from databricks.labs.dqx.rule import (
    DQRule,
    Criticality,
    DQRuleColSet,
    ChecksValidationStatus,
    ColumnArguments,
    ExtraParams,
    DefaultColumnNames,
)
from databricks.labs.dqx.utils import deserialize_dicts
from databricks.labs.dqx import col_functions
from databricks.labs.blueprint.installation import Installation

from databricks.labs.dqx.base import DQEngineBase, DQEngineCoreBase
from databricks.labs.dqx.config import WorkspaceConfig, RunConfig
from databricks.sdk.errors import NotFound
from databricks.sdk.service.workspace import ImportFormat
from databricks.sdk import WorkspaceClient


logger = logging.getLogger(__name__)


class DQEngineCore(DQEngineCoreBase):
    """Data Quality Engine Core class to apply data quality checks to a given dataframe.
    Args:
        workspace_client (WorkspaceClient): WorkspaceClient instance to use for accessing the workspace.
        extra_params (ExtraParams): Extra parameters for the DQEngine.
    """

    def __init__(self, workspace_client: WorkspaceClient, extra_params: ExtraParams | None = None):
        super().__init__(workspace_client)

        extra_params = extra_params or ExtraParams()

        self._column_names = {
            ColumnArguments.ERRORS: extra_params.column_names.get(
                ColumnArguments.ERRORS.value, DefaultColumnNames.ERRORS.value
            ),
            ColumnArguments.WARNINGS: extra_params.column_names.get(
                ColumnArguments.WARNINGS.value, DefaultColumnNames.WARNINGS.value
            ),
        }

    def apply_checks(self, df: DataFrame, checks: list[DQRule]) -> DataFrame:
        if not checks:
            return self._append_empty_checks(df)

        warning_checks = self._get_check_columns(checks, Criticality.WARN.value)
        error_checks = self._get_check_columns(checks, Criticality.ERROR.value)
        ndf = self._create_results_map(df, error_checks, self._column_names[ColumnArguments.ERRORS])
        ndf = self._create_results_map(ndf, warning_checks, self._column_names[ColumnArguments.WARNINGS])

        return ndf

    def apply_checks_and_split(self, df: DataFrame, checks: list[DQRule]) -> tuple[DataFrame, DataFrame]:
        if not checks:
            return df, self._append_empty_checks(df).limit(0)

        checked_df = self.apply_checks(df, checks)

        good_df = self.get_valid(checked_df)
        bad_df = self.get_invalid(checked_df)

        return good_df, bad_df

    def apply_checks_by_metadata_and_split(
        self, df: DataFrame, checks: list[dict], glbs: dict[str, Any] | None = None
    ) -> tuple[DataFrame, DataFrame]:
        dq_rule_checks = self.build_checks_by_metadata(checks, glbs)

        good_df, bad_df = self.apply_checks_and_split(df, dq_rule_checks)

        return good_df, bad_df

    def apply_checks_by_metadata(
        self, df: DataFrame, checks: list[dict], glbs: dict[str, Any] | None = None
    ) -> DataFrame:
        dq_rule_checks = self.build_checks_by_metadata(checks, glbs)

        return self.apply_checks(df, dq_rule_checks)

    @staticmethod
    def validate_checks(checks: list[dict], glbs: dict[str, Any] | None = None) -> ChecksValidationStatus:
        status = ChecksValidationStatus()

        for check in checks:
            logger.debug(f"Processing check definition: {check}")
            if isinstance(check, dict):
                status.add_errors(DQEngineCore._validate_checks_dict(check, glbs))
            else:
                status.add_error(f"Unsupported check type: {type(check)}")

        return status

    def get_invalid(self, df: DataFrame) -> DataFrame:
        return df.where(
            F.col(self._column_names[ColumnArguments.ERRORS]).isNotNull()
            | F.col(self._column_names[ColumnArguments.WARNINGS]).isNotNull()
        )

    def get_valid(self, df: DataFrame) -> DataFrame:
        return df.where(F.col(self._column_names[ColumnArguments.ERRORS]).isNull()).drop(
            self._column_names[ColumnArguments.ERRORS], self._column_names[ColumnArguments.WARNINGS]
        )

    @staticmethod
    def load_checks_from_local_file(filepath: str) -> list[dict]:
        if not filepath:
            raise ValueError("filepath must be provided")

        try:
            checks = Installation.load_local(list[dict[str, str]], Path(filepath))
            return deserialize_dicts(checks)
        except FileNotFoundError:
            msg = f"Checks file {filepath} missing"
            raise FileNotFoundError(msg) from None

    @staticmethod
    def save_checks_in_local_file(checks: list[dict], filepath: str):
        if not filepath:
            raise ValueError("filepath must be provided")

        try:
            with open(filepath, 'w', encoding="utf-8") as file:
                yaml.safe_dump(checks, file)
        except FileNotFoundError:
            msg = f"Checks file {filepath} missing"
            raise FileNotFoundError(msg) from None

    @staticmethod
    def build_checks_by_metadata(checks: list[dict], glbs: dict[str, Any] | None = None) -> list[DQRule]:
        """Build checks based on check specification, i.e. function name plus arguments.

        :param checks: list of dictionaries describing checks. Each check is a dictionary consisting of following fields:
        * `check` - Column expression to evaluate. This expression should return string value if it's evaluated to true -
        it will be used as an error/warning message, or `null` if it's evaluated to `false`
        * `name` - name that will be given to a resulting column. Autogenerated if not provided
        * `criticality` (optional) - possible values are `error` (data going only into "bad" dataframe),
        and `warn` (data is going into both dataframes)
        :param glbs: dictionary with functions mapping (eg. ``globals()`` of the calling module).
        If not specified, then only built-in functions are used for the checks.
        :return: list of data quality check rules
        """
        status = DQEngineCore.validate_checks(checks, glbs)
        if status.has_errors:
            raise ValueError(str(status))

        dq_rule_checks = []
        for check_def in checks:
            logger.debug(f"Processing check definition: {check_def}")
            check = check_def.get("check", {})
            func_name = check.get("function", None)
            func = DQEngineCore._resolve_function(func_name, glbs, fail_on_missing=True)
            assert func  # should already be validated
            func_args = check.get("arguments", {})
            criticality = check_def.get("criticality", "error")
            filter_expr = check_def.get("filter")

            if "col_names" in func_args:
                logger.debug(f"Adding DQRuleColSet with columns: {func_args['col_names']}")
                dq_rule_checks += DQRuleColSet(
                    columns=func_args["col_names"],
                    check_func=func,
                    criticality=criticality,
                    filter=filter_expr,
                    # provide arguments without "col_names"
                    check_func_kwargs={k: func_args[k] for k in func_args.keys() - {"col_names"}},
                ).get_rules()
            else:
                name = check_def.get("name", None)
                check_func = func(**func_args)
                dq_rule_checks.append(DQRule(check=check_func, name=name, criticality=criticality, filter=filter_expr))

        logger.debug("Exiting build_checks_by_metadata function with dq_rule_checks")
        return dq_rule_checks

    @staticmethod
    def build_checks(*rules_col_set: DQRuleColSet) -> list[DQRule]:
        """
        Build rules from dq rules and rule sets.

        :param rules_col_set: list of dq rules which define multiple columns for the same check function
        :return: list of dq rules
        """
        rules_nested = [rule_set.get_rules() for rule_set in rules_col_set]
        flat_rules = list(itertools.chain(*rules_nested))

        return list(filter(None, flat_rules))

    @staticmethod
    def _get_check_columns(checks: list[DQRule], criticality: str) -> list[DQRule]:
        """Get check columns based on criticality.

        :param checks: list of checks to apply to the dataframe
        :param criticality: criticality
        :return: list of check columns
        """
        return [check for check in checks if check.rule_criticality == criticality]

    def _append_empty_checks(self, df: DataFrame) -> DataFrame:
        """Append empty checks at the end of dataframe.

        :param df: dataframe without checks
        :return: dataframe with checks
        """
        return df.select(
            "*",
            F.lit(None).cast("map<string, string>").alias(self._column_names[ColumnArguments.ERRORS]),
            F.lit(None).cast("map<string, string>").alias(self._column_names[ColumnArguments.WARNINGS]),
        )

    @staticmethod
    def _create_results_map(df: DataFrame, checks: list[DQRule], dest_col: str) -> DataFrame:
        """ ""Create a map from the values of the specified columns, using the column names as a key.  This function is
        used to collect individual check columns into corresponding errors and/or warnings columns.

        :param df: dataframe with added check columns
        :param checks: list of checks to apply to the dataframe
        :param dest_col: name of the map column
        """
        empty_type = F.lit(None).cast("map<string, string>").alias(dest_col)
        if len(checks) == 0:
            return df.select("*", empty_type)

        name_cols = []
        check_cols = []
        for check in checks:
            check_cols.append(check.check_column())
            name_cols.append(F.lit(check.name))

        m_col = F.map_from_arrays(F.array(*name_cols), F.array(*check_cols))
        m_col = F.map_filter(m_col, lambda _, v: v.isNotNull())
        return df.withColumn(dest_col, F.when(F.size(m_col) > 0, m_col).otherwise(empty_type))

    @staticmethod
    def _validate_checks_dict(check: dict, glbs: dict[str, Any] | None) -> list[str]:
        """
        Validates the structure and content of a given check dictionary.

        Args:
            check (dict): The dictionary to validate.
            glbs (dict[str, Any] | None): A dictionary of global variables, or None.

        Returns:
            list[str]: The updated list of error messages.
        """
        errors: list[str] = []

        if "criticality" in check and check["criticality"] not in [c.value for c in Criticality]:
            errors.append(f"Invalid value for 'criticality' field: {check}")

        if "check" not in check:
            errors.append(f"'check' field is missing: {check}")
        elif not isinstance(check["check"], dict):
            errors.append(f"'check' field should be a dictionary: {check}")
        else:
            errors.extend(DQEngineCore._validate_check_block(check, glbs))

        return errors

    @staticmethod
    def _validate_check_block(check: dict, glbs: dict[str, Any] | None) -> list[str]:
        """
        Validates a check block within a configuration.

        Args:
            check (dict): The entire check configuration.
            glbs (dict[str, Any] | None): A dictionary of global functions or None.

        Returns:
            list[str]: The updated list of error messages.
        """
        check_block = check["check"]

        if "function" not in check_block:
            return [f"'function' field is missing in the 'check' block: {check}"]

        func_name = check_block["function"]
        func = DQEngineCore._resolve_function(func_name, glbs, fail_on_missing=False)
        if not callable(func):
            return [f"function '{func_name}' is not defined: {check}"]

        arguments = check_block.get("arguments", {})
        return DQEngineCore._validate_check_function_arguments(arguments, func, check)

    @staticmethod
    def _validate_check_function_arguments(arguments: dict, func: Callable, check: dict) -> list[str]:
        """
        Validates the provided arguments for a given function and updates the errors list if any validation fails.

        Args:
            arguments (dict): The arguments to validate.
            func (Callable): The function for which the arguments are being validated.
            check (dict): A dictionary containing the validation checks.

        Returns:
            list[str]: The updated list of error messages.
        """
        if not isinstance(arguments, dict):
            return [f"'arguments' should be a dictionary in the 'check' block: {check}"]

        if "col_names" in arguments:
            if not isinstance(arguments["col_names"], list):
                return [f"'col_names' should be a list in the 'arguments' block: {check}"]

            if len(arguments["col_names"]) == 0:
                return [f"'col_names' should not be empty in the 'arguments' block: {check}"]

            arguments = {
                'col_name' if k == 'col_names' else k: arguments['col_names'][0] if k == 'col_names' else v
                for k, v in arguments.items()
            }
            return DQEngineCore._validate_func_args(arguments, func, check)

        return DQEngineCore._validate_func_args(arguments, func, check)

    @staticmethod
    def _validate_func_args(arguments: dict, func: Callable, check: dict) -> list[str]:
        """
        Validates the arguments passed to a function against its signature.
        Args:
            arguments (dict): A dictionary of argument names and their values to be validated.
            func (Callable): The function whose arguments are being validated.
            check (dict): A dictionary containing additional context or information for error messages.
        Returns:
            list[str]: The updated list of error messages after validation.
        """

        @ft.lru_cache(None)
        def cached_signature(check_func):
            return inspect.signature(check_func)

        errors: list[str] = []
        sig = cached_signature(func)
        if not arguments and sig.parameters:
            errors.append(
                f"No arguments provided for function '{func.__name__}' in the 'arguments' block: {check}. "
                f"Expected arguments are: {list(sig.parameters.keys())}"
            )
        for arg, value in arguments.items():
            if arg not in sig.parameters:
                expected_args = list(sig.parameters.keys())
                errors.append(
                    f"Unexpected argument '{arg}' for function '{func.__name__}' in the 'arguments' block: {check}. "
                    f"Expected arguments are: {expected_args}"
                )
            else:
                expected_type = sig.parameters[arg].annotation
                if expected_type is not inspect.Parameter.empty and not isinstance(value, expected_type):
                    errors.append(
                        f"Argument '{arg}' should be of type '{expected_type.__name__}' for function '{func.__name__}' "
                        f"in the 'arguments' block: {check}"
                    )
        return errors

    @staticmethod
    def _resolve_function(func_name: str, glbs: dict[str, Any] | None = None, fail_on_missing=True) -> Callable | None:
        logger.debug(f"Resolving function: {func_name}")
        if glbs:
            func = glbs.get(func_name)
        elif fail_on_missing:
            func = getattr(col_functions, func_name)
        else:
            func = getattr(col_functions, func_name, None)
        logger.debug(f"Function {func_name} resolved successfully")
        return func


class DQEngine(DQEngineBase):
    """Data Quality Engine class to apply data quality checks to a given dataframe."""

    def __init__(
        self,
        workspace_client: WorkspaceClient,
        engine: DQEngineCoreBase | None = None,
        extra_params: ExtraParams | None = None,
    ):
        super().__init__(workspace_client)
        self._engine = engine or DQEngineCore(workspace_client, extra_params)

    def apply_checks(self, df: DataFrame, checks: list[DQRule]) -> DataFrame:
        """Applies data quality checks to a given dataframe.

        :param df: dataframe to check
        :param checks: list of checks to apply to the dataframe. Each check is an instance of DQRule class.
        :return: dataframe with errors and warning reporting columns
        """
        return self._engine.apply_checks(df, checks)

    def apply_checks_and_split(self, df: DataFrame, checks: list[DQRule]) -> tuple[DataFrame, DataFrame]:
        """Applies data quality checks to a given dataframe and split it into two ("good" and "bad"),
        according to the data quality checks.

        :param df: dataframe to check
        :param checks: list of checks to apply to the dataframe. Each check is an instance of DQRule class.
        :return: two dataframes - "good" which includes warning rows but no reporting columns, and "data" having
        error and warning rows and corresponding reporting columns
        """
        return self._engine.apply_checks_and_split(df, checks)

    def apply_checks_by_metadata_and_split(
        self, df: DataFrame, checks: list[dict], glbs: dict[str, Any] | None = None
    ) -> tuple[DataFrame, DataFrame]:
        """Wrapper around `apply_checks_and_split` for use in the metadata-driven pipelines. The main difference
        is how the checks are specified - instead of using functions directly, they are described as function name plus
        arguments.

        :param df: dataframe to check
        :param checks: list of dictionaries describing checks. Each check is a dictionary consisting of following fields:
        * `check` - Column expression to evaluate. This expression should return string value if it's evaluated to true -
        it will be used as an error/warning message, or `null` if it's evaluated to `false`
        * `name` - name that will be given to a resulting column. Autogenerated if not provided
        * `criticality` (optional) - possible values are `error` (data going only into "bad" dataframe),
        and `warn` (data is going into both dataframes)
        :param glbs: dictionary with functions mapping (eg. ``globals()`` of the calling module).
        If not specified, then only built-in functions are used for the checks.
        :return: two dataframes - "good" which includes warning rows but no reporting columns, and "bad" having
        error and warning rows and corresponding reporting columns
        """
        return self._engine.apply_checks_by_metadata_and_split(df, checks, glbs)

    def apply_checks_by_metadata(
        self, df: DataFrame, checks: list[dict], glbs: dict[str, Any] | None = None
    ) -> DataFrame:
        """Wrapper around `apply_checks` for use in the metadata-driven pipelines. The main difference
        is how the checks are specified - instead of using functions directly, they are described as function name plus
        arguments.

        :param df: dataframe to check
        :param checks: list of dictionaries describing checks. Each check is a dictionary consisting of following fields:
        * `check` - Column expression to evaluate. This expression should return string value if it's evaluated to true -
        it will be used as an error/warning message, or `null` if it's evaluated to `false`
        * `name` - name that will be given to a resulting column. Autogenerated if not provided
        * `criticality` (optional) - possible values are `error` (data going only into "bad" dataframe),
        and `warn` (data is going into both dataframes)
        :param glbs: dictionary with functions mapping (eg. ``globals()`` of calling module).
        If not specified, then only built-in functions are used for the checks.
        :return: dataframe with errors and warning reporting columns
        """
        return self._engine.apply_checks_by_metadata(df, checks, glbs)

    @staticmethod
    def validate_checks(checks: list[dict], glbs: dict[str, Any] | None = None) -> ChecksValidationStatus:
        """
        Validate the input dict to ensure they conform to expected structure and types.

        Each check can be a dictionary. The function validates
        the presence of required keys, the existence and callability of functions, and the types
        of arguments passed to these functions.

        :param checks: List of checks to apply to the dataframe. Each check should be a dictionary.
        :param glbs: Optional dictionary of global functions that can be used in checks.

        :return ValidationStatus: The validation status.
        """
        return DQEngineCore.validate_checks(checks, glbs)

    def get_invalid(self, df: DataFrame) -> DataFrame:
        """
        Get records that violate data quality checks (records with warnings and errors).
        @param df: input DataFrame.
        @return: dataframe with error and warning rows and corresponding reporting columns.
        """
        return self._engine.get_invalid(df)

    def get_valid(self, df: DataFrame) -> DataFrame:
        """
        Get records that don't violate data quality checks (records with warnings but no errors).
        @param df: input DataFrame.
        @return: dataframe with warning rows but no reporting columns.
        """
        return self._engine.get_valid(df)

    @staticmethod
    def load_checks_from_local_file(filepath: str) -> list[dict]:
        """
        Load checks (dq rules) from a file (json or yml) in the local filesystem.

        :param filepath: path to the file containing the checks.
        :return: list of dq rules or raise an error if checks file is missing or is invalid.
        """
        parsed_checks = DQEngineCore.load_checks_from_local_file(filepath)
        if not parsed_checks:
            raise ValueError(f"Invalid or no checks in file: {filepath}")
        return parsed_checks

    def load_checks_from_workspace_file(self, workspace_path: str) -> list[dict]:
        """Load checks (dq rules) from a file (json or yml) in the workspace.
        This does not require installation of DQX in the workspace.
        The returning checks can be used as input for `apply_checks_by_metadata` function.

        :param workspace_path: path to the file in the workspace.
        :return: list of dq rules or raise an error if checks file is missing or is invalid.
        """
        workspace_dir = os.path.dirname(workspace_path)
        filename = os.path.basename(workspace_path)
        installation = Installation(self.ws, "dqx", install_folder=workspace_dir)

        logger.info(f"Loading quality rules (checks) from {workspace_path} in the workspace.")
        parsed_checks = self._load_checks_from_file(installation, filename)
        if not parsed_checks:
            raise ValueError(f"Invalid or no checks in workspace file: {workspace_path}")
        return parsed_checks

    def load_checks_from_installation(
        self, run_config_name: str | None = "default", product_name: str = "dqx", assume_user: bool = True
    ) -> list[dict]:
        """
        Load checks (dq rules) from a file (json or yml) defined in the installation config.
        The returning checks can be used as input for `apply_checks_by_metadata` function.

        :param run_config_name: name of the run (config) to use
        :param product_name: name of the product/installation directory
        :param assume_user: if True, assume user installation
        :return: list of dq rules or raise an error if checks file is missing or is invalid.
        """
        installation = self._get_installation(assume_user, product_name)
        run_config = self._load_run_config(installation, run_config_name)
        filename = run_config.checks_file or "checks.yml"

        logger.info(f"Loading quality rules (checks) from {installation.install_folder()}/{filename} in the workspace.")
        parsed_checks = self._load_checks_from_file(installation, filename)
        if not parsed_checks:
            raise ValueError(f"Invalid or no checks in workspace file: {installation.install_folder()}/{filename}")
        return parsed_checks

    @staticmethod
    def save_checks_in_local_file(checks: list[dict], path: str):
        return DQEngineCore.save_checks_in_local_file(checks, path)

    def save_checks_in_installation(
        self,
        checks: list[dict],
        run_config_name: str | None = "default",
        product_name: str = "dqx",
        assume_user: bool = True,
    ):
        """
        Save checks (dq rules) to yml file in the installation folder.

        :param checks: list of dq rules to save
        :param run_config_name: name of the run (config) to use
        :param product_name: name of the product/installation directory
        :param assume_user: if True, assume user installation
        """
        installation = self._get_installation(assume_user, product_name)
        run_config = self._load_run_config(installation, run_config_name)

        logger.info(
            f"Saving quality rules (checks) to {installation.install_folder()}/{run_config.checks_file} "
            f"in the workspace."
        )
        installation.upload(run_config.checks_file, yaml.safe_dump(checks).encode('utf-8'))

    def save_checks_in_workspace_file(self, checks: list[dict], workspace_path: str):
        """Save checks (dq rules) to yml file in the workspace.
        This does not require installation of DQX in the workspace.

        :param checks: list of dq rules to save
        :param workspace_path: destination path to the file in the workspace.
        """
        workspace_dir = os.path.dirname(workspace_path)

        logger.info(f"Saving quality rules (checks) to {workspace_path} in the workspace.")
        self.ws.workspace.mkdirs(workspace_dir)
        self.ws.workspace.upload(
            workspace_path, yaml.safe_dump(checks).encode('utf-8'), format=ImportFormat.AUTO, overwrite=True
        )

    def load_run_config(
        self, run_config_name: str | None = "default", assume_user: bool = True, product_name: str = "dqx"
    ) -> RunConfig:
        """
        Load run configuration from the installation.

        :param run_config_name: name of the run configuration to use
        :param assume_user: if True, assume user installation
        :param product_name: name of the product
        """
        installation = self._get_installation(assume_user, product_name)
        return self._load_run_config(installation, run_config_name)

    def _get_installation(self, assume_user, product_name):
        if assume_user:
            installation = Installation.assume_user_home(self.ws, product_name)
        else:
            installation = Installation.assume_global(self.ws, product_name)

        # verify the installation
        installation.current(self.ws, product_name, assume_user=assume_user)
        return installation

    @staticmethod
    def _load_run_config(installation, run_config_name):
        """Load run configuration from the installation."""
        config = installation.load(WorkspaceConfig)
        return config.get_run_config(run_config_name)

    @staticmethod
    def _load_checks_from_file(installation: Installation, filename: str) -> list[dict]:
        try:
            checks = installation.load(list[dict[str, str]], filename=filename)
            return deserialize_dicts(checks)
        except NotFound:
            msg = f"Checks file {filename} missing"
            raise NotFound(msg) from None
