import pytest
from databricks.labs.dqx.engine import DQEngine


TEST_CHECKS = [
    {"criticality": "error", "check": {"function": "is_not_null", "arguments": {"col_names": ["col1", "col2"]}}}
]


def test_save_checks_to_local_file(make_local_check_file_as_yml):
    file = make_local_check_file_as_yml
    DQEngine.save_checks_in_local_file(TEST_CHECKS, file)
    checks = DQEngine.load_checks_from_local_file(file)
    assert checks == TEST_CHECKS, "The loaded checks do not match the expected checks."


def test_save_checks_to_local_file_when_filename_is_empty():
    with pytest.raises(ValueError, match="filepath must be provided"):
        DQEngine.save_checks_in_local_file(TEST_CHECKS, "")
