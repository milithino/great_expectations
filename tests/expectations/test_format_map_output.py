import datetime

import pandas as pd
from great_expectations.self_check.util import build_pandas_validator_with_data

from great_expectations.execution_engine import PandasExecutionEngine

from great_expectations.expectations.expectation import _format_map_output


def test_format_map_output_with_numbers():
    success = False
    element_count = 5
    nonnull_count = 5
    success_count = 0
    unexpected_list = [
        {"foreign_key_1": 1, "foreign_key_2": 2},
        {"foreign_key_1": 1, "foreign_key_2": 2},
        {"foreign_key_1": 1, "foreign_key_2": 2},
    ]
    unexpected_index_list = [1, 2, 3]
    assert _format_map_output(
        result_format={
            "result_format": "COMPLETE",
            "partial_unexpected_count": 20,
            "include_unexpected_rows": False,
        },
        success=success,
        element_count=element_count,
        nonnull_count=nonnull_count,
        unexpected_count=len(unexpected_list),
        unexpected_list=unexpected_list,
        unexpected_index_list=unexpected_index_list,
    ) == {
        "success": False,
        "result": {
            "element_count": 5,
            "missing_count": 0,
            "missing_percent": 0.0,
            "partial_unexpected_list": [
                {"foreign_key_1": 1, "foreign_key_2": 2},
                {"foreign_key_1": 1, "foreign_key_2": 2},
                {"foreign_key_1": 1, "foreign_key_2": 2},
            ],
            "unexpected_count": 3,
            "unexpected_percent": 60.0,
            "unexpected_percent_total": 60.0,
            "unexpected_percent_nonmissing": 60.0,
            "partial_unexpected_counts": [{"value": (1, 2), "count": 3}],
            "partial_unexpected_index_list": [1, 2, 3],
            "unexpected_list": [
                {"foreign_key_1": 1, "foreign_key_2": 2},
                {"foreign_key_1": 1, "foreign_key_2": 2},
                {"foreign_key_1": 1, "foreign_key_2": 2},
            ],
            "unexpected_index_list": [1, 2, 3],
        },
    }


def test_format_map_output_with_strings():
    success = False
    element_count = 5
    nonnull_count = 5
    success_count = 0
    unexpected_list = [
        {"foreign_key_1": "a", "foreign_key_2": 2},
        {"foreign_key_1": "a", "foreign_key_2": 2},
        {"foreign_key_1": "a", "foreign_key_2": 2},
    ]
    unexpected_index_list = [1, 2, 3]
    assert _format_map_output(
        result_format={
            "result_format": "COMPLETE",
            "partial_unexpected_count": 20,
            "include_unexpected_rows": False,
        },
        success=success,
        element_count=element_count,
        nonnull_count=nonnull_count,
        unexpected_count=len(unexpected_list),
        unexpected_list=unexpected_list,
        unexpected_index_list=unexpected_index_list,
    ) == {
        "success": False,
        "result": {
            "element_count": 5,
            "missing_count": 0,
            "missing_percent": 0.0,
            "partial_unexpected_list": [
                {"foreign_key_1": "a", "foreign_key_2": 2},
                {"foreign_key_1": "a", "foreign_key_2": 2},
                {"foreign_key_1": "a", "foreign_key_2": 2},
            ],
            "unexpected_count": 3,
            "unexpected_percent": 60.0,
            "unexpected_percent_total": 60.0,
            "unexpected_percent_nonmissing": 60.0,
            "partial_unexpected_counts": [{"value": ("a", 2), "count": 3}],
            "partial_unexpected_index_list": [1, 2, 3],
            "unexpected_list": [
                {"foreign_key_1": "a", "foreign_key_2": 2},
                {"foreign_key_1": "a", "foreign_key_2": 2},
                {"foreign_key_1": "a", "foreign_key_2": 2},
            ],
            "unexpected_index_list": [1, 2, 3],
        },
    }


def test_format_map_output_with_strings_two_matches():
    success = False
    element_count = 5
    nonnull_count = 5
    success_count = 0
    unexpected_list = [
        {"foreign_key_1": "a", "foreign_key_2": 2},
        {"foreign_key_1": "a", "foreign_key_2": 2},
        {"foreign_key_1": "b", "foreign_key_2": 3},
    ]
    unexpected_index_list = [1, 3]
    assert _format_map_output(
        result_format={
            "result_format": "COMPLETE",
            "partial_unexpected_count": 20,
            "include_unexpected_rows": False,
        },
        success=success,
        element_count=element_count,
        nonnull_count=nonnull_count,
        unexpected_count=len(unexpected_list),
        unexpected_list=unexpected_list,
        unexpected_index_list=unexpected_index_list,
    ) == {
        "success": False,
        "result": {
            "element_count": 5,
            "missing_count": 0,
            "missing_percent": 0.0,
            "partial_unexpected_list": [
                {"foreign_key_1": "a", "foreign_key_2": 2},
                {"foreign_key_1": "a", "foreign_key_2": 2},
                {"foreign_key_1": "b", "foreign_key_2": 3},
            ],
            "unexpected_count": 3,
            "unexpected_percent": 60.0,
            "unexpected_percent_total": 60.0,
            "unexpected_percent_nonmissing": 60.0,
            "partial_unexpected_counts": [
                {"value": ("a", 2), "count": 2},
                {"value": ("b", 3), "count": 1},
            ],
            "partial_unexpected_index_list": [1, 3],
            "unexpected_list": [
                {"foreign_key_1": "a", "foreign_key_2": 2},
                {"foreign_key_1": "a", "foreign_key_2": 2},
                {"foreign_key_1": "b", "foreign_key_2": 3},
            ],
            "unexpected_index_list": [1, 3],
        },
    }


def test_validator_pandas_df_issue_4295():
    df = pd.DataFrame(
        {
            "foreign_key_1": [1, 1, 1, 1, 1],
            "foreign_key_2": [1, 2, 2, 2, 3],
            "start_date": [
                datetime.date(2021, 1, 1),
                datetime.date(2021, 1, 1),
                datetime.date(2021, 1, 2),
                datetime.date(2021, 1, 2),
                datetime.date(2021, 1, 1),
            ],
        }
    )

    validator = build_pandas_validator_with_data(df=df)

    assert not pd.api.types.is_datetime64_any_dtype(
        validator.active_batch_data.dataframe.dtypes["start_date"]
    )

    result = validator.expect_compound_columns_to_be_unique(
        column_list=["foreign_key_1", "foreign_key_2", "start_date"]
    )

    assert result.to_json_dict() == {
        "exception_info": {
            "raised_exception": False,
            "exception_traceback": None,
            "exception_message": None,
        },
        "expectation_config": {
            "expectation_type": "expect_compound_columns_to_be_unique",
            "kwargs": {
                "column_list": ["foreign_key_1", "foreign_key_2", "start_date"],
                "batch_id": [],
            },
            "meta": {},
        },
        "success": False,
        "result": {
            "element_count": 5,
            "unexpected_count": 2,
            "unexpected_percent": 40.0,
            "partial_unexpected_list": [
                {
                    "foreign_key_1": 1,
                    "foreign_key_2": 2,
                    "start_date": "2021-01-02",
                },
                {
                    "foreign_key_1": 1,
                    "foreign_key_2": 2,
                    "start_date": "2021-01-02",
                },
            ],
            "missing_count": 0,
            "missing_percent": 0.0,
            "unexpected_percent_total": 40.0,
            "unexpected_percent_nonmissing": 40.0,
        },
        "meta": {},
    }

    # Let's try also converting our start_date column to datetime type

    validator.active_batch_data.dataframe["start_date"] = pd.to_datetime(
        validator.active_batch_data.dataframe["start_date"]
    )

    assert pd.api.types.is_datetime64_any_dtype(
        validator.active_batch_data.dataframe.dtypes["start_date"]
    )

    result = validator.expect_compound_columns_to_be_unique(
        column_list=["foreign_key_1", "foreign_key_2", "start_date"]
    )

    assert result.to_json_dict() == {
        "exception_info": {
            "raised_exception": False,
            "exception_traceback": None,
            "exception_message": None,
        },
        "expectation_config": {
            "expectation_type": "expect_compound_columns_to_be_unique",
            "kwargs": {
                "column_list": ["foreign_key_1", "foreign_key_2", "start_date"],
                "batch_id": [],
            },
            "meta": {},
        },
        "success": False,
        "result": {
            "element_count": 5,
            "unexpected_count": 2,
            "unexpected_percent": 40.0,
            "partial_unexpected_list": [
                {
                    "foreign_key_1": 1,
                    "foreign_key_2": 2,
                    "start_date": "2021-01-02T00:00:00",
                },
                {
                    "foreign_key_1": 1,
                    "foreign_key_2": 2,
                    "start_date": "2021-01-02T00:00:00",
                },
            ],
            "missing_count": 0,
            "missing_percent": 0.0,
            "unexpected_percent_total": 40.0,
            "unexpected_percent_nonmissing": 40.0,
        },
        "meta": {},
    }
