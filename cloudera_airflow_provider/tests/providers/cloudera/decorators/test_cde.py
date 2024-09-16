#  Cloudera Airflow Provider
#  (C) Cloudera, Inc. 2021-2022
#  All rights reserved.
#  Applicable Open Source License: Apache License Version 2.0
#
#  NOTE: Cloudera open source products are modular software products
#  made up of hundreds of individual components, each of which was
#  individually copyrighted.  Each Cloudera open source product is a
#  collective work under U.S. Copyright Law. Your license to use the
#  collective work is as provided in your written agreement with
#  Cloudera.  Used apart from the collective work, this file is
#  licensed for your use pursuant to the open source license
#  identified above.
#
#  This code is provided to you pursuant a written agreement with
#  (i) Cloudera, Inc. or (ii) a third-party authorized to distribute
#  this code. If you do not have a written agreement with Cloudera nor
#  with an authorized and properly licensed third party, you do not
#  have any rights to access nor to use this code.
#
#  Absent a written agreement with Cloudera, Inc. (“Cloudera”) to the
#  contrary, A) CLOUDERA PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY
#  KIND; (B) CLOUDERA DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED
#  WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT LIMITED TO
#  IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE; (C) CLOUDERA IS NOT LIABLE TO YOU,
#  AND WILL NOT DEFEND, INDEMNIFY, NOR HOLD YOU HARMLESS FOR ANY CLAIMS
#  ARISING FROM OR RELATED TO THE CODE; AND (D)WITH RESPECT TO YOUR EXERCISE
#  OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, CLOUDERA IS NOT LIABLE FOR ANY
#  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR
#  CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO, DAMAGES
#  RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF
#  BUSINESS ADVANTAGE OR UNAVAILABILITY, OR LOSS OR CORRUPTION OF
#  DATA.

"""Tests related to the CDE Job operator"""

from __future__ import annotations

import unittest
from datetime import datetime
from unittest import mock

import pytest
from parameterized import parameterized

from airflow.decorators import task
from airflow.models.baseoperator import BaseOperator
from airflow.models.connection import Connection
from airflow.models.dag import DAG
from airflow.models.taskinstance import TaskInstance
from cloudera.airflow.providers.hooks.cde import CdeHook
from cloudera.airflow.providers.operators.cde import CdeRunJobOperator

TEST_JOB_NAME = "testjob"
TEST_JOB_RUN_ID = 10
TEST_AIRFLOW_DAG_ID = "dag_1"
TEST_AIRFLOW_RUN_ID = "run_1"
TEST_AIRFLOW_RUN_EXECUTION_DATE = datetime.now()
TEST_AIRFLOW_TASK_ID = "task_1"
TEST_TIMEOUT = 4
TEST_JOB_POLL_INTERVAL = 1
TEST_API_RETRIES = 3
TEST_API_TIMEOUT = 5
TEST_VARIABLES = {"var1": "someval_{{ ds_nodash }}"}
TEST_OVERRIDES = {"spark": {"conf": {"myparam": "val_{{ ds_nodash }}"}}}
TEST_CONTEXT = {
    "ds": "2020-11-25",
    "ds_nodash": "20201125",
    "ts": "2020-11-25T00:00:00+00:00",
    "ts_nodash": "20201125T000000",
    "run_id": TEST_AIRFLOW_RUN_ID,
}
TEST_HOST = "vc1.cde-2.cdp-3.cloudera.site"
TEST_SCHEME = "http"
TEST_PORT = 9090
TEST_AK = "access_key"
TEST_PK = "private_key"
TEST_CUSTOM_CA_CERTIFICATE = "/ca_cert/letsencrypt-stg-root-x1.pem"
TEST_EXTRA = (
    f'{{"access_key": "{TEST_AK}", "private_key": "{TEST_PK}", "ca_cert": "{TEST_CUSTOM_CA_CERTIFICATE}"}}'
)

TEST_DEFAULT_CONNECTION_DICT = {
    "conn_id": CdeHook.DEFAULT_CONN_ID,
    "conn_type": "http",
    "host": TEST_HOST,
    "port": TEST_PORT,
    "schema": TEST_SCHEME,
    "extra": TEST_EXTRA,
}

TEST_DEFAULT_CONNECTION = Connection(
    conn_id=CdeHook.DEFAULT_CONN_ID,
    conn_type="http",
    host=TEST_HOST,
    port=TEST_PORT,
    schema=TEST_SCHEME,
    extra=TEST_EXTRA,
)


def mock_task_instance_for_context():
    """Mocks task_instance for test context."""
    TEST_CONTEXT["task_instance"] = TaskInstance(
        execution_date=TEST_AIRFLOW_RUN_EXECUTION_DATE,
        task=BaseOperator(
            task_id=TEST_AIRFLOW_TASK_ID, dag=DAG(TEST_AIRFLOW_DAG_ID, start_date=datetime.now())
        ),
    )


@mock.patch('sqlalchemy.orm.Query.scalar', return_value=TEST_AIRFLOW_RUN_ID)
@mock.patch.object(CdeHook, "submit_job", return_value=TEST_JOB_RUN_ID)
@mock.patch.object(CdeHook, "check_job_run_status", side_effect=["starting", "running", "succeeded"])
@mock.patch.object(CdeHook, "get_connection", return_value=TEST_DEFAULT_CONNECTION)
class CdeRunJobTaskTest(unittest.TestCase):
    """Unit tests for @task.cde"""

    @parameterized.expand(
        [
            [
                ["invalid", "return"],
                r"The returned parameters from the TaskFlow callable must be a Union.*Got.*'list'.*",
            ],
            [
                {"job_name": ["invalid"]},
                r"The returned parameter='job_name' from the TaskFlow callable.*Got.*'list'.*",
            ],
            [
                {"job_name": TEST_JOB_NAME, "variables": "invalid"},
                r"The returned parameter='variables' from the TaskFlow callable.*Got.*'str'.*",
            ],
            [
                {"job_name": TEST_JOB_NAME, "overrides": "invalid"},
                r"The returned parameter='overrides' from the TaskFlow callable.*Got.*'str'.*",
            ],
            [
                {"job_name": TEST_JOB_NAME, "connection_id": {}},
                r"The returned parameter='connection_id' from the TaskFlow callable.*Got.*'dict'.*",
            ],
            [
                {"job_name": TEST_JOB_NAME, "wait": {}},
                r"The returned parameter='wait' from the TaskFlow callable.*Got.*'dict'.*",
            ],
            [
                {"job_name": TEST_JOB_NAME, "timeout": {}},
                r"The returned parameter='timeout' from the TaskFlow callable.*Got.*'dict'.*",
            ],
            [
                {"job_name": TEST_JOB_NAME, "job_poll_interval": {}},
                r"The returned parameter='job_poll_interval' from the TaskFlow callable.*Got.*'dict'.*",
            ],
            [
                {"job_name": TEST_JOB_NAME, "api_retries": {}},
                r"The returned parameter='api_retries' from the TaskFlow callable.*Got.*'dict'.*",
            ],
            [
                {"job_name": TEST_JOB_NAME, "api_timeout": {}},
                r"The returned parameter='api_timeout' from the TaskFlow callable.*Got.*'dict'.*",
            ],
        ]
    )
    def test_task_fail_invalid_return_types(
        self, get_connection, check_job_run_status, submit_job, scalar, return_value, expected_error
    ):
        """Test invalid return types for the python callable."""

        # pylint: disable=unused-argument
        @task.cde
        def task_cde():
            return return_value

        mock_task_instance_for_context()
        operator = task_cde().operator  # pylint: disable=E1101
        with pytest.raises(TypeError, match=expected_error):
            operator.execute(TEST_CONTEXT)

    def test_task_fail_job_name_required(self, get_connection, check_job_run_status, submit_job, scalar):
        """Test missing job name."""

        # pylint: disable=unused-argument
        @task.cde
        def task_cde():
            return ""

        mock_task_instance_for_context()
        operator = task_cde().operator  # pylint: disable=E1101
        with pytest.raises(ValueError, match=r"job_name is required"):
            operator.execute(TEST_CONTEXT)

    def test_task_default_args(self, get_connection, check_job_run_status, submit_job, scalar):
        """Test the default arguments with a returned job name."""

        # pylint: disable=unused-argument
        @task.cde
        def task_cde():
            return TEST_JOB_NAME

        mock_task_instance_for_context()
        operator = task_cde().operator  # pylint: disable=E1101
        operator.execute(TEST_CONTEXT)
        self.assert_defaults(operator)

    def test_task_default_args_decorator(self, get_connection, check_job_run_status, submit_job, scalar):
        """Test that job name can be specified in the decorator arguments."""

        # pylint: disable=unused-argument
        @task.cde(job_name=TEST_JOB_NAME)
        def task_cde():
            return None

        mock_task_instance_for_context()
        operator = task_cde().operator  # pylint: disable=E1101
        operator.execute(TEST_CONTEXT)
        self.assert_defaults(operator)

    def test_task_args_decorator(self, get_connection, check_job_run_status, submit_job, scalar):
        """Test task decorator arguments."""

        # pylint: disable=unused-argument
        @task.cde(
            job_name=TEST_JOB_NAME,
            variables=TEST_VARIABLES,
            overrides=TEST_OVERRIDES,
            connection_id="connection_id",
            wait=False,
            timeout=TEST_TIMEOUT,
            job_poll_interval=TEST_JOB_POLL_INTERVAL,
            api_retries=TEST_API_RETRIES,
            api_timeout=TEST_API_TIMEOUT,
        )
        def task_cde():
            return None

        mock_task_instance_for_context()
        operator = task_cde().operator  # pylint: disable=E1101
        operator.execute(TEST_CONTEXT)
        self.assert_custom(operator)

    def test_task_args_precedence(self, get_connection, check_job_run_status, submit_job, scalar):
        """Test returned parameters takes precedence over decorator's arguments."""

        # pylint: disable=unused-argument
        @task.cde(
            job_name="job",
            variables={},
            overrides={},
            connection_id="connection_id_2",
            wait=True,
            timeout=11,
            job_poll_interval=22,
            api_retries=33,
            api_timeout=44,
        )
        def task_cde():
            return {
                "job_name": TEST_JOB_NAME,
                "variables": TEST_VARIABLES,
                "overrides": TEST_OVERRIDES,
                "connection_id": "connection_id",
                "wait": False,
                "timeout": TEST_TIMEOUT,
                "job_poll_interval": TEST_JOB_POLL_INTERVAL,
                "api_retries": TEST_API_RETRIES,
                "api_timeout": TEST_API_TIMEOUT,
                "user": 'silently_ignored',
                "abc": "sliently_ignored",
            }

        mock_task_instance_for_context()
        operator = task_cde().operator  # pylint: disable=E1101
        operator.execute(TEST_CONTEXT)
        self.assert_custom(operator)

    def assert_defaults(self, operator):
        """Asserts the default operator properties."""
        self.assertEqual(operator.job_name, TEST_JOB_NAME)
        self.assertEqual(operator.variables, {})
        self.assertEqual(operator.overrides, {})
        self.assertEqual(operator.connection_id, CdeRunJobOperator.DEFAULT_CONNECTION_ID)
        self.assertEqual(operator.wait, CdeRunJobOperator.DEFAULT_WAIT)
        self.assertEqual(operator.timeout, CdeRunJobOperator.DEFAULT_TIMEOUT)
        self.assertEqual(operator.job_poll_interval, CdeRunJobOperator.DEFAULT_POLL_INTERVAL)
        self.assertEqual(operator.api_retries, None)
        self.assertEqual(operator.api_timeout, None)
        self.assertEqual(operator.user, None)

    def assert_custom(self, operator):
        """Asserts the test operator properties."""
        self.assertEqual(operator.job_name, TEST_JOB_NAME)
        self.assertEqual(operator.variables, TEST_VARIABLES)
        self.assertEqual(operator.overrides, TEST_OVERRIDES)
        self.assertEqual(operator.connection_id, "connection_id")
        self.assertEqual(operator.wait, False)
        self.assertEqual(operator.timeout, TEST_TIMEOUT)
        self.assertEqual(operator.job_poll_interval, TEST_JOB_POLL_INTERVAL)
        self.assertEqual(operator.api_retries, TEST_API_RETRIES)
        self.assertEqual(operator.api_timeout, TEST_API_TIMEOUT)
        self.assertEqual(operator.user, None)


if __name__ == "__main__":
    unittest.main()
