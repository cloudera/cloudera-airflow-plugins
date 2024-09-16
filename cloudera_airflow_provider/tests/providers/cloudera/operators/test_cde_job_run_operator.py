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

import os
import unittest
from datetime import datetime
from unittest import mock
from unittest.mock import Mock, call

from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.models.connection import Connection
from airflow.models.dag import DAG
from airflow.models.taskinstance import TaskInstance
from cloudera.airflow.providers.hooks.cde import CdeHook
from cloudera.airflow.providers.operators.cde import FORMAT_DATE_TIME, CdeRunJobOperator

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
# for airflow < 2.2.0 there's no run_id, so we use execution_date instead
VALID_REQUEST_IDS = [
    f"{TEST_AIRFLOW_DAG_ID}#{TEST_AIRFLOW_RUN_ID}#{TEST_AIRFLOW_TASK_ID}#1",
    f"{TEST_AIRFLOW_DAG_ID}#{TEST_AIRFLOW_RUN_EXECUTION_DATE.strftime(FORMAT_DATE_TIME)}"
    + f"#{TEST_AIRFLOW_TASK_ID}#1",
]
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
    """Mock the task instance for the context"""
    TEST_CONTEXT["task_instance"] = TaskInstance(
        execution_date=TEST_AIRFLOW_RUN_EXECUTION_DATE,
        task=BaseOperator(
            task_id=TEST_AIRFLOW_TASK_ID, dag=DAG(TEST_AIRFLOW_DAG_ID, start_date=datetime.now())
        ),
    )


@mock.patch.object(CdeHook, "get_connection", return_value=TEST_DEFAULT_CONNECTION)
class CdeRunJobOperatorTest(unittest.TestCase):
    """Test cases for CDE operator"""

    def test_init(self, get_connection: Mock):
        """Test constructor"""
        cde_operator = CdeRunJobOperator(
            task_id="task",
            job_name=TEST_JOB_NAME,
            variables=TEST_VARIABLES,
            overrides=TEST_OVERRIDES,
            api_retries=TEST_API_RETRIES,
            api_timeout=TEST_API_TIMEOUT,
        )
        get_connection.assert_called()
        self.assertEqual(cde_operator.job_name, TEST_JOB_NAME)
        self.assertDictEqual(cde_operator.variables, TEST_VARIABLES)
        self.assertDictEqual(cde_operator.overrides, TEST_OVERRIDES)
        self.assertEqual(cde_operator.connection_id, CdeRunJobOperator.DEFAULT_CONNECTION_ID)
        self.assertEqual(cde_operator.wait, CdeRunJobOperator.DEFAULT_WAIT)
        self.assertEqual(cde_operator.timeout, CdeRunJobOperator.DEFAULT_TIMEOUT)
        self.assertEqual(cde_operator.job_poll_interval, CdeRunJobOperator.DEFAULT_POLL_INTERVAL)
        self.assertEqual(cde_operator.api_retries, TEST_API_RETRIES)
        self.assertEqual(cde_operator.api_timeout, TEST_API_TIMEOUT)
        # Make sure that retries and timeout are passed to the hook object. Retry and timeout behaviours
        # are tested in CdeHook unit tests
        self.assertEqual(cde_operator.get_hook().num_retries, TEST_API_RETRIES)
        self.assertEqual(cde_operator.get_hook().api_timeout, TEST_API_TIMEOUT)

    @mock.patch.dict(os.environ, {"AIRFLOW__CDE__DEFAULT_NUM_RETRIES": "4"})
    @mock.patch.dict(os.environ, {"AIRFLOW__CDE__DEFAULT_API_TIMEOUT": "10"})
    def test_init_env(self, get_connection: Mock):
        """Test constructor if the timeout and retry values are set by env vars in the hook."""
        cde_operator = CdeRunJobOperator(
            task_id="task",
            job_name=TEST_JOB_NAME,
            variables=TEST_VARIABLES,
            overrides=TEST_OVERRIDES,
        )
        get_connection.assert_called()
        self.assertEqual(cde_operator.job_name, TEST_JOB_NAME)
        self.assertDictEqual(cde_operator.variables, TEST_VARIABLES)
        self.assertDictEqual(cde_operator.overrides, TEST_OVERRIDES)
        self.assertEqual(cde_operator.connection_id, CdeRunJobOperator.DEFAULT_CONNECTION_ID)
        self.assertEqual(cde_operator.wait, CdeRunJobOperator.DEFAULT_WAIT)
        self.assertEqual(cde_operator.timeout, CdeRunJobOperator.DEFAULT_TIMEOUT)
        self.assertEqual(cde_operator.job_poll_interval, CdeRunJobOperator.DEFAULT_POLL_INTERVAL)
        self.assertEqual(cde_operator.api_retries, None)  # the value will be set only int the hook
        self.assertEqual(cde_operator.api_timeout, None)  # the value will be set only int the hook
        # Make sure that retries and timeout are passed to the hook object. Retry and timeout behaviours
        # are tested in CdeHook unit tests
        self.assertEqual(cde_operator.get_hook().num_retries, 4)
        self.assertEqual(cde_operator.get_hook().api_timeout, 10)

    @mock.patch.dict(os.environ, {"AIRFLOW__CDE__DEFAULT_API_TIMEOUT": str(TEST_API_TIMEOUT + 2)})
    @mock.patch.dict(os.environ, {"AIRFLOW__CDE__DEFAULT_NUM_RETRIES": str(TEST_API_RETRIES + 2)})
    def test_init_override_env_value(self, get_connection: Mock):
        """Test if the constructor values override the env vars for the timeout and retry."""
        cde_operator = CdeRunJobOperator(
            task_id="task",
            job_name=TEST_JOB_NAME,
            variables=TEST_VARIABLES,
            overrides=TEST_OVERRIDES,
            api_retries=TEST_API_RETRIES,
            api_timeout=TEST_API_TIMEOUT,
        )
        get_connection.assert_called()
        self.assertEqual(cde_operator.job_name, TEST_JOB_NAME)
        self.assertDictEqual(cde_operator.variables, TEST_VARIABLES)
        self.assertDictEqual(cde_operator.overrides, TEST_OVERRIDES)
        self.assertEqual(cde_operator.connection_id, CdeRunJobOperator.DEFAULT_CONNECTION_ID)
        self.assertEqual(cde_operator.wait, CdeRunJobOperator.DEFAULT_WAIT)
        self.assertEqual(cde_operator.timeout, CdeRunJobOperator.DEFAULT_TIMEOUT)
        self.assertEqual(cde_operator.job_poll_interval, CdeRunJobOperator.DEFAULT_POLL_INTERVAL)
        self.assertEqual(cde_operator.api_retries, TEST_API_RETRIES)
        self.assertEqual(cde_operator.api_timeout, TEST_API_TIMEOUT)
        # Make sure that retries and timeout are passed to the hook object. Retry and timeout behaviours
        # are tested in CdeHook unit tests
        self.assertEqual(cde_operator.get_hook().num_retries, TEST_API_RETRIES)
        self.assertEqual(cde_operator.get_hook().api_timeout, TEST_API_TIMEOUT)

    # pylint: disable=unused-argument
    @mock.patch('sqlalchemy.orm.Query.scalar', return_value=TEST_AIRFLOW_RUN_ID)
    @mock.patch.object(CdeHook, "kill_job_run")
    @mock.patch.object(CdeHook, "submit_job", return_value=TEST_JOB_RUN_ID)
    @mock.patch.object(CdeHook, "check_job_run_status", side_effect=["starting", "running", "succeeded"])
    def test_execute_and_wait(self, check_job_mock, submit_mock, kill_job_mock, db_mock, get_connection):
        """Test executing a job run and waiting for success"""
        mock_task_instance_for_context()
        cde_operator = CdeRunJobOperator(
            task_id="task",
            job_name=TEST_JOB_NAME,
            variables=TEST_VARIABLES,
            overrides=TEST_OVERRIDES,
            timeout=TEST_TIMEOUT,
            job_poll_interval=TEST_JOB_POLL_INTERVAL,
        )
        get_connection.assert_called()
        cde_operator.execute(TEST_CONTEXT)
        called_args = submit_mock.call_args.kwargs
        self.assertIsInstance(called_args, dict)
        self.assertEqual(dict(called_args["variables"], **TEST_VARIABLES), called_args["variables"])
        self.validate_context_variables(called_args["variables"])
        self.validate_request_id(called_args["request_id"])
        self.assertDictEqual(TEST_OVERRIDES, called_args["overrides"])
        check_job_mock.assert_has_calls(
            [
                call(TEST_JOB_RUN_ID),
                call(TEST_JOB_RUN_ID),
                call(TEST_JOB_RUN_ID),
            ]
        )
        kill_job_mock.assert_not_called()

    @mock.patch('sqlalchemy.orm.Query.scalar', return_value=TEST_AIRFLOW_RUN_ID)
    @mock.patch.object(CdeHook, "kill_job_run")
    @mock.patch.object(CdeHook, "submit_job", return_value=TEST_JOB_RUN_ID)
    @mock.patch.object(CdeHook, "check_job_run_status")
    def test_execute_and_do_not_wait(
        self, check_job_mock, submit_mock, kill_job_mock, db_mock, get_connection
    ):
        """Test executing a job and not waiting"""
        mock_task_instance_for_context()
        cde_operator = CdeRunJobOperator(
            task_id="task",
            job_name=TEST_JOB_NAME,
            variables=TEST_VARIABLES,
            overrides=TEST_OVERRIDES,
            timeout=TEST_TIMEOUT,
            job_poll_interval=TEST_JOB_POLL_INTERVAL,
            wait=False,
        )
        get_connection.assert_called()
        cde_operator.execute(TEST_CONTEXT)
        called_args = submit_mock.call_args.kwargs
        self.assertEqual(dict(called_args["variables"], **TEST_VARIABLES), called_args["variables"])
        self.validate_context_variables(called_args["variables"])
        self.validate_request_id(called_args["request_id"])
        self.assertDictEqual(TEST_OVERRIDES, called_args["overrides"])
        check_job_mock.assert_not_called()
        kill_job_mock.assert_not_called()

    @mock.patch.object(CdeHook, "kill_job_run")
    def test_on_kill(self, kill_job_mock, get_connection):
        """Test killing a running job"""
        cde_operator = CdeRunJobOperator(
            task_id="task",
            job_name=TEST_JOB_NAME,
            variables=TEST_VARIABLES,
            overrides=TEST_OVERRIDES,
            timeout=TEST_TIMEOUT,
            job_poll_interval=TEST_JOB_POLL_INTERVAL,
        )
        get_connection.assert_called()
        cde_operator._job_run_id = 1  # pylint: disable=W0212
        cde_operator.on_kill()
        kill_job_mock.assert_called()
        self.assertTrue(cde_operator._job_run_finished)  # pylint: disable=W0212

    @mock.patch.object(CdeHook, "check_job_run_status", return_value="starting")
    def test_wait_for_job_times_out(self, check_job_mock, get_connection):
        """Test a job run timeout"""
        cde_operator = CdeRunJobOperator(
            task_id="task",
            job_name=TEST_JOB_NAME,
            timeout=TEST_TIMEOUT,
            job_poll_interval=TEST_JOB_POLL_INTERVAL,
        )
        get_connection.assert_called()
        try:
            cde_operator.wait_for_job()
        except TimeoutError:
            self.assertRaisesRegex(TimeoutError, f"Job run did not complete in {TEST_TIMEOUT} seconds")
            check_job_mock.assert_called()

    @mock.patch.object(CdeHook, "check_job_run_status", side_effect=["failed", "killed", "unknown"])
    def test_wait_for_job_fails_failed_status(self, check_job_mock, get_connection):
        """Test a failed job run"""
        cde_operator = CdeRunJobOperator(
            task_id="task",
            job_name=TEST_JOB_NAME,
            timeout=TEST_TIMEOUT,
            job_poll_interval=TEST_JOB_POLL_INTERVAL,
        )
        get_connection.assert_called()
        for status in ["failed", "killed", "unknown"]:
            try:
                cde_operator.wait_for_job()
            except AirflowException:
                self.assertRaisesRegex(AirflowException, f"Job run exited with {status} status")
                check_job_mock.assert_called()

    @mock.patch.object(CdeHook, "check_job_run_status", return_value="not_a_status")
    def test_wait_for_job_fails_unexpected_status(self, check_job_mock, get_connection):
        """Test an unusual status from API"""
        cde_operator = CdeRunJobOperator(
            task_id="task",
            job_name=TEST_JOB_NAME,
            timeout=TEST_TIMEOUT,
            job_poll_interval=TEST_JOB_POLL_INTERVAL,
        )
        get_connection.assert_called()
        try:
            cde_operator.wait_for_job()
        except AirflowException:
            self.assertRaisesRegex(
                AirflowException, "Got unexpected status when polling for job: not_a_status"
            )
            check_job_mock.assert_called()

    @mock.patch('sqlalchemy.orm.Query.scalar', return_value=TEST_AIRFLOW_RUN_ID)
    @mock.patch.object(CdeHook, "kill_job_run")
    @mock.patch.object(CdeHook, "submit_job", return_value=TEST_JOB_RUN_ID)
    @mock.patch.object(CdeHook, "check_job_run_status", side_effect=["starting", "running", "bad_status"])
    def test_execute_and_kill_unfinished_run(
        self, check_job_mock, submit_mock, kill_mock, db_mock, get_connection
    ):
        """Test executing a job run return bad_status and check the run is being killed"""
        mock_task_instance_for_context()
        cde_operator = CdeRunJobOperator(
            task_id="task",
            job_name=TEST_JOB_NAME,
            variables=TEST_VARIABLES,
            overrides=TEST_OVERRIDES,
            timeout=TEST_TIMEOUT,
            job_poll_interval=TEST_JOB_POLL_INTERVAL,
        )
        get_connection.assert_called()
        try:
            cde_operator.execute(TEST_CONTEXT)
        except AirflowException:
            self.assertRaisesRegex(
                AirflowException, "Got unexpected status when polling for job: not_a_status"
            )

        check_job_mock.assert_called()
        check_job_mock.assert_has_calls(
            [
                call(TEST_JOB_RUN_ID),
                call(TEST_JOB_RUN_ID),
                call(TEST_JOB_RUN_ID),
            ]
        )
        kill_mock.assert_called()
        check_job_mock.assert_has_calls([call(TEST_JOB_RUN_ID)])
        submit_called_args = submit_mock.call_args.kwargs
        self.validate_request_id(submit_called_args["request_id"])

    def test_templating(self, get_connection):
        """Test templated fields"""
        dag = DAG("dagid", start_date=datetime.now())
        cde_operator = CdeRunJobOperator(
            dag=dag,
            task_id="task",
            job_name=TEST_JOB_NAME,
            variables=TEST_VARIABLES,
            overrides=TEST_OVERRIDES,
        )
        get_connection.assert_called()
        cde_operator.render_template_fields(TEST_CONTEXT)
        self.assertEqual(dict(cde_operator.variables, **{"var1": "someval_20201125"}), cde_operator.variables)
        self.assertDictEqual(cde_operator.overrides, {"spark": {"conf": {"myparam": "val_20201125"}}})

    @mock.patch('sqlalchemy.orm.Query.scalar', return_value=TEST_AIRFLOW_RUN_ID)
    def test_get_request_id(self, db_mock, get_connection):
        """Test get_request_id calculation"""
        cde_operator = CdeRunJobOperator(
            task_id="task",
            job_name=TEST_JOB_NAME,
            timeout=TEST_TIMEOUT,
            job_poll_interval=TEST_JOB_POLL_INTERVAL,
        )
        # should throw exception, because the `task_instance` is not yet populated
        try:
            cde_operator.get_request_id(TEST_CONTEXT)
        except AirflowException:
            self.assertRaisesRegex(AirflowException, "task_instance key is missing from the context:")

        mock_task_instance_for_context()
        self.validate_request_id(cde_operator.get_request_id(TEST_CONTEXT))

    def validate_request_id(self, request_id):
        """Validate the request ID"""
        timestamp = request_id.split("#")[-1]
        for valid_request_id in VALID_REQUEST_IDS:
            if f'{valid_request_id}#{timestamp}' == request_id:
                return
        self.fail(
            f"Request ID '{request_id}' doesn't match valid patterns: {VALID_REQUEST_IDS}"
            f" and parsed timestamp {timestamp}"
        )

    def validate_context_variables(self, variables):
        """Validate the context variables"""
        self.assertEqual(variables["ds"], TEST_CONTEXT["ds"])
        self.assertEqual(variables["ds_nodash"], TEST_CONTEXT["ds_nodash"])
        self.assertEqual(variables["ts"], TEST_CONTEXT["ts"])
        self.assertEqual(variables["ts_nodash"], TEST_CONTEXT["ts_nodash"])
        self.assertEqual(variables["run_id"], TEST_CONTEXT["run_id"])


if __name__ == "__main__":
    unittest.main()
