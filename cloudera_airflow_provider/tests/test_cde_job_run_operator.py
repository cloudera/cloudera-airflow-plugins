#  Cloudera Airflow Provider
#  (C) Cloudera, Inc. 2021-2021
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

import unittest
from datetime import datetime
from unittest import mock
from unittest.mock import Mock, call

from airflow.exceptions import AirflowException
from airflow.models.connection import Connection
from airflow.models.dag import DAG
from tests.utils import _get_call_arguments
from cloudera.cdp.airflow.hooks.cde_hook import CDEHook
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator


TEST_JOB_NAME = 'testjob'
TEST_JOB_RUN_ID = 10
TEST_TIMEOUT = 4
TEST_JOB_POLL_INTERVAL = 1
TEST_VARIABLES = {
    'var1': 'someval_{{ ds_nodash }}'
}
TEST_OVERRIDES = {
    'spark': {
        'conf': {
            'myparam': 'val_{{ ds_nodash }}'
        }
    }
}
TEST_CONTEXT = {
    'ds': '2020-11-25',
    'ds_nodash': '20201125',
    'ts': '2020-11-25T00:00:00+00:00',
    'ts_nodash': '20201125T000000',
    'run_id': 'runid'
}

TEST_HOST = 'vc1.cde-2.cdp-3.cloudera.site'
TEST_SCHEME = 'http'
TEST_PORT = 9090
TEST_AK="access_key"
TEST_PK="private_key"
TEST_CUSTOM_CA_CERTIFICATE="/ca_cert/letsencrypt-stg-root-x1.pem"
TEST_EXTRA=(f'{{"access_key": "{TEST_AK}", "private_key": "{TEST_PK}",'
            f'"ca_cert": "{TEST_CUSTOM_CA_CERTIFICATE}"}}'
        )

TEST_DEFAULT_CONNECTION_DICT = {
    'conn_id' : CDEHook.DEFAULT_CONN_ID,
    'conn_type' : 'http',
    'host' : TEST_HOST,
    'port' : TEST_PORT,
    'schema' : TEST_SCHEME,
    'extra' : TEST_EXTRA}

TEST_DEFAULT_CONNECTION = Connection(
    conn_id=CDEHook.DEFAULT_CONN_ID,
    conn_type='http',
    host=TEST_HOST,
    port=TEST_PORT,
    schema=TEST_SCHEME,
    extra=TEST_EXTRA)




@mock.patch.object(CDEHook, 'get_connection', return_value=TEST_DEFAULT_CONNECTION)
class CDEJobRunOperatorTest(unittest.TestCase):

    """Test cases for CDE operator"""

    def test_init(self, get_connection: Mock):
        """Test constructor"""
        cde_operator = CDEJobRunOperator(
            task_id="task",
            job_name=TEST_JOB_NAME,
            variables=TEST_VARIABLES,
            overrides=TEST_OVERRIDES,
        )
        get_connection.assert_called()
        self.assertEqual(cde_operator.job_name, TEST_JOB_NAME)
        self.assertDictEqual(cde_operator.variables, TEST_VARIABLES)
        self.assertDictEqual(cde_operator.overrides, TEST_OVERRIDES)
        self.assertEqual(cde_operator.connection_id, CDEJobRunOperator.DEFAULT_CONNECTION_ID)
        self.assertEqual(cde_operator.wait, CDEJobRunOperator.DEFAULT_WAIT)
        self.assertEqual(cde_operator.timeout, CDEJobRunOperator.DEFAULT_TIMEOUT)
        self.assertEqual(cde_operator.job_poll_interval, CDEJobRunOperator.DEFAULT_POLL_INTERVAL)
        self.assertEqual(cde_operator.api_retries, CDEJobRunOperator.DEFAULT_RETRIES)

    @mock.patch.object(CDEHook, 'submit_job', return_value=TEST_JOB_RUN_ID)
    @mock.patch.object(CDEHook, 'check_job_run_status',
        side_effect=['starting','running','succeeded'])
    def test_execute_and_wait(self, check_job_mock, submit_mock, get_connection):
        """Test executing a job run and waiting for success"""
        cde_operator = CDEJobRunOperator(
            task_id="task",
            job_name=TEST_JOB_NAME,
            variables=TEST_VARIABLES,
            overrides=TEST_OVERRIDES,
            timeout=TEST_TIMEOUT,
            job_poll_interval=TEST_JOB_POLL_INTERVAL
        )
        get_connection.assert_called()
        cde_operator.execute(TEST_CONTEXT)
        # Python 3.8 works with called_args = submit_mock.call_args.kwargs,
        # but kwargs method is missing in <=3.7.1
        called_args = _get_call_arguments(submit_mock.call_args)
        self.assertIsInstance(called_args,dict)
        self.assertEqual(dict(called_args['variables'], **TEST_VARIABLES), called_args['variables'])
        self.assertEqual(dict(called_args['variables'], **TEST_CONTEXT), called_args['variables'])
        self.assertDictEqual(TEST_OVERRIDES, called_args['overrides'])
        check_job_mock.assert_has_calls([
            call(TEST_JOB_RUN_ID),
            call(TEST_JOB_RUN_ID),
            call(TEST_JOB_RUN_ID),
        ])

    @mock.patch.object(CDEHook, 'submit_job', return_value=TEST_JOB_RUN_ID)
    @mock.patch.object(CDEHook, 'check_job_run_status')
    def test_execute_and_do_not_wait(self, check_job_mock, submit_mock, get_connection):
        """Test executing a job and not waiting"""
        cde_operator = CDEJobRunOperator(
            task_id="task",
            job_name=TEST_JOB_NAME,
            variables=TEST_VARIABLES,
            overrides=TEST_OVERRIDES,
            timeout=TEST_TIMEOUT,
            job_poll_interval=TEST_JOB_POLL_INTERVAL,
            wait=False
        )
        get_connection.assert_called()
        cde_operator.execute(TEST_CONTEXT)
        # Python 3.8 works with called_args = submit_mock.call_args.kwargs,
        # but kwargs method is missing in <=3.7.1
        called_args = _get_call_arguments(submit_mock.call_args)
        self.assertEqual(dict(called_args['variables'], **TEST_VARIABLES), called_args['variables'])
        self.assertEqual(dict(called_args['variables'], **TEST_CONTEXT), called_args['variables'])
        self.assertDictEqual(TEST_OVERRIDES, called_args['overrides'])
        check_job_mock.assert_not_called()

    @mock.patch.object(CDEHook, 'kill_job_run')
    def test_on_kill(self, kill_job_mock, get_connection):
        """Test killing a running job"""
        cde_operator = CDEJobRunOperator(
            task_id="task",
            job_name=TEST_JOB_NAME,
            variables=TEST_VARIABLES,
            overrides=TEST_OVERRIDES,
            timeout=TEST_TIMEOUT,
            job_poll_interval=TEST_JOB_POLL_INTERVAL,
        )
        get_connection.assert_called()
        cde_operator._job_run_id = 1 # pylint: disable=W0212
        cde_operator.on_kill()
        kill_job_mock.assert_called()

    @mock.patch.object(CDEHook, 'check_job_run_status', return_value='starting')
    def test_wait_for_job_times_out(self, check_job_mock, get_connection):
        """Test a job run timeout"""
        cde_operator = CDEJobRunOperator(
            task_id="task",
            job_name=TEST_JOB_NAME,
            timeout=TEST_TIMEOUT,
            job_poll_interval=TEST_JOB_POLL_INTERVAL
        )
        get_connection.assert_called()
        try:
            cde_operator.wait_for_job()
        except TimeoutError:
            self.assertRaisesRegex(TimeoutError,
                f'Job run did not complete in {TEST_TIMEOUT} seconds')
            check_job_mock.assert_called()

    @mock.patch.object(CDEHook, 'check_job_run_status', side_effect=['failed','killed','unknown'])
    def test_wait_for_job_fails_failed_status(self, check_job_mock, get_connection):
        """Test a failed job run"""
        cde_operator = CDEJobRunOperator(
            task_id="task",
            job_name=TEST_JOB_NAME,
            timeout=TEST_TIMEOUT,
            job_poll_interval=TEST_JOB_POLL_INTERVAL
        )
        get_connection.assert_called()
        for status in ['failed','killed','unknown']:
            try:
                cde_operator.wait_for_job()
            except AirflowException:
                self.assertRaisesRegex(AirflowException, f'Job run exited with {status} status')
                check_job_mock.assert_called()

    @mock.patch.object(CDEHook, 'check_job_run_status', return_value='not_a_status')
    def test_wait_for_job_fails_unexpected_status(self, check_job_mock, get_connection):
        """Test an unusual status from API"""
        cde_operator = CDEJobRunOperator(
            task_id="task",
            job_name=TEST_JOB_NAME,
            timeout=TEST_TIMEOUT,
            job_poll_interval=TEST_JOB_POLL_INTERVAL
        )
        get_connection.assert_called()
        try:
            cde_operator.wait_for_job()
        except AirflowException:
            self.assertRaisesRegex(AirflowException,
                                   'Got unexpected status whe polling for job: not_a_status')
            check_job_mock.assert_called()

    def test_templating(self, get_connection):
        """Test templated fields"""
        dag = DAG("dagid", start_date=datetime.now())
        cde_operator = CDEJobRunOperator(
            dag=dag,
            task_id="task",
            job_name=TEST_JOB_NAME,
            variables=TEST_VARIABLES,
            overrides=TEST_OVERRIDES
        )
        get_connection.assert_called()
        cde_operator.render_template_fields(TEST_CONTEXT)
        self.assertEqual(dict(cde_operator.variables, **{'var1':'someval_20201125'}),
            cde_operator.variables)
        self.assertDictEqual(cde_operator.overrides, {'spark':{'conf':{'myparam':'val_20201125'}}})


if __name__ == "__main__":
    unittest.main()
