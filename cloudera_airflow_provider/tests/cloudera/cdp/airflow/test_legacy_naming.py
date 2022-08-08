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

from unittest import mock, TestCase
from unittest.mock import Mock, call

from airflow.hooks.base_hook import BaseHook
from airflow.models.connection import Connection

from cloudera.airflow.providers.hooks import CdpHookException
from cloudera.airflow.providers.hooks.cde_hook import CdeHook, CdeHookException
from cloudera.airflow.providers.hooks.cdw_hook import CdwHook, CdwHiveMetastoreHook
from cloudera.airflow.providers.model.connection import CdeConnection
from cloudera.airflow.providers.operators.cde_operator import CdeRunJobOperator
from cloudera.airflow.providers.operators.cdw_operator import CdwExecuteQueryOperator
from cloudera.airflow.providers.sensors.cdw_sensor import CdwHivePartitionSensor

from tests.utils import _get_call_arguments

TEST_JOB_NAME = 'testjob'
TEST_JOB_RUN_ID = 10
TEST_TIMEOUT = 4
TEST_JOB_POLL_INTERVAL = 1
TEST_API_RETRIES = 3
TEST_API_TIMEOUT = 5
TEST_VARIABLES = {'var1': 'someval_{{ ds_nodash }}'}
TEST_OVERRIDES = {'spark': {'conf': {'myparam': 'val_{{ ds_nodash }}'}}}
TEST_CONTEXT = {
    'ds': '2020-11-25',
    'ds_nodash': '20201125',
    'ts': '2020-11-25T00:00:00+00:00',
    'ts_nodash': '20201125T000000',
    'run_id': 'runid',
}

TEST_HOST = 'vc1.cde-2.cdp-3.cloudera.site'
TEST_SCHEME = 'http'
TEST_PORT = 9090
TEST_AK = "access_key"
TEST_PK = "private_key"
TEST_CUSTOM_CA_CERTIFICATE = "/ca_cert/letsencrypt-stg-root-x1.pem"
TEST_EXTRA = (
    f'{{"access_key": "{TEST_AK}", "private_key": "{TEST_PK}",' f'"ca_cert": "{TEST_CUSTOM_CA_CERTIFICATE}"}}'
)

TEST_DEFAULT_CONNECTION_DICT = {
    'conn_id': CdeHook.DEFAULT_CONN_ID,
    'conn_type': 'http',
    'host': TEST_HOST,
    'port': TEST_PORT,
    'schema': TEST_SCHEME,
    'extra': TEST_EXTRA,
}

TEST_DEFAULT_CONNECTION = Connection(
    conn_id=CdeHook.DEFAULT_CONN_ID,
    conn_type='http',
    host=TEST_HOST,
    port=TEST_PORT,
    schema=TEST_SCHEME,
    extra=TEST_EXTRA,
)

class CdeRunJobOperatorTest(TestCase):

    @mock.patch.object(CdeHook, 'get_connection', return_value=TEST_DEFAULT_CONNECTION)
    @mock.patch.object(CdeHook, 'submit_job', return_value=TEST_JOB_RUN_ID)
    @mock.patch.object(CdeHook, 'check_job_run_status', side_effect=['starting', 'running', 'succeeded'])
    def test_execute_and_wait(self, check_job_mock, submit_mock, get_connection):
        """Sanity check to ensure old operator technically works and can be invoked"""
        from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator
        old_cde_operator = CDEJobRunOperator(
            task_id="task",
            job_name=TEST_JOB_NAME,
            variables=TEST_VARIABLES,
            overrides=TEST_OVERRIDES,
            timeout=TEST_TIMEOUT,
            job_poll_interval=TEST_JOB_POLL_INTERVAL,
        )
        self.assertIsInstance(old_cde_operator, CdeRunJobOperator)
        get_connection.assert_called()
        old_cde_operator.execute(TEST_CONTEXT)
        # Python 3.8 works with called_args = submit_mock.call_args.kwargs,
        # but kwargs method is missing in <=3.7.1
        called_args = _get_call_arguments(submit_mock.call_args)
        self.assertIsInstance(called_args, dict)
        self.assertEqual(dict(called_args['variables'], **TEST_VARIABLES), called_args['variables'])
        self.assertEqual(dict(called_args['variables'], **TEST_CONTEXT), called_args['variables'])
        self.assertDictEqual(TEST_OVERRIDES, called_args['overrides'])
        check_job_mock.assert_has_calls(
            [
                call(TEST_JOB_RUN_ID),
                call(TEST_JOB_RUN_ID),
                call(TEST_JOB_RUN_ID),
            ]
        )

    def test_cdp_legacy_imports(self):
        """Test legacy exception"""
        from cloudera.cdp.airflow.hooks import CDPHookException
        old_cdp_hook_exception = CDPHookException()
        self.assertIsInstance(old_cdp_hook_exception, CdpHookException)


    @mock.patch.object(CdeHook, 'get_connection', return_value=TEST_DEFAULT_CONNECTION)
    def test_cde_legacy_imports(self, get_connection_mock: Mock):
        """Test legacy hook, exception and connection"""
        from cloudera.cdp.airflow.hooks.cde_hook import CDEHook, CDEHookException
        old_cde_hook = CDEHook()
        old_cde_hook_exception = CDEHookException()
        self.assertIsInstance(old_cde_hook, CdeHook)
        self.assertIsInstance(old_cde_hook_exception, CdeHookException)
        get_connection_mock.assert_called()

        from cloudera.cdp.airflow.model.connection import CDEConnection
        old_cde_connection = CDEConnection("","","","","","")
        self.assertIsInstance(old_cde_connection, CdeConnection)


    @mock.patch.object(BaseHook, 'get_connection',
        return_value=Connection(conn_id='fake', conn_type='hive_cli',
                                host='hs2-beeline.host', login='user', password='pass',
                                schema='hello', port=10001, extra=None,
                                uri=None))
    def test_cdw_legacy_imports(self, get_connection_mock: Mock):
        """Checks CDWHook, CDWHiveMetastoreHook and CDWHivePartitionSensor"""
        from cloudera.cdp.airflow.hooks.cdw_hook import CDWHook, CDWHiveMetastoreHook
        old_cdw_hook = CDWHook()
        self.assertIsInstance(old_cdw_hook, CdwHook)
        get_connection_mock.assert_called()

        from cloudera.cdp.airflow.operators.cdw_operator import CDWOperator
        old_cdw_operator = CDWOperator(task_id="task", hql="select 1" )
        self.assertIsInstance(old_cdw_operator, CdwExecuteQueryOperator)

        old_cdw_hms_hook = CDWHiveMetastoreHook()
        self.assertIsInstance(old_cdw_hms_hook, CdwHiveMetastoreHook)

        from cloudera.cdp.airflow.sensors.cdw_sensor import CDWHivePartitionSensor
        old_cdw_hive_partition_sensor= CDWHivePartitionSensor(task_id="task",
            table="table1", partition="partition1")
        self.assertIsInstance(old_cdw_hive_partition_sensor, CdwHivePartitionSensor)
