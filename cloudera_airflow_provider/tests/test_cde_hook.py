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

"""Unit Tests for CDEHook related operations"""

import logging
import unittest
from unittest import mock

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.models import Connection
from airflow.utils.log.logging_mixin import LoggingMixin
from requests import Session
from tests.utils import _get_call_arguments, _make_response
from cloudera.cdp.airflow.hooks.cde_hook import CDEHook, CDEHookException
from cloudera.cdp.security.cde_security import (CDEAPITokenAuth,
                                                CDETokenAuthResponse)


LOG = LoggingMixin().log
LOG.setLevel(logging.DEBUG)

TEST_HOST = 'https://vc1.cde-2.cdp-3.cloudera.site'
TEST_SCHEME = 'http'
TEST_PORT = 9090
TEST_JOB_NAME = 'testjob'
TEST_VARIABLES = {
    'var1': 'someval_{{ ds_nodash }}',
    'ds': '2020-11-25',
    'ds_nodash': '20201125',
    'ts': '2020-11-25T00:00:00+00:00',
    'ts_nodash': '20201125T000000',
    'run_id': 'runid'
}
TEST_OVERRIDES = {
    'spark': {
        'conf': {
            'myparam': 'val_{{ ds_nodash }}'
        }
    }
}
TEST_AK="access_key"
TEST_PK="private_key_xxxxx_xxxxx_xxxxx_xxxxx"
TEST_CUSTOM_CA_CERTIFICATE="/ca_cert/letsencrypt-stg-root-x1.pem"
TEST_EXTRA=f'{{"ca_cert_path": "{TEST_CUSTOM_CA_CERTIFICATE}"}}'

def _get_test_connection(**kwargs):
    kwargs = {**TEST_DEFAULT_CONNECTION_DICT, **kwargs}
    return Connection(**kwargs)

TEST_DEFAULT_CONNECTION_DICT = {
    'conn_id' : CDEHook.DEFAULT_CONN_ID,
    'conn_type' : 'http',
    'host' : TEST_HOST,
    'login' : TEST_AK,
    'password' : TEST_PK,
    'port' : TEST_PORT,
    'schema' : TEST_SCHEME,
    'extra' : TEST_EXTRA}

TEST_DEFAULT_CONNECTION = _get_test_connection()

VALID_CDE_TOKEN = "my_cde_token"
VALID_CDE_TOKEN_AUTH_REQUEST_RESPONSE = _make_response(200,
    {"access_token": VALID_CDE_TOKEN, "expires_in": 123 }, "")
VALID_CDE_TOKEN_AUTH_RESPONSE = CDETokenAuthResponse.from_response(
    VALID_CDE_TOKEN_AUTH_REQUEST_RESPONSE)



class CDEHookTest(unittest.TestCase):
    """Unit tests for CDEHook"""

    @mock.patch.object(BaseHook, 'get_connection',
        return_value=_get_test_connection(extra='{"insecure": False}'))
    def test_wrong_extra_in_connection(self, connection_mock):
        """Test when wrong input is provided in the extra field of the connection"""
        with self.assertRaises(ValueError):
            CDEHook()
        connection_mock.assert_called()

    @mock.patch('cloudera.cdp.security.cde_security.CDEAPITokenAuth.get_cde_authentication_token',
        return_value=VALID_CDE_TOKEN_AUTH_RESPONSE)
    @mock.patch.object(Session, 'send', return_value=_make_response(201, {'id': 10}, ""))
    @mock.patch.object(BaseHook, 'get_connection', return_value=TEST_DEFAULT_CONNECTION)
    def test_submit_job_ok(self, connection_mock, session_send_mock, cde_mock):
        """Test a successful submission to the API"""
        cde_hook = CDEHook()
        run_id = cde_hook.submit_job(TEST_JOB_NAME)
        self.assertEqual(run_id, 10)
        cde_mock.assert_called()
        connection_mock.assert_called()
        session_send_mock.assert_called()


    @mock.patch('cloudera.cdp.security.cde_security.CDEAPITokenAuth.get_cde_authentication_token',
        return_value=VALID_CDE_TOKEN_AUTH_RESPONSE)
    @mock.patch.object(Session, 'send', return_value=_make_response(201, {'id': 10}, ""))
    @mock.patch.object(BaseHook, 'get_connection',
        return_value=_get_test_connection(host='abc.svc'))
    def test_submit_job_ok_internal_connection(self,
                    connection_mock, session_send_mock, cde_mock: mock.Mock):
        """Test a successful submission to the API"""
        cde_hook = CDEHook()
        run_id = cde_hook.submit_job(TEST_JOB_NAME)
        self.assertEqual(run_id, 10)
        cde_mock.assert_not_called()
        connection_mock.assert_called()
        session_send_mock.assert_called()

    @mock.patch.object(CDEAPITokenAuth, 'get_cde_authentication_token',
        return_value=VALID_CDE_TOKEN_AUTH_RESPONSE)
    @mock.patch.object(BaseHook, 'get_connection', return_value=TEST_DEFAULT_CONNECTION)
    @mock.patch.object(Session, 'send', side_effect=[
        _make_response(503, None, "Internal Server Error"),
        _make_response(500, None, "Internal Server Error"),
        _make_response(201, {'id': 10}, "")
    ])
    def test_submit_job_retry_after_5xx_works(self, send_mock, connection_mock, cde_mock):
        """Ensure that 5xx errors are retried"""
        cde_hook = CDEHook()
        run_id = cde_hook.submit_job(TEST_JOB_NAME)
        self.assertEqual(run_id, 10)
        self.assertEqual(cde_mock.call_count, 1)
        self.assertEqual(send_mock.call_count, 3)
        connection_mock.assert_called()

    @mock.patch('cloudera.cdp.security.cde_security.CDEAPITokenAuth.get_cde_authentication_token',
        return_value=VALID_CDE_TOKEN_AUTH_RESPONSE)
    @mock.patch.object(BaseHook, 'get_connection', return_value=TEST_DEFAULT_CONNECTION)
    @mock.patch.object(Session, 'send', return_value=_make_response(404, None, "Not Found"))
    def test_submit_job_fails_immediately_for_4xx(self, send_mock, connection_mock, cde_mock):
        """Ensure that 4xx errors are _not_ retried"""
        cde_hook = CDEHook()
        with self.assertRaises(CDEHookException) as err:
            cde_hook.submit_job(TEST_JOB_NAME)
        self.assertEqual(send_mock.call_count, 1)
        self.assertIsInstance(err.exception.raised_from, AirflowException)
        cde_mock.assert_called()
        connection_mock.assert_called()

    @mock.patch('cloudera.cdp.security.cde_security.CDEAPITokenAuth.get_cde_authentication_token',
        return_value=VALID_CDE_TOKEN_AUTH_RESPONSE)
    @mock.patch.object(Session, 'send', return_value=_make_response(201, {'id': 10}, ""))
    @mock.patch.object(BaseHook, 'get_connection',
        return_value=_get_test_connection(extra='{"insecure": true}'))
    def test_submit_job_insecure(self, connection_mock, session_send_mock, cde_mock):
        """Ensure insecure mode is taken into account"""
        cde_hook = CDEHook()
        run_id = cde_hook.submit_job(TEST_JOB_NAME)
        self.assertEqual(run_id, 10)
        cde_mock.assert_called()
        connection_mock.assert_called()
        session_send_mock.assert_called()
        called_args = _get_call_arguments(session_send_mock.call_args)
        self.assertEqual(called_args['verify'], False)

    @mock.patch('cloudera.cdp.security.cde_security.CDEAPITokenAuth.get_cde_authentication_token',
        return_value=VALID_CDE_TOKEN_AUTH_RESPONSE)
    @mock.patch.object(Session, 'send', return_value=_make_response(201, {'id': 10}, ""))
    @mock.patch.object(BaseHook, 'get_connection', return_value=_get_test_connection(extra='{}'))
    def test_submit_job_no_custom_ca_certificate(self, connection_mock, session_send_mock, cde_mock):
        """Ensure that default TLS security configuration runs fine"""
        cde_hook = CDEHook()
        run_id = cde_hook.submit_job(TEST_JOB_NAME)
        self.assertEqual(run_id, 10)
        cde_mock.assert_called()
        connection_mock.assert_called()
        session_send_mock.assert_called()
        called_args = _get_call_arguments(session_send_mock.call_args)
        self.assertEqual(called_args['verify'], True)

    @mock.patch('cloudera.cdp.security.cde_security.CDEAPITokenAuth.get_cde_authentication_token',
        return_value=VALID_CDE_TOKEN_AUTH_RESPONSE)
    @mock.patch.object(Session, 'send', return_value=_make_response(201, {'id': 10}, ""))
    @mock.patch.object(BaseHook, 'get_connection', return_value=TEST_DEFAULT_CONNECTION)
    def test_submit_job_custom_ca_certificate(self, connection_mock, session_send_mock, cde_mock):
        """Ensure custom is taken into account"""
        cde_hook = CDEHook()
        run_id = cde_hook.submit_job(TEST_JOB_NAME)
        self.assertEqual(run_id, 10)
        cde_mock.assert_called()
        connection_mock.assert_called()
        session_send_mock.assert_called()
        called_args = _get_call_arguments(session_send_mock.call_args)
        self.assertEqual(called_args['verify'], TEST_CUSTOM_CA_CERTIFICATE)

    @mock.patch.object(BaseHook, 'get_connection',
        return_value=_get_test_connection(extra='{"cache_dir": " "}'))
    def test_wrong_cache_dir(self, connection_mock):
        """Ensure that CDEHook object creation fails if cache dir value is wrong"""
        cde_hook = CDEHook()
        with self.assertRaises(CDEHookException):
            cde_hook.submit_job(TEST_JOB_NAME)
        connection_mock.assert_called()

if __name__ == "__main__":
    unittest.main()
