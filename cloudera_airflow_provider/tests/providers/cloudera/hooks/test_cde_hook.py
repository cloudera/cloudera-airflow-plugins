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

"""Unit Tests for CdeHook related operations"""

from __future__ import annotations

import contextlib
import logging
import os
import unittest
from concurrent.futures import Future
from json import JSONDecodeError
from typing import Callable
from unittest import mock
from unittest.mock import call

from requests import Session
from requests.exceptions import ConnectionError, HTTPError, Timeout  # pylint: disable=redefined-builtin
from retrying import RetryError  # type: ignore

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.models import Connection
from cloudera.airflow.providers.hooks.cde import (
    DEFAULT_RETRY_INTERVAL,
    RETRY_AFTER_HEADER,
    CdeHook,
    CdeHookException,
)
from cloudera.cdp.security.cde_security import CdeApiTokenAuth, CdeTokenAuthResponse
from tests.providers.cloudera.utils import _get_call_arguments, _make_response

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)

TEST_HOST = "https://vc1.cde-2.cdp-3.cloudera.site"
TEST_SCHEME = "http"
TEST_PORT = 9090
TEST_CLUSTER_NAME = "mycluster"
TEST_JOB_NAME = "testjob"
TEST_JOB_RUN_ID = 10
TEST_JOB_RUN_STATUS = "active"
TEST_VARIABLES = {
    "var1": "someval_{{ ds_nodash }}",
    "ds": "2020-11-25",
    "ds_nodash": "20201125",
    "ts": "2020-11-25T00:00:00+00:00",
    "ts_nodash": "20201125T000000",
    "run_id": "runid",
}
TEST_OVERRIDES = {"spark": {"conf": {"myparam": "val_{{ ds_nodash }}"}}}
TEST_AK = "access_key"
TEST_PK = "private_key_xxxxx_xxxxx_xxxxx_xxxxx"
TEST_CUSTOM_CA_CERTIFICATE = "/ca_cert/letsencrypt-stg-root-x1.pem"
TEST_EXTRA = f'{{"ca_cert_path": "{TEST_CUSTOM_CA_CERTIFICATE}", "region": "us-west-1"}}'
GET_CDE_AUTH_TOKEN_METHOD = "cloudera.cdp.security.cde_security.CdeApiTokenAuth.get_cde_authentication_token"
GET_AIRFLOW_CONFIG = "airflow.providers.cloudera.hooks.cde_hook.conf"
INVALID_JSON_STRING = "{'invalid_json"


def _get_test_connection(**kwargs):
    kwargs = {**TEST_DEFAULT_CONNECTION_DICT, **kwargs}
    return Connection(**kwargs)


TEST_DEFAULT_CONNECTION_DICT = {
    "conn_id": CdeHook.DEFAULT_CONN_ID,
    "conn_type": "http",
    "host": TEST_HOST,
    "login": TEST_AK,
    "password": TEST_PK,
    "port": TEST_PORT,
    "schema": TEST_SCHEME,
    "extra": TEST_EXTRA,
}

TEST_DEFAULT_CONNECTION = _get_test_connection()

VALID_CDE_TOKEN = "my_cde_token"
VALID_CDE_TOKEN_AUTH_REQUEST_RESPONSE = _make_response(
    200, {"access_token": VALID_CDE_TOKEN, "expires_in": 123}, ""
)
VALID_CDE_TOKEN_AUTH_RESPONSE = CdeTokenAuthResponse.from_response(VALID_CDE_TOKEN_AUTH_REQUEST_RESPONSE)


# pylint: disable=too-many-public-methods
class CdeHookTest(unittest.TestCase):
    """Unit tests for CdeHook"""

    @contextlib.contextmanager
    def tenacity_wait_mocks(self, wait_impl: Callable = None):
        """
        Creates mocks for tenacity.wait_fixed and tenacity.wait_exponential.
        The tests require to verify calls of the constructor of wait_fixed
        to check if the parameter was correct. Also, test can be interested in
        the call count of the wait instance for exponential waits, therefore we
        yield these two.
        The default implementation for both wait methods is the same:
        to wait 0.1 seconds.
        This can be overriden with the method arguments.
        We call both classes with an arbitrary constructor param (-1111) in order
        to have access to the stop instance object.
        """

        # This is the actual wait time between requests
        actual_wait_seconds = 0.1

        def wait_very_short(retry_state):
            LOG.debug("Called wait_fixed or wait_exponential mock. Arg: %s", retry_state)
            return actual_wait_seconds

        with mock.patch('tenacity.wait_fixed') as wait_fixed_cls_mock, mock.patch(
            'tenacity.wait_exponential'
        ) as wait_exp_cls_mock:
            arbitrary_ctor_param = -1111
            wait_fixed_obj = wait_fixed_cls_mock(arbitrary_ctor_param)
            wait_exp_obj = wait_exp_cls_mock(arbitrary_ctor_param)

            wait_fixed_obj.side_effect = wait_very_short
            wait_exp_obj.side_effect = wait_very_short

            if wait_impl:
                wait_exp_obj.side_effect = wait_impl
                wait_fixed_obj.side_effect = wait_impl

            yield wait_fixed_cls_mock, wait_exp_obj

    @contextlib.contextmanager
    def tenacity_stop_mocks(
        self, stop_after_attempt_impl: Callable = None, stop_after_delay_impl: Callable = None
    ):
        """
        Creates mocks for tenacity.stop_after_attempt and tenacity.stop_after_delay.
        Some tests may require to verify calls of the constructor of the wait class
        or calls to the instance of the wait class, therefore we yield all of these.
        The default implementation for the attempt-based and the delay-based stops
        is the same: to never stop.
        This can be overriden with the method arguments.
        We call both classes with an arbitrary constructor param (-1111) in order
        to have access to the stop instance object.
        """

        def never_stop_impl(retry_state):
            LOG.debug("Called never_stop_impl on mock. Arg: %s", retry_state)
            return False

        with mock.patch('tenacity.stop_after_attempt') as stop_after_attempt_cls_mock, mock.patch(
            'tenacity.stop_after_delay'
        ) as stop_after_delay_cls_mock:
            arbitrary_ctor_param = -1111
            stop_after_attempt_obj = stop_after_attempt_cls_mock(arbitrary_ctor_param)
            stop_after_attempt_obj.side_effect = never_stop_impl
            if stop_after_attempt_impl:
                stop_after_attempt_obj.side_effect = stop_after_attempt_impl

            stop_after_delay_obj = stop_after_delay_cls_mock(arbitrary_ctor_param)
            stop_after_delay_obj.side_effect = never_stop_impl
            if stop_after_delay_impl:
                stop_after_delay_obj.side_effect = stop_after_delay_impl

            yield (
                stop_after_attempt_cls_mock,
                stop_after_attempt_obj,
                stop_after_delay_cls_mock,
                stop_after_delay_obj,
            )

    @mock.patch.object(
        BaseHook,
        "get_connection",
        return_value=_get_test_connection(extra='{"insecure": False, "region": "us-west-1"}'),
    )
    def test_wrong_extra_in_connection(self, connection_mock):
        """Test when wrong input is provided in the extra field of the connection"""
        with self.assertRaises(ValueError):
            CdeHook()
        connection_mock.assert_called()

    @mock.patch(GET_CDE_AUTH_TOKEN_METHOD, return_value=VALID_CDE_TOKEN_AUTH_RESPONSE)
    @mock.patch.object(Session, "send", return_value=_make_response(200, {"appName": TEST_CLUSTER_NAME}, ""))
    @mock.patch.object(BaseHook, "get_connection", return_value=TEST_DEFAULT_CONNECTION)
    def test_test_connection_ok(self, connection_mock, session_send_mock, cde_mock):
        """Test a successful connection to the API"""
        cde_hook = CdeHook()
        test_result = cde_hook.test_connection()
        self.assertTrue(test_result[0])
        cde_mock.assert_called()
        connection_mock.assert_called()
        session_send_mock.assert_called()

    @mock.patch(GET_CDE_AUTH_TOKEN_METHOD, return_value=VALID_CDE_TOKEN_AUTH_RESPONSE)
    @mock.patch.object(Session, "send", return_value=_make_response(200, None, ""))
    @mock.patch.object(BaseHook, "get_connection", return_value=TEST_DEFAULT_CONNECTION)
    def test_test_connection_response_none_fail(self, connection_mock, session_send_mock, cde_mock):
        """Test a failing connection to the API, with a None response"""
        cde_hook = CdeHook()
        test_result = cde_hook.test_connection()
        self.assertFalse(test_result[0])
        cde_mock.assert_called()
        connection_mock.assert_called()
        session_send_mock.assert_called()

    @mock.patch(GET_CDE_AUTH_TOKEN_METHOD, return_value=VALID_CDE_TOKEN_AUTH_RESPONSE)
    @mock.patch.object(Session, "send", return_value=_make_response(404, None, ""))
    @mock.patch.object(BaseHook, "get_connection", return_value=TEST_DEFAULT_CONNECTION)
    def test_test_connection_not_found_fail(self, connection_mock, session_send_mock, cde_mock):
        """Test a failing connection to the API, with a 404 response"""
        cde_hook = CdeHook()
        test_result = cde_hook.test_connection()
        self.assertFalse(test_result[0])
        cde_mock.assert_called()
        connection_mock.assert_called()
        session_send_mock.assert_called()

    @mock.patch(GET_CDE_AUTH_TOKEN_METHOD, return_value=VALID_CDE_TOKEN_AUTH_RESPONSE)
    @mock.patch.object(
        Session, "send", return_value=_make_response(200, {"notAppName": TEST_CLUSTER_NAME}, "")
    )
    @mock.patch.object(BaseHook, "get_connection", return_value=TEST_DEFAULT_CONNECTION)
    def test_test_connection_app_name_missing_fail(self, connection_mock, session_send_mock, cde_mock):
        """Test a failing connection to the API with a missing app name"""
        cde_hook = CdeHook()
        test_result = cde_hook.test_connection()
        self.assertFalse(test_result[0])
        cde_mock.assert_called()
        connection_mock.assert_called()
        session_send_mock.assert_called()

    @mock.patch(GET_CDE_AUTH_TOKEN_METHOD, return_value=VALID_CDE_TOKEN_AUTH_RESPONSE)
    @mock.patch.object(Session, "send", return_value=_make_response(201, {"id": TEST_JOB_RUN_ID}, ""))
    @mock.patch.object(BaseHook, "get_connection", return_value=TEST_DEFAULT_CONNECTION)
    def test_submit_job_ok(self, connection_mock, session_send_mock, cde_mock):
        """Test a successful submission to the API"""
        cde_hook = CdeHook()
        run_id = cde_hook.submit_job(TEST_JOB_NAME)
        self.assertEqual(run_id, TEST_JOB_RUN_ID)
        cde_mock.assert_called()
        connection_mock.assert_called()
        session_send_mock.assert_called()

    @mock.patch(GET_CDE_AUTH_TOKEN_METHOD, return_value=VALID_CDE_TOKEN_AUTH_RESPONSE)
    @mock.patch.object(Session, "send", return_value=_make_response(201, {"id": TEST_JOB_RUN_ID}, ""))
    @mock.patch.object(BaseHook, "get_connection", return_value=_get_test_connection(host="abc.svc"))
    def test_submit_job_ok_internal_connection(self, connection_mock, session_send_mock, cde_mock: mock.Mock):
        """Test a successful submission to the API"""
        cde_hook = CdeHook()
        run_id = cde_hook.submit_job(TEST_JOB_NAME)
        self.assertEqual(run_id, TEST_JOB_RUN_ID)
        cde_mock.assert_not_called()
        connection_mock.assert_called()
        session_send_mock.assert_called()

    @mock.patch.dict(os.environ, {"AIRFLOW__CDE__DEFAULT_API_TIMEOUT": "400"})
    @mock.patch(GET_CDE_AUTH_TOKEN_METHOD, return_value=VALID_CDE_TOKEN_AUTH_RESPONSE)
    @mock.patch.object(Session, "send", return_value=_make_response(201, {"id": TEST_JOB_RUN_ID}, ""))
    @mock.patch.object(BaseHook, "get_connection", return_value=_get_test_connection(host="abc.svc"))
    def test_submit_job_ok_internal_connection_set_timeout_by_env(
        self, connection_mock, session_send_mock, cde_mock: mock.Mock
    ):
        """Test a successful submission to the API"""
        cde_hook = CdeHook()
        run_id = cde_hook.submit_job(TEST_JOB_NAME)
        self.assertEqual(run_id, TEST_JOB_RUN_ID)
        cde_mock.assert_not_called()
        connection_mock.assert_called()
        session_send_mock.assert_called_with(
            mock.ANY,
            allow_redirects=mock.ANY,
            cert=mock.ANY,
            proxies=mock.ANY,
            stream=mock.ANY,
            timeout=400,
            verify=mock.ANY,
        )

    @mock.patch.dict(os.environ, {"AIRFLOW__CDE__DEFAULT_API_TIMEOUT": "400asdf"})
    @mock.patch(GET_CDE_AUTH_TOKEN_METHOD, return_value=VALID_CDE_TOKEN_AUTH_RESPONSE)
    @mock.patch.object(Session, "send", return_value=_make_response(201, {"id": TEST_JOB_RUN_ID}, ""))
    @mock.patch.object(BaseHook, "get_connection", return_value=_get_test_connection(host="abc.svc"))
    def test_submit_job_failed_internal_connection_set_timeout_by_env(
        self, connection_mock, session_send_mock, cde_mock: mock.Mock
    ):
        """Test a wrong api timeout value via the AIRFLOW__CDE__DEFAULT_API_TIMEOUT
        environment variable, the default value should be used in this case."""
        cde_hook = CdeHook()
        run_id = cde_hook.submit_job(TEST_JOB_NAME)
        self.assertEqual(run_id, TEST_JOB_RUN_ID)
        cde_mock.assert_not_called()
        connection_mock.assert_called()
        session_send_mock.assert_called_with(
            mock.ANY,
            allow_redirects=mock.ANY,
            cert=mock.ANY,
            proxies=mock.ANY,
            stream=mock.ANY,
            timeout=CdeHook.DEFAULT_API_TIMEOUT,
            verify=mock.ANY,
        )

    @mock.patch(GET_CDE_AUTH_TOKEN_METHOD, return_value=VALID_CDE_TOKEN_AUTH_RESPONSE)
    @mock.patch.object(Session, "send", return_value=_make_response(201, None, ""))
    @mock.patch.object(BaseHook, "get_connection", return_value=TEST_DEFAULT_CONNECTION)
    def test_submit_job_empty_response_none_fail(
        self, connection_mock, session_send_mock, cde_mock: mock.Mock
    ):
        """Test a fail on empty response from CDE"""
        cde_hook = CdeHook()
        with self.assertRaises(CdeHookException) as err:
            cde_hook.submit_job(TEST_JOB_NAME)
        # Ensure that there is no previous exceptions in Exception stack
        self.assertFalse(hasattr(err.exception.raised_from, "raised_from"))
        cde_mock.assert_called()
        connection_mock.assert_called()
        session_send_mock.assert_called()

    @mock.patch(GET_CDE_AUTH_TOKEN_METHOD, return_value=VALID_CDE_TOKEN_AUTH_RESPONSE)
    @mock.patch.object(Session, "send", return_value=_make_response(201, b"", ""))
    @mock.patch.object(BaseHook, "get_connection", return_value=TEST_DEFAULT_CONNECTION)
    def test_submit_job_empty_response_bytes_fail(
        self, connection_mock, session_send_mock, cde_mock: mock.Mock
    ):
        """Test a fail on empty response from CDE"""
        cde_hook = CdeHook()
        with self.assertRaises(CdeHookException) as err:
            cde_hook.submit_job(TEST_JOB_NAME)
        # Ensure that there is no previous exceptions in Exception stack
        self.assertFalse(hasattr(err.exception.raised_from, "raised_from"))
        cde_mock.assert_called()
        connection_mock.assert_called()
        session_send_mock.assert_called()

    @mock.patch(GET_CDE_AUTH_TOKEN_METHOD, return_value=VALID_CDE_TOKEN_AUTH_RESPONSE)
    @mock.patch.object(Session, "send", return_value=_make_response(201, {"wrong": "wrong"}, ""))
    @mock.patch.object(BaseHook, "get_connection", return_value=TEST_DEFAULT_CONNECTION)
    def test_submit_job_invalid_key_fail(self, connection_mock, session_send_mock, cde_mock: mock.Mock):
        """Test a fail on incorrect response from CDE"""
        cde_hook = CdeHook()
        with self.assertRaises(CdeHookException) as err:
            cde_hook.submit_job(TEST_JOB_NAME)
        self.assertIsInstance(err.exception.raised_from, KeyError)
        cde_mock.assert_called()
        connection_mock.assert_called()
        session_send_mock.assert_called()

    @mock.patch(GET_CDE_AUTH_TOKEN_METHOD, return_value=VALID_CDE_TOKEN_AUTH_RESPONSE)
    @mock.patch.object(Session, "send", return_value=_make_response(201, INVALID_JSON_STRING, ""))
    @mock.patch.object(BaseHook, "get_connection", return_value=TEST_DEFAULT_CONNECTION)
    def test_submit_job_invalid_json_fail(self, connection_mock, session_send_mock, cde_mock: mock.Mock):
        """Test a fail on invalid JSON response from CDE"""
        cde_hook = CdeHook()
        with self.assertRaises(CdeHookException) as err:
            cde_hook.submit_job(TEST_JOB_NAME)
        self.assertIsInstance(err.exception.raised_from, JSONDecodeError)
        cde_mock.assert_called()
        connection_mock.assert_called()
        session_send_mock.assert_called()

    @mock.patch.object(
        CdeApiTokenAuth, "get_cde_authentication_token", return_value=VALID_CDE_TOKEN_AUTH_RESPONSE
    )
    @mock.patch.object(BaseHook, "get_connection", return_value=TEST_DEFAULT_CONNECTION)
    @mock.patch.object(
        Session,
        "send",
        side_effect=[
            _make_response(429, None, "Too Many Requests"),
            _make_response(429, None, "Too Many Requests"),
            _make_response(201, {"id": TEST_JOB_RUN_ID}, ""),
        ],
    )
    def test_submit_job_retry_after_429_works(self, send_mock, connection_mock, cde_mock):
        """Ensure that 429 errors are handled"""

        cde_hook = CdeHook()
        run_id = cde_hook.submit_job(TEST_JOB_NAME)
        self.assertEqual(run_id, TEST_JOB_RUN_ID)
        self.assertEqual(cde_mock.call_count, 1)
        self.assertEqual(send_mock.call_count, 3)
        connection_mock.assert_called()

    @mock.patch.object(
        CdeApiTokenAuth, "get_cde_authentication_token", return_value=VALID_CDE_TOKEN_AUTH_RESPONSE
    )
    @mock.patch.object(BaseHook, "get_connection", return_value=TEST_DEFAULT_CONNECTION)
    @mock.patch.object(
        Session,
        "send",
        side_effect=[
            _make_response(429, None, "Too Many Requests"),
            _make_response(429, None, "Too Many Requests"),
            _make_response(429, None, "Too Many Requests"),
            _make_response(429, None, "Too Many Requests", headers=[(RETRY_AFTER_HEADER, "5")]),
            _make_response(429, None, "Too Many Requests", headers=[(RETRY_AFTER_HEADER, "6")]),
            _make_response(500, None, "Internal Server Error"),
            _make_response(500, None, "Internal Server Error"),
            _make_response(201, {"id": TEST_JOB_RUN_ID}, ""),
        ],
    )
    def test_submit_job_retry_after_429_with_and_without_header(self, send_mock, connection_mock, cde_mock):
        """Ensure that 429 errors are handled properly.
        Test expectations:
        3 retries with default retry interval for HTTP 429, without 'Retry-After' header/
        2 retries with 5 and 6 seconds, respectively for HTTP 429, retry interval is parsed from
        the 'Retry-After' header.
        2 retries for the HTTP 500 responses, that is using the default exponential retry mechanism.
        """

        with self.tenacity_wait_mocks() as wait_mocks:
            wait_fixed_cls_mock = wait_mocks[0]
            wait_exp_obj_mock = wait_mocks[1]

            # 429 errors should not be taken into account by num_retries
            cde_hook = CdeHook(num_retries=4)

            run_id = cde_hook.submit_job(TEST_JOB_NAME)
            self.assertEqual(run_id, TEST_JOB_RUN_ID)
            self.assertEqual(cde_mock.call_count, 1)
            self.assertEqual(send_mock.call_count, 8)
            connection_mock.assert_called()

            LOG.debug("wait_fixed_cls_mock.mock_calls: %s", wait_fixed_cls_mock.mock_calls)
            LOG.debug("wait_exp_obj_mock.mock_calls: %s", wait_exp_obj_mock.mock_calls)

            # Ignore the first call, that comes from tenacity_wait_mocks.
            # The first call to tenacity.wait_fixed was performed to get an instance reference (mock)
            self.assertEqual(
                wait_fixed_cls_mock.call_args_list[1:],
                [
                    call(DEFAULT_RETRY_INTERVAL),
                    call(DEFAULT_RETRY_INTERVAL),
                    call(DEFAULT_RETRY_INTERVAL),
                    call(5),
                    call(6),
                ],
            )

            calls = self._filter_calls(wait_exp_obj_mock, include={""}, skip={"__str__"})
            self.assertEqual(len(calls), 2)

    @mock.patch.object(
        CdeApiTokenAuth, "get_cde_authentication_token", return_value=VALID_CDE_TOKEN_AUTH_RESPONSE
    )
    @mock.patch.object(BaseHook, "get_connection", return_value=TEST_DEFAULT_CONNECTION)
    @mock.patch.object(
        Session,
        "send",
        side_effect=[
            _make_response(500, None, "Internal Server Error"),
            _make_response(429, None, "Too Many Requests"),
            _make_response(501, None, "Some other Server Error"),
            _make_response(429, None, "Too Many Requests"),
            _make_response(502, None, "Some other Server Error 2"),
        ],
    )
    def test_submit_job_retry_fails_different_response_codes(self, send_mock, connection_mock, cde_mock):
        """Ensure that 429 errors attempts are counted separately.
        Test expectations (num_retries=3):
        500+ and 429 response codes one by one, until the num_retries is reached
        """

        with self.tenacity_wait_mocks():
            # 429 errors should not be taken into account by num_retries
            cde_hook = CdeHook(num_retries=3)

            with self.assertRaises(CdeHookException):
                cde_hook.submit_job(TEST_JOB_NAME)
            self.assertEqual(cde_mock.call_count, 1)
            self.assertEqual(send_mock.call_count, 5)
            connection_mock.assert_called()

    @mock.patch.object(
        CdeApiTokenAuth, "get_cde_authentication_token", return_value=VALID_CDE_TOKEN_AUTH_RESPONSE
    )
    @mock.patch.object(BaseHook, "get_connection", return_value=TEST_DEFAULT_CONNECTION)
    @mock.patch.object(
        Session,
        "send",
        side_effect=[
            _make_response(500, None, "Internal Server Error"),
            _make_response(429, None, "Too Many Requests"),
            _make_response(501, None, "Some other Server Error"),
            _make_response(429, None, "Too Many Requests"),
            _make_response(502, None, "Some other Server Error 2"),
            _make_response(429, None, "Too Many Requests"),
            _make_response(201, {"id": TEST_JOB_RUN_ID}, ""),
        ],
    )
    def test_submit_job_retry_succeeds_different_response_codes(self, send_mock, connection_mock, cde_mock):
        """Ensure that 429 errors attempts are counted separately.
        Test expectations (num_retries=4):
        500+ and 429 response codes one by one, until the call succeeds
        """

        with self.tenacity_wait_mocks() as wait_mocks:
            wait_exp_obj_mock = wait_mocks[1]
            # 429 errors should not be taken into account by num_retries
            cde_hook = CdeHook(num_retries=4)

            run_id = cde_hook.submit_job(TEST_JOB_NAME)
            self.assertEqual(run_id, TEST_JOB_RUN_ID)
            self.assertEqual(cde_mock.call_count, 1)
            self.assertEqual(send_mock.call_count, 7)
            connection_mock.assert_called()

            LOG.debug("wait_exp_obj_mock.mock_calls: %s", wait_exp_obj_mock.mock_calls)
            calls = self._filter_calls(wait_exp_obj_mock, include={""}, skip={"__str__"})
            self.assertEqual(len(calls), 3)

    @mock.patch.object(
        CdeApiTokenAuth, "get_cde_authentication_token", return_value=VALID_CDE_TOKEN_AUTH_RESPONSE
    )
    @mock.patch.object(BaseHook, "get_connection", return_value=TEST_DEFAULT_CONNECTION)
    @mock.patch.object(
        Session,
        "send",
        side_effect=[
            _make_response(429, None, "Too Many Requests"),
            _make_response(429, None, "Too Many Requests"),
            _make_response(429, None, "Too Many Requests"),
            _make_response(429, None, "Too Many Requests", headers=[(RETRY_AFTER_HEADER, "5")]),
            _make_response(429, None, "Too Many Requests", headers=[(RETRY_AFTER_HEADER, "6")]),
            _make_response(500, None, "Internal Server Error"),
            _make_response(500, None, "Internal Server Error"),
            _make_response(500, None, "Internal Server Error"),
            _make_response(201, {"id": TEST_JOB_RUN_ID}, ""),
        ],
    )
    def test_submit_job_retry_after_429_stop_handler(self, send_mock, connection_mock, cde_mock):
        """
        Test expectations:
        3 retries with default retry interval for HTTP 429, without 'Retry-After' header
        2 retries with 5 and 6 seconds, respectively for HTTP 429, retry interval is parsed from
        the 'Retry-After' header
        3 retries for the HTTP 500 Internal Server Error, that is using the default exponential
        retry mechanism
        5 calls to stop_after_delay from class RateLimitedStop
        5 calls to stop_after_attempt from class RateLimitedStop
        3 calls to stop_after_attempt from class CustomStop, non rate-limited scenario

        """
        with self.tenacity_wait_mocks() as wait_mocks, self.tenacity_stop_mocks() as stop_mocks:
            wait_fixed_cls_mock = wait_mocks[0]
            wait_exp_obj_mock = wait_mocks[1]

            stop_after_attempt_cls_mock = stop_mocks[0]
            stop_after_attempt_obj = stop_mocks[1]
            stop_after_delay_cls_mock = stop_mocks[2]
            stop_after_delay_obj = stop_mocks[3]

            cde_hook = CdeHook()
            run_id = cde_hook.submit_job(TEST_JOB_NAME)
            self.assertEqual(run_id, TEST_JOB_RUN_ID)
            self.assertEqual(cde_mock.call_count, 1)
            self.assertEqual(send_mock.call_count, 9)
            connection_mock.assert_called()

            LOG.debug("wait_fixed_cls_mock.mock_calls: %s", wait_fixed_cls_mock.mock_calls)
            LOG.debug("wait_exp_obj_mock.mock_calls: %s", wait_exp_obj_mock.mock_calls)
            LOG.debug("stop_after_attempt_cls_mock.mock_calls: %s", stop_after_attempt_cls_mock.mock_calls)
            LOG.debug("stop_after_attempt_obj.mock_calls: %s", stop_after_attempt_obj.mock_calls)
            LOG.debug("stop_after_delay_cls_mock.mock_calls: %s", stop_after_delay_cls_mock.mock_calls)
            LOG.debug("stop_after_delay_obj.mock_calls: %s", stop_after_delay_obj.mock_calls)

            # Ignore the first calls, that comes from tenacity_wait_mocks.
            # The first call to tenacity.wait_fixed was performed to get an instance reference (mock)
            self.assertEqual(
                wait_fixed_cls_mock.call_args_list[1:],
                [
                    call(DEFAULT_RETRY_INTERVAL),
                    call(DEFAULT_RETRY_INTERVAL),
                    call(DEFAULT_RETRY_INTERVAL),
                    call(5),
                    call(6),
                ],
            )

            calls = self._filter_calls(wait_exp_obj_mock, include={""}, skip={"__str__"})
            self.assertEqual(len(calls), 3)

            self.assertEqual(
                stop_after_attempt_cls_mock.call_args_list[1:],
                [call(CdeHook.DEFAULT_NUM_RETRIES), call(CdeHook.DEFAULT_NUM_RETRIES_RATE_LIMITED)],
            )
            self.assertEqual(
                stop_after_delay_cls_mock.call_args_list[1:],
                [call(CdeHook.DEFAULT_NUM_RETRIES_RATE_LIMITED * DEFAULT_RETRY_INTERVAL)],
            )

            self.assertEqual(stop_after_attempt_obj.call_count, 8)
            self.assertEqual(stop_after_delay_obj.call_count, 5)

    @mock.patch.object(
        CdeApiTokenAuth, "get_cde_authentication_token", return_value=VALID_CDE_TOKEN_AUTH_RESPONSE
    )
    @mock.patch.object(BaseHook, "get_connection", return_value=TEST_DEFAULT_CONNECTION)
    @mock.patch.object(
        Session,
        "send",
        side_effect=[_make_response(500, None, "Internal Server Error")] * 24
        + [_make_response(201, {"id": TEST_JOB_RUN_ID}, "")]
        + [_make_response(500, None, "Internal Server Error")],
    )
    def test_submit_job_custom_stop_normal_stop_after_attempts(self, send_mock, connection_mock, cde_mock):
        """
        This test verifies if we have 24 HTTP 500 server responses, tenacity.stop_after_attempt is
        called 24 times.
        Then, we have a successful HTTP 201 response, we don't call tenacity.stop_after_attempt anymore.
        It's also verified that tenacity.stop_after_delay is not called for regular Server errors.
        The last HTTP 500 Internal Server Error as a side effect is just added to prove that after
        HTTP 201, none of the stop objects are called.
        """

        def stop_after_attempt_impl(retry_state):
            LOG.debug("Called stop_after_attempt_impl on mock. Arg: %s", retry_state)
            return retry_state.attempt_number >= 25

        with self.tenacity_wait_mocks() as wait_mocks, self.tenacity_stop_mocks(
            stop_after_attempt_impl=stop_after_attempt_impl
        ) as stop_mocks:
            wait_fixed_cls_mock = wait_mocks[0]
            wait_exp_obj_mock = wait_mocks[1]

            stop_after_attempt_cls_mock = stop_mocks[0]
            stop_after_attempt_obj = stop_mocks[1]
            stop_after_delay_cls_mock = stop_mocks[2]
            stop_after_delay_obj = stop_mocks[3]

            cde_hook = CdeHook()
            run_id = cde_hook.submit_job(TEST_JOB_NAME)
            self.assertEqual(run_id, TEST_JOB_RUN_ID)
            self.assertEqual(cde_mock.call_count, 1)
            self.assertEqual(send_mock.call_count, 25)
            connection_mock.assert_called()

            LOG.debug("wait_fixed_cls_mock.mock_calls: %s", wait_fixed_cls_mock.mock_calls)
            LOG.debug("wait_exp_obj_mock.mock_calls: %s", wait_exp_obj_mock.mock_calls)
            LOG.debug("stop_after_attempt_cls_mock.mock_calls: %s", stop_after_attempt_cls_mock.mock_calls)
            LOG.debug("stop_after_attempt_obj.mock_calls: %s", stop_after_attempt_obj.mock_calls)
            LOG.debug("stop_after_delay_cls_mock.mock_calls: %s", stop_after_delay_cls_mock.mock_calls)
            LOG.debug("stop_after_delay_obj.mock_calls: %s", stop_after_delay_obj.mock_calls)

            # Assert 24 calls to stop_after_attempt from CustomStop, non rate-limited scenario
            # Assert 0 calls to stop_after_delay_obj from CustomStop, non rate-limited scenario
            self.assertEqual(stop_after_attempt_obj.call_count, 24)
            self.assertEqual(stop_after_delay_obj.call_count, 0)

    @mock.patch.object(
        CdeApiTokenAuth, "get_cde_authentication_token", return_value=VALID_CDE_TOKEN_AUTH_RESPONSE
    )
    @mock.patch.object(BaseHook, "get_connection", return_value=TEST_DEFAULT_CONNECTION)
    @mock.patch.object(
        Session,
        "send",
        side_effect=[_make_response(429, None, "Too Many Requests", headers=[(RETRY_AFTER_HEADER, "5")])] * 24
        + [_make_response(201, {"id": TEST_JOB_RUN_ID}, "")]
        + [_make_response(500, None, "Internal Server Error")] * 999,
    )
    def test_submit_job_custom_stop_retry_after_429_stop_after_attempts(
        self, send_mock, connection_mock, cde_mock
    ):
        """
        This test verifies if we have 24 HTTP 429 server responses, tenacity.stop_after_attempt
        and tenacity.stop_after_delay is called 24 times.
        Then, we have a successful HTTP 201 response, we don't call tenacity.stop_after_attempt
        and tenacity.stop_after_delay anymore.
        """

        def stop_after_attempt_impl(retry_state):
            LOG.debug("Called stop_after_attempt_impl on mock. Arg: %s", retry_state)
            return retry_state.attempt_number >= 25

        with self.tenacity_wait_mocks() as wait_mocks, self.tenacity_stop_mocks(
            stop_after_attempt_impl=stop_after_attempt_impl
        ) as stop_mocks:
            wait_fixed_cls_mock = wait_mocks[0]
            wait_exp_obj_mock = wait_mocks[1]

            stop_after_attempt_cls_mock = stop_mocks[0]
            stop_after_attempt_obj = stop_mocks[1]
            stop_after_delay_cls_mock = stop_mocks[2]
            stop_after_delay_obj = stop_mocks[3]

            cde_hook = CdeHook()
            run_id = cde_hook.submit_job(TEST_JOB_NAME)
            self.assertEqual(run_id, TEST_JOB_RUN_ID)
            self.assertEqual(cde_mock.call_count, 1)
            self.assertEqual(send_mock.call_count, 25)
            connection_mock.assert_called()

            LOG.debug("wait_fixed_cls_mock.mock_calls: %s", wait_fixed_cls_mock.mock_calls)
            LOG.debug("wait_exp_obj_mock.mock_calls: %s", wait_exp_obj_mock.mock_calls)
            LOG.debug("stop_after_attempt_cls_mock.mock_calls: %s", stop_after_attempt_cls_mock.mock_calls)
            LOG.debug("stop_after_attempt_obj.mock_calls: %s", stop_after_attempt_obj.mock_calls)
            LOG.debug("stop_after_delay_cls_mock.mock_calls: %s", stop_after_delay_cls_mock.mock_calls)
            LOG.debug("stop_after_delay_obj.mock_calls: %s", stop_after_delay_obj.mock_calls)

            # Assert 24 calls to stop_after_attempt & stop_after_delay_obj from CustomStop,
            # rate-limited scenario
            self.assertEqual(stop_after_attempt_obj.call_count, 24)
            self.assertEqual(stop_after_delay_obj.call_count, 24)

    @mock.patch.object(
        CdeApiTokenAuth, "get_cde_authentication_token", return_value=VALID_CDE_TOKEN_AUTH_RESPONSE
    )
    @mock.patch.object(BaseHook, "get_connection", return_value=TEST_DEFAULT_CONNECTION)
    @mock.patch.object(
        Session,
        "send",
        side_effect=[_make_response(429, None, "Too Many Requests", headers=[(RETRY_AFTER_HEADER, "5")])] * 5
        + [_make_response(201, {"id": TEST_JOB_RUN_ID}, "")],
    )
    def test_submit_job_custom_stop_retry_after_429_stop_after_delay(
        self, send_mock, connection_mock, cde_mock
    ):
        """This test verifies if we have 5 HTTP 429 server responses, tenacity.stop_after_delay"""

        def stop_after_delay_impl(retry_state):
            LOG.debug("Called stop_after_delay_impl on mock. Arg: %s", retry_state)
            return retry_state.seconds_since_start >= 5

        def wait_1_second(retry_state):
            LOG.debug("Called wait_fixed or wait_exponential mock. Arg: %s", retry_state)
            return 1

        with self.tenacity_wait_mocks(wait_impl=wait_1_second) as wait_mocks, self.tenacity_stop_mocks(
            stop_after_delay_impl=stop_after_delay_impl
        ) as stop_mocks:
            wait_fixed_cls_mock = wait_mocks[0]
            wait_exp_obj_mock = wait_mocks[1]

            stop_after_attempt_cls_mock = stop_mocks[0]
            stop_after_attempt_obj = stop_mocks[1]
            stop_after_delay_cls_mock = stop_mocks[2]
            stop_after_delay_obj = stop_mocks[3]

            cde_hook = CdeHook()
            run_id = cde_hook.submit_job(TEST_JOB_NAME)
            self.assertEqual(run_id, TEST_JOB_RUN_ID)
            self.assertEqual(cde_mock.call_count, 1)
            self.assertEqual(send_mock.call_count, 6)
            connection_mock.assert_called()

            LOG.debug("wait_fixed_cls_mock.mock_calls: %s", wait_fixed_cls_mock.mock_calls)
            LOG.debug("wait_exp_obj_mock.mock_calls: %s", wait_exp_obj_mock.mock_calls)
            LOG.debug("stop_after_attempt_cls_mock.mock_calls: %s", stop_after_attempt_cls_mock.mock_calls)
            LOG.debug("stop_after_attempt_obj.mock_calls: %s", stop_after_attempt_obj.mock_calls)
            LOG.debug("stop_after_delay_cls_mock.mock_calls: %s", stop_after_delay_cls_mock.mock_calls)
            LOG.debug("stop_after_delay_obj.mock_calls: %s", stop_after_delay_obj.mock_calls)

            # Assert 5 calls to stop_after_attempt & stop_after_delay_obj from CustomStop,
            # rate-limited scenario
            self.assertEqual(stop_after_attempt_obj.call_count, 5)
            self.assertEqual(stop_after_delay_obj.call_count, 5)

    @staticmethod
    def _filter_calls(mock_param, include: set[str], skip: set[str]):
        captured_calls = []
        for name, args, kwargs in mock_param.mock_calls:
            if name in skip:
                LOG.debug(
                    "Skipping call from verification. name: %s, args: %s, kwargs: %s",
                    name,
                    args,
                    kwargs,
                )
                continue
            if name in include:
                LOG.debug("Call details: name: %s, args: %s, kwargs: %s", name, args, kwargs)
                captured_calls.append((name, args, kwargs))
            else:
                raise ValueError(
                    f"Unexpected call, not in include list nor exclude list! "
                    f"mock_param: {mock_param}, name: {name}, args: {args}, kwargs: {kwargs}"
                )
        return captured_calls

    @mock.patch.object(
        CdeApiTokenAuth, "get_cde_authentication_token", return_value=VALID_CDE_TOKEN_AUTH_RESPONSE
    )
    @mock.patch.object(BaseHook, "get_connection", return_value=TEST_DEFAULT_CONNECTION)
    @mock.patch.object(
        Session,
        "send",
        side_effect=[
            _make_response(503, None, "Internal Server Error"),
            _make_response(500, None, "Internal Server Error"),
            _make_response(201, {"id": TEST_JOB_RUN_ID}, ""),
        ],
    )
    def test_submit_job_retry_after_5xx_works(self, send_mock, connection_mock, cde_mock):
        """Ensure that 5xx errors are retried"""
        cde_hook = CdeHook()
        run_id = cde_hook.submit_job(TEST_JOB_NAME)
        self.assertEqual(run_id, TEST_JOB_RUN_ID)
        self.assertEqual(cde_mock.call_count, 1)
        self.assertEqual(send_mock.call_count, 3)
        connection_mock.assert_called()

    @mock.patch(GET_CDE_AUTH_TOKEN_METHOD, return_value=VALID_CDE_TOKEN_AUTH_RESPONSE)
    @mock.patch.object(BaseHook, "get_connection", return_value=TEST_DEFAULT_CONNECTION)
    @mock.patch.object(Session, "send", return_value=_make_response(404, None, "Not Found"))
    def test_submit_job_fails_immediately_for_4xx(self, send_mock, connection_mock, cde_mock):
        """Ensure that 4xx errors are _not_ retried"""
        cde_hook = CdeHook()
        with self.assertRaises(CdeHookException) as err:
            cde_hook.submit_job(TEST_JOB_NAME)
        self.assertEqual(send_mock.call_count, 1)
        self.assertIsInstance(err.exception.raised_from, AirflowException)
        cde_mock.assert_called()
        connection_mock.assert_called()

    @mock.patch(GET_CDE_AUTH_TOKEN_METHOD, return_value=VALID_CDE_TOKEN_AUTH_RESPONSE)
    @mock.patch.object(BaseHook, "get_connection", return_value=TEST_DEFAULT_CONNECTION)
    @mock.patch.object(
        Session,
        "send",
        side_effect=[
            _make_response(503, None, "Internal Server Error"),
            _make_response(409, {"id": TEST_JOB_RUN_ID}, ""),
        ],
    )
    def test_submit_job_retries_exit_after_409(self, send_mock, connection_mock, cde_mock):
        """Ensure that after 409 response hook exit the retries"""
        cde_hook = CdeHook()
        run_id = cde_hook.submit_job(TEST_JOB_NAME)
        self.assertEqual(run_id, TEST_JOB_RUN_ID)
        self.assertEqual(cde_mock.call_count, 1)
        self.assertEqual(send_mock.call_count, 2)
        connection_mock.assert_called()

    @mock.patch(GET_CDE_AUTH_TOKEN_METHOD, return_value=VALID_CDE_TOKEN_AUTH_RESPONSE)
    @mock.patch.object(Session, "send", return_value=_make_response(201, {"id": TEST_JOB_RUN_ID}, ""))
    @mock.patch.object(
        BaseHook,
        "get_connection",
        return_value=_get_test_connection(extra='{"insecure": true, "region": "us-west-1"}'),
    )
    def test_submit_job_insecure(self, connection_mock, session_send_mock, cde_mock):
        """Ensure insecure mode is taken into account"""
        cde_hook = CdeHook()
        run_id = cde_hook.submit_job(TEST_JOB_NAME)
        self.assertEqual(run_id, TEST_JOB_RUN_ID)
        cde_mock.assert_called()
        connection_mock.assert_called()
        session_send_mock.assert_called()
        called_args = _get_call_arguments(session_send_mock.call_args)
        self.assertEqual(called_args["verify"], False)

    @mock.patch(GET_CDE_AUTH_TOKEN_METHOD, return_value=VALID_CDE_TOKEN_AUTH_RESPONSE)
    @mock.patch.object(Session, "send", return_value=_make_response(201, {"id": TEST_JOB_RUN_ID}, ""))
    @mock.patch.object(
        BaseHook,
        "get_connection",
        return_value=_get_test_connection(extra='{"region": "us-west-1"}'),
    )
    def test_submit_job_no_custom_ca_certificate(self, connection_mock, session_send_mock, cde_mock):
        """Ensure that default TLS security configuration runs fine"""
        cde_hook = CdeHook()
        run_id = cde_hook.submit_job(TEST_JOB_NAME)
        self.assertEqual(run_id, TEST_JOB_RUN_ID)
        cde_mock.assert_called()
        connection_mock.assert_called()
        session_send_mock.assert_called()
        called_args = _get_call_arguments(session_send_mock.call_args)
        self.assertEqual(called_args["verify"], True)

    @mock.patch(GET_CDE_AUTH_TOKEN_METHOD, return_value=VALID_CDE_TOKEN_AUTH_RESPONSE)
    @mock.patch.object(Session, "send", return_value=_make_response(201, {"id": TEST_JOB_RUN_ID}, ""))
    @mock.patch.object(BaseHook, "get_connection", return_value=TEST_DEFAULT_CONNECTION)
    def test_submit_job_custom_ca_certificate(self, connection_mock, session_send_mock, cde_mock):
        """Ensure custom is taken into account"""
        cde_hook = CdeHook()
        run_id = cde_hook.submit_job(TEST_JOB_NAME)
        self.assertEqual(run_id, TEST_JOB_RUN_ID)
        cde_mock.assert_called()
        connection_mock.assert_called()
        session_send_mock.assert_called()
        called_args = _get_call_arguments(session_send_mock.call_args)
        self.assertEqual(called_args["verify"], TEST_CUSTOM_CA_CERTIFICATE)

    @mock.patch.object(
        BaseHook, "get_connection", return_value=_get_test_connection(extra='{"cache_dir": " "}')
    )
    def test_wrong_cache_dir(self, connection_mock):
        """Ensure that CdeHook object creation fails if cache dir value is wrong"""
        cde_hook = CdeHook()
        with self.assertRaises(CdeHookException):
            cde_hook.submit_job(TEST_JOB_NAME)
        connection_mock.assert_called()

    @mock.patch(GET_CDE_AUTH_TOKEN_METHOD, return_value=VALID_CDE_TOKEN_AUTH_RESPONSE)
    @mock.patch.object(Session, "send", return_value=_make_response(201, {"id": TEST_JOB_RUN_ID}, ""))
    @mock.patch.object(BaseHook, "get_connection", return_value=TEST_DEFAULT_CONNECTION)
    def test_submit_job_custom_timeout_success(self, connection_mock, session_send_mock, cde_mock):
        """Ensure custom timeout is taken into account
        and succeeds as expected if the request does not timeout."""

        # Regular test - request completes below timeout
        cde_hook = CdeHook(api_timeout=10)
        run_id = cde_hook.submit_job(TEST_JOB_NAME)
        self.assertEqual(run_id, TEST_JOB_RUN_ID)
        cde_mock.assert_called()
        connection_mock.assert_called()
        session_send_mock.assert_called()

    @mock.patch.dict(os.environ, {"AIRFLOW__CDE__DEFAULT_API_TIMEOUT": "10"})
    @mock.patch(GET_CDE_AUTH_TOKEN_METHOD, return_value=VALID_CDE_TOKEN_AUTH_RESPONSE)
    @mock.patch.object(Session, "send", return_value=_make_response(201, {"id": TEST_JOB_RUN_ID}, ""))
    @mock.patch.object(BaseHook, "get_connection", return_value=TEST_DEFAULT_CONNECTION)
    def test_submit_job_custom_env_timeout_success(self, connection_mock, session_send_mock, cde_mock):
        """Ensure custom timeout from the env var is taken into account
        and succeeds as expected if the request does not timeout."""

        # Regular test - request completes below timeout
        cde_hook = CdeHook()
        run_id = cde_hook.submit_job(TEST_JOB_NAME)
        self.assertEqual(run_id, TEST_JOB_RUN_ID)
        cde_mock.assert_called()
        connection_mock.assert_called()
        session_send_mock.assert_called()

    @mock.patch(GET_CDE_AUTH_TOKEN_METHOD, return_value=VALID_CDE_TOKEN_AUTH_RESPONSE)
    @mock.patch.object(Session, "send", side_effect=Timeout())
    @mock.patch.object(BaseHook, "get_connection", return_value=TEST_DEFAULT_CONNECTION)
    def test_submit_job_custom_timeout_failure(self, connection_mock, session_send_mock: mock.Mock, cde_mock):
        """Ensure custom timeout is taken into account and fails as expected if request keeps timing out."""
        # Request times out
        cde_hook = CdeHook(api_timeout=3, num_retries=0)
        with self.assertRaises(CdeHookException) as err:
            cde_hook.submit_job(TEST_JOB_NAME)

        cde_mock.assert_called()
        connection_mock.assert_called()
        session_send_mock.assert_called()

        retry_error: RetryError = err.exception.raised_from
        last_attempt: Future = retry_error.last_attempt
        self.assertIsInstance(last_attempt.exception(), Timeout)

    @mock.patch(GET_CDE_AUTH_TOKEN_METHOD, return_value=VALID_CDE_TOKEN_AUTH_RESPONSE)
    @mock.patch.object(
        Session,
        "send",
        return_value=_make_response(201, {"id": TEST_JOB_RUN_ID}, ""),
        side_effect=[
            ConnectionError(),
            ConnectionError(),
            # Returns what is specified in return_value
            mock.DEFAULT,
        ],
    )
    @mock.patch.object(BaseHook, "get_connection", return_value=TEST_DEFAULT_CONNECTION)
    def test_submit_job_custom_api_retries_success(self, connection_mock, session_send_mock, cde_mock):
        """Ensure custom api_retries is taken into account and succeeds as expected.

        Set retry number to 4 times but the third call will be successful so it will succeed.
        """
        cde_hook = CdeHook(num_retries=4)
        run_id = cde_hook.submit_job(TEST_JOB_NAME)
        self.assertEqual(run_id, TEST_JOB_RUN_ID)
        cde_mock.assert_called()
        connection_mock.assert_called()
        self.assertEqual(session_send_mock.call_count, 3)

    @mock.patch.dict(os.environ, {"AIRFLOW__CDE__DEFAULT_NUM_RETRIES": "4"})
    @mock.patch(GET_CDE_AUTH_TOKEN_METHOD, return_value=VALID_CDE_TOKEN_AUTH_RESPONSE)
    @mock.patch.object(
        Session,
        "send",
        return_value=_make_response(201, {"id": TEST_JOB_RUN_ID}, ""),
        side_effect=[
            ConnectionError(),
            ConnectionError(),
            # Returns what is specified in return_value
            mock.DEFAULT,
        ],
    )
    @mock.patch.object(BaseHook, "get_connection", return_value=TEST_DEFAULT_CONNECTION)
    def test_submit_job_custom_env_api_retries_success(self, connection_mock, session_send_mock, cde_mock):
        """Ensure custom api_retries is taken into account from the env var and succeeds as expected.

        Set retry number to 4 times but the third call will be successful, so it will succeed.
        """
        cde_hook = CdeHook()
        run_id = cde_hook.submit_job(TEST_JOB_NAME)
        self.assertEqual(run_id, TEST_JOB_RUN_ID)
        cde_mock.assert_called()
        connection_mock.assert_called()
        self.assertEqual(session_send_mock.call_count, 3)

    @mock.patch.dict(os.environ, {"AIRFLOW__CDE__DEFAULT_NUM_RETRIES": "4asdf"})
    @mock.patch(GET_CDE_AUTH_TOKEN_METHOD, return_value=VALID_CDE_TOKEN_AUTH_RESPONSE)
    @mock.patch.object(
        Session,
        "send",
        return_value=_make_response(201, {"id": TEST_JOB_RUN_ID}, ""),
        side_effect=[
            ConnectionError(),
            ConnectionError(),
            ConnectionError(),
            ConnectionError(),
            ConnectionError(),
            ConnectionError(),
            ConnectionError(),
            ConnectionError(),
            # Returns what is specified in return_value
            mock.DEFAULT,
        ],
    )
    @mock.patch.object(BaseHook, "get_connection", return_value=TEST_DEFAULT_CONNECTION)
    def test_submit_job_custom_env_api_default_num_retries_success(
        self, connection_mock, session_send_mock, cde_mock
    ):
        """The default num retries value should be used if a wrong value is set via the
        AIRFLOW__CDE_DEFAULT_NUM_RETRIES environment variable.
        The call will succeed for the 9th call which is the CdeHook.DEFAULT_NUM_RETRIES value.
        """
        with self.tenacity_wait_mocks():
            cde_hook = CdeHook()
            run_id = cde_hook.submit_job(TEST_JOB_NAME)
            self.assertEqual(run_id, TEST_JOB_RUN_ID)
            cde_mock.assert_called()
            connection_mock.assert_called()
            self.assertEqual(session_send_mock.call_count, CdeHook.DEFAULT_NUM_RETRIES)

    @mock.patch(GET_CDE_AUTH_TOKEN_METHOD, return_value=VALID_CDE_TOKEN_AUTH_RESPONSE)
    @mock.patch.object(Session, "send", side_effect=ConnectionError())
    @mock.patch.object(BaseHook, "get_connection", return_value=TEST_DEFAULT_CONNECTION)
    def test_submit_job_custom_api_retries_failure(self, connection_mock, session_send_mock, cde_mock):
        """Ensure custom api_retries is taken into account and fails as expected.

        The request keeps failing so the job sumbission will end up in failure after exhausiting the
        number of retries.
        """
        cde_hook = CdeHook(num_retries=3)
        with self.assertRaises(CdeHookException) as err:
            cde_hook.submit_job(TEST_JOB_NAME)

        cde_mock.assert_called()
        connection_mock.assert_called()
        self.assertEqual(session_send_mock.call_count, 3)

        retry_error: RetryError = err.exception.raised_from
        last_attempt: Future = retry_error.last_attempt
        self.assertIsInstance(last_attempt.exception(), ConnectionError)

    @mock.patch(GET_CDE_AUTH_TOKEN_METHOD, return_value=VALID_CDE_TOKEN_AUTH_RESPONSE)
    @mock.patch.object(Session, "send", side_effect=HTTPError())
    @mock.patch.object(BaseHook, "get_connection", return_value=TEST_DEFAULT_CONNECTION)
    def test_submit_job_non_always_retry_exception(self, connection_mock, session_send_mock, cde_mock):
        """Ensure no retry is attempted if the request exception
        is not part of the ALWAYS_RETRY_EXCEPTIONS list"""

        # Request times out
        cde_hook = CdeHook()
        with self.assertRaises(CdeHookException) as err:
            cde_hook.submit_job(TEST_JOB_NAME)

        cde_mock.assert_called()
        connection_mock.assert_called()
        # Only called once because never retried
        (session_send_mock.call_count, 1)

        self.assertIsInstance(err.exception.raised_from, HTTPError)

    @mock.patch(GET_CDE_AUTH_TOKEN_METHOD, return_value=VALID_CDE_TOKEN_AUTH_RESPONSE)
    @mock.patch.object(Session, "send", return_value=_make_response(201, None, ""))
    @mock.patch.object(BaseHook, "get_connection", return_value=TEST_DEFAULT_CONNECTION)
    def test_kill_job_empty_response_none(self, connection_mock, session_send_mock, cde_mock):
        """Test request of job run deletion from CDE API"""
        cde_hook = CdeHook()
        cde_hook.kill_job_run(TEST_JOB_NAME)
        cde_mock.assert_called()
        connection_mock.assert_called()
        session_send_mock.assert_called()

    @mock.patch(GET_CDE_AUTH_TOKEN_METHOD, return_value=VALID_CDE_TOKEN_AUTH_RESPONSE)
    @mock.patch.object(Session, "send", return_value=_make_response(201, b"", ""))
    @mock.patch.object(BaseHook, "get_connection", return_value=TEST_DEFAULT_CONNECTION)
    def test_kill_job_empty_response_bytes(self, connection_mock, session_send_mock, cde_mock):
        """Test request of job run deletion from CDE API"""
        cde_hook = CdeHook()
        cde_hook.kill_job_run(TEST_JOB_NAME)
        cde_mock.assert_called()
        connection_mock.assert_called()
        session_send_mock.assert_called()

    @mock.patch.dict(os.environ, {"AIRFLOW__CDE__DEFAULT_API_TIMEOUT": "400"})
    @mock.patch(GET_CDE_AUTH_TOKEN_METHOD, return_value=VALID_CDE_TOKEN_AUTH_RESPONSE)
    @mock.patch.object(Session, "send", return_value=_make_response(201, b"", ""))
    @mock.patch.object(BaseHook, "get_connection", return_value=TEST_DEFAULT_CONNECTION)
    def test_kill_job_empty_response_bytes_timeout(self, connection_mock, session_send_mock, cde_mock):
        """Test request of job run deletion from CDE API with modified timeout value"""
        cde_hook = CdeHook()
        cde_hook.kill_job_run(TEST_JOB_NAME)
        cde_mock.assert_called()
        connection_mock.assert_called()
        session_send_mock.assert_called_with(
            mock.ANY,
            allow_redirects=mock.ANY,
            cert=mock.ANY,
            proxies=mock.ANY,
            stream=mock.ANY,
            timeout=400,
            verify=mock.ANY,
        )

    @mock.patch(GET_CDE_AUTH_TOKEN_METHOD, return_value=VALID_CDE_TOKEN_AUTH_RESPONSE)
    @mock.patch.object(Session, "send", return_value=_make_response(201, INVALID_JSON_STRING, ""))
    @mock.patch.object(BaseHook, "get_connection", return_value=TEST_DEFAULT_CONNECTION)
    def test_kill_job_invalid_json(self, connection_mock, session_send_mock, cde_mock):
        """Test invalid JSON response on job run deletion from CDE API"""
        cde_hook = CdeHook()
        with self.assertRaises(CdeHookException) as err:
            cde_hook.kill_job_run(TEST_JOB_NAME)
        self.assertIsInstance(err.exception.raised_from, JSONDecodeError)
        cde_mock.assert_called()
        connection_mock.assert_called()
        session_send_mock.assert_called()

    @mock.patch(GET_CDE_AUTH_TOKEN_METHOD, return_value=VALID_CDE_TOKEN_AUTH_RESPONSE)
    @mock.patch.object(Session, "send", return_value=_make_response(201, {"status": TEST_JOB_RUN_STATUS}, ""))
    @mock.patch.object(BaseHook, "get_connection", return_value=TEST_DEFAULT_CONNECTION)
    def test_check_job_run_status(self, connection_mock, session_send_mock, cde_mock):
        """Test a successful request of job run status from CDE API"""
        cde_hook = CdeHook()
        status = cde_hook.check_job_run_status(TEST_JOB_RUN_ID)
        self.assertEqual(status, TEST_JOB_RUN_STATUS)
        cde_mock.assert_called()
        connection_mock.assert_called()
        session_send_mock.assert_called()

    @mock.patch(GET_CDE_AUTH_TOKEN_METHOD, return_value=VALID_CDE_TOKEN_AUTH_RESPONSE)
    @mock.patch.object(Session, "send", return_value=_make_response(201, {"status": TEST_JOB_RUN_STATUS}, ""))
    @mock.patch.object(BaseHook, "get_connection", return_value=TEST_DEFAULT_CONNECTION)
    def test_check_job_run_status_success_supplied_timeout(
        self, connection_mock, session_send_mock, cde_mock
    ):
        """Test a successful request of job run status from CDE API"""
        cde_hook = CdeHook()
        status = cde_hook.check_job_run_status(TEST_JOB_RUN_ID)
        self.assertEqual(status, TEST_JOB_RUN_STATUS)
        cde_mock.assert_called()
        connection_mock.assert_called()
        session_send_mock.assert_called_with(
            mock.ANY,
            allow_redirects=True,
            cert=None,
            proxies=mock.ANY,
            stream=False,
            timeout=CdeHook.DEFAULT_API_TIMEOUT // 10,
            verify='/ca_cert/letsencrypt-stg-root-x1.pem',
        )

    @mock.patch.dict(os.environ, {"AIRFLOW__CDE__DEFAULT_API_TIMEOUT": "450"})
    @mock.patch(GET_CDE_AUTH_TOKEN_METHOD, return_value=VALID_CDE_TOKEN_AUTH_RESPONSE)
    @mock.patch.object(Session, "send", return_value=_make_response(201, {"status": TEST_JOB_RUN_STATUS}, ""))
    @mock.patch.object(BaseHook, "get_connection", return_value=TEST_DEFAULT_CONNECTION)
    def test_check_job_run_status_minimal_timeout(self, connection_mock, session_send_mock, cde_mock):
        """Test the used minimal timeout value for job status call"""
        cde_hook = CdeHook()
        status = cde_hook.check_job_run_status(TEST_JOB_RUN_ID)
        self.assertEqual(status, TEST_JOB_RUN_STATUS)
        cde_mock.assert_called()
        connection_mock.assert_called()
        session_send_mock.assert_called_with(
            mock.ANY,
            allow_redirects=mock.ANY,
            cert=mock.ANY,
            proxies=mock.ANY,
            stream=mock.ANY,
            timeout=450 // 10,
            verify=mock.ANY,
        )

    @mock.patch.dict(os.environ, {"AIRFLOW__CDE__DEFAULT_API_TIMEOUT": "10"})
    @mock.patch(GET_CDE_AUTH_TOKEN_METHOD, return_value=VALID_CDE_TOKEN_AUTH_RESPONSE)
    @mock.patch.object(Session, "send", return_value=_make_response(201, {"status": TEST_JOB_RUN_STATUS}, ""))
    @mock.patch.object(BaseHook, "get_connection", return_value=TEST_DEFAULT_CONNECTION)
    def test_check_job_run_status_less_default_timeout_env(
        self, connection_mock, session_send_mock, cde_mock
    ):
        """Test the default minimal timeout value for job status even if a
        lower value was set by the env var."""
        cde_hook = CdeHook()
        status = cde_hook.check_job_run_status(TEST_JOB_RUN_ID)
        self.assertEqual(status, TEST_JOB_RUN_STATUS)
        cde_mock.assert_called()
        connection_mock.assert_called()
        session_send_mock.assert_called_with(
            mock.ANY,
            allow_redirects=mock.ANY,
            cert=mock.ANY,
            proxies=mock.ANY,
            stream=mock.ANY,
            timeout=CdeHook.DEFAULT_API_TIMEOUT // 10,
            verify=mock.ANY,
        )

    @mock.patch(GET_CDE_AUTH_TOKEN_METHOD, return_value=VALID_CDE_TOKEN_AUTH_RESPONSE)
    @mock.patch.object(Session, "send", return_value=_make_response(201, {"status": TEST_JOB_RUN_STATUS}, ""))
    @mock.patch.object(BaseHook, "get_connection", return_value=TEST_DEFAULT_CONNECTION)
    def test_check_job_run_status_less_default_timeout_init(
        self, connection_mock, session_send_mock, cde_mock
    ):
        """Test the default minimal timeout value for job status even if a
        lower value was set by a parameter."""
        cde_hook = CdeHook(api_timeout=10)
        status = cde_hook.check_job_run_status(TEST_JOB_RUN_ID)
        self.assertEqual(status, TEST_JOB_RUN_STATUS)
        cde_mock.assert_called()
        connection_mock.assert_called()
        session_send_mock.assert_called_with(
            mock.ANY,
            allow_redirects=mock.ANY,
            cert=mock.ANY,
            proxies=mock.ANY,
            stream=mock.ANY,
            timeout=CdeHook.DEFAULT_API_TIMEOUT // 10,
            verify=mock.ANY,
        )

    @mock.patch(GET_CDE_AUTH_TOKEN_METHOD, return_value=VALID_CDE_TOKEN_AUTH_RESPONSE)
    @mock.patch.object(Session, "send", return_value=_make_response(201, None, ""))
    @mock.patch.object(BaseHook, "get_connection", return_value=TEST_DEFAULT_CONNECTION)
    def test_check_job_run_status_empty_response_none(self, connection_mock, session_send_mock, cde_mock):
        """Test a fail on empty None response from CDE API"""
        cde_hook = CdeHook()
        with self.assertRaises(CdeHookException) as err:
            cde_hook.check_job_run_status(TEST_JOB_RUN_ID)
        # Ensure that there is no previous exceptions in Exception stack
        self.assertFalse(hasattr(err.exception.raised_from, "raised_from"))
        cde_mock.assert_called()
        connection_mock.assert_called()
        session_send_mock.assert_called()

    @mock.patch(GET_CDE_AUTH_TOKEN_METHOD, return_value=VALID_CDE_TOKEN_AUTH_RESPONSE)
    @mock.patch.object(Session, "send", return_value=_make_response(201, b"", ""))
    @mock.patch.object(BaseHook, "get_connection", return_value=TEST_DEFAULT_CONNECTION)
    def test_check_job_run_status_empty_response_bytes(self, connection_mock, session_send_mock, cde_mock):
        """Test a fail on empty bytes() response from CDE API"""
        cde_hook = CdeHook()
        with self.assertRaises(CdeHookException) as err:
            cde_hook.check_job_run_status(TEST_JOB_RUN_ID)
        # Ensure that there is no previous exceptions in Exception stack
        self.assertFalse(hasattr(err.exception.raised_from, "raised_from"))
        cde_mock.assert_called()
        connection_mock.assert_called()
        session_send_mock.assert_called()

    @mock.patch(GET_CDE_AUTH_TOKEN_METHOD, return_value=VALID_CDE_TOKEN_AUTH_RESPONSE)
    @mock.patch.object(Session, "send", return_value=_make_response(201, {"wrong": "wrong"}, ""))
    @mock.patch.object(BaseHook, "get_connection", return_value=TEST_DEFAULT_CONNECTION)
    def test_check_job_run_status_invalid_key(self, connection_mock, session_send_mock, cde_mock):
        """Test a fail on incorrect response from CDE API"""
        cde_hook = CdeHook()
        with self.assertRaises(CdeHookException) as err:
            cde_hook.check_job_run_status(TEST_JOB_RUN_ID)
        self.assertIsInstance(err.exception.raised_from, KeyError)
        cde_mock.assert_called()
        connection_mock.assert_called()
        session_send_mock.assert_called()

    @mock.patch(GET_CDE_AUTH_TOKEN_METHOD, return_value=VALID_CDE_TOKEN_AUTH_RESPONSE)
    @mock.patch.object(Session, "send", return_value=_make_response(201, INVALID_JSON_STRING, ""))
    @mock.patch.object(BaseHook, "get_connection", return_value=TEST_DEFAULT_CONNECTION)
    def test_check_job_run_status_invalid_json(self, connection_mock, session_send_mock, cde_mock):
        """Test a fail on response with invalid JSON from CDE API"""
        cde_hook = CdeHook()
        with self.assertRaises(CdeHookException) as err:
            cde_hook.check_job_run_status(TEST_JOB_RUN_ID)
        self.assertIsInstance(err.exception.raised_from, JSONDecodeError)
        cde_mock.assert_called()
        connection_mock.assert_called()
        session_send_mock.assert_called()


if __name__ == "__main__":
    unittest.main()
