#  Cloudera Airflow Provider
#  Copyright (c) Cloudera, Inc. 2021-2026
#  All rights reserved.
#  Applicable Open Source License: Apache License Version 2.0
#
#  NOTE: Cloudera software products are modular software products
#  made up of hundreds of individual components, each of which was
#  individually copyrighted.  Each Cloudera software product is a
#  collective work under U.S. Copyright Law. Your license to use the
#  collective work is as provided in your written agreement with
#  Cloudera.  Used apart from the collective work, this specific file or component is
#  licensed for your use pursuant to the open source license
#  identified above.
#
#  Cloudera software products are provided to you pursuant a written agreement with
#  (i) Cloudera, Inc. or (ii) a third-party authorized to distribute
#  the Cloudera software products. If you do not have a written agreement with Cloudera nor
#  with an authorized and properly licensed third party, you do not
#  have any rights to access nor to use any Cloudera software product.
#
#  Absent a written agreement with Cloudera, Inc. ("Cloudera") to the
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
"""Tests for legacy suffix naming compatibility."""

from unittest import TestCase, mock
from unittest.mock import Mock

from airflow.models.connection import Connection
from cloudera.airflow.providers.hooks.cde import CdeHook, CdeHookException
from cloudera.airflow.providers.operators.cde import CdeRunJobOperator

TEST_JOB_NAME = "testjob"
TEST_JOB_RUN_ID = 10
TEST_VARIABLES = {"var1": ("someval_{{ ds_nodash }}")}
TEST_OVERRIDES = {"spark": {"conf": {"myparam": ("val_{{ ds_nodash }}")}}}
TEST_TIMEOUT = 4
TEST_JOB_POLL_INTERVAL = 1

TEST_HOST = "vc1.cde-2.cdp-3.cloudera.site"
TEST_SCHEME = "http"
TEST_PORT = 9090
TEST_AK = "access_key"
TEST_PK = "private_key"
TEST_CUSTOM_CA_CERTIFICATE = "/ca_cert/letsencrypt-stg-root-x1.pem"
TEST_EXTRA = (
    f'{{"access_key": "{TEST_AK}", "private_key": "{TEST_PK}","ca_cert": "{TEST_CUSTOM_CA_CERTIFICATE}"}}'
)

TEST_DEFAULT_CONNECTION = Connection(
    conn_id=CdeHook.DEFAULT_CONN_ID,
    conn_type="http",
    host=TEST_HOST,
    port=TEST_PORT,
    schema=TEST_SCHEME,
    extra=TEST_EXTRA,
)


class CdeRunJobOperatorTest(TestCase):
    """Test cases for CdeRunJobOperator"""

    @mock.patch.object(
        CdeHook,
        "get_connection",
        return_value=TEST_DEFAULT_CONNECTION,
    )
    def test_cde_hook_legacy_suffix_imports(
        self,
        get_connection_mock: Mock,
    ):
        """Test legacy hook, exception and connection"""
        from cloudera.airflow.providers.hooks.cde_hook import (
            CdeHook as LegacyCdeHook,
            CdeHookException as LegacyCdeHookException,
        )

        old_cde_hook = LegacyCdeHook()
        old_cde_hook_exception = LegacyCdeHookException()
        self.assertIsInstance(
            old_cde_hook,
            CdeHook,
        )
        self.assertIsInstance(
            old_cde_hook_exception,
            CdeHookException,
        )
        get_connection_mock.assert_called()

    @mock.patch.object(
        CdeHook,
        "get_connection",
        return_value=TEST_DEFAULT_CONNECTION,
    )
    def test_cde_operator_legacy_suffix_imports(
        self,
        get_connection_mock: Mock,
    ):
        """Checks CdeJobRunOperator"""
        from cloudera.airflow.providers.operators.cde_operator import (
            CdeRunJobOperator as LegacyCdeRunJobOperator,
        )

        old_cde_operator = LegacyCdeRunJobOperator(
            task_id="task",
            job_name=TEST_JOB_NAME,
            variables=TEST_VARIABLES,
            overrides=TEST_OVERRIDES,
            timeout=TEST_TIMEOUT,
            job_poll_interval=TEST_JOB_POLL_INTERVAL,
        )
        self.assertIsInstance(
            old_cde_operator,
            CdeRunJobOperator,
        )

        get_connection_mock.assert_called()
