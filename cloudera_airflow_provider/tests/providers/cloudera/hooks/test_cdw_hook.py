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

from __future__ import annotations

from unittest import mock

from airflow.hooks.base_hook import BaseHook
from airflow.models import Connection
from cloudera.airflow.providers.hooks.cdw import CdwHook


@mock.patch.object(
    BaseHook,
    "get_connection",
    return_value=Connection(
        conn_id="fake",
        conn_type="hive_cli",
        host="hs2-beeline.host",
        login="user",
        password="pass",
        schema="hello",
        port=10001,
        extra=None,
        uri=None,
    ),
)
def test_beeline_command_hive(connection_mock):
    """
    Tests whether the expected beeline command is generated from CDHHook's parameters.
    """

    hook = CdwHook(cli_conn_id="anything")
    connection_mock.assert_called()
    beeline_command = hook.get_cli_cmd()
    assert (
        " ".join(beeline_command) == "beeline -u jdbc:hive2://hs2-beeline.host/hello;"
        "transportMode=http;httpPath=cliservice;ssl=true -n user -p pass "
        "--hiveconf hive.query.isolation.scan.size.threshold=0B "
        "--hiveconf hive.query.results.cache.enabled=false "
        "--hiveconf hive.auto.convert.join.noconditionaltask.size=2505397589"
    ), "invalid beeline command"


@mock.patch.object(
    BaseHook,
    "get_connection",
    return_value=Connection(
        conn_id="fake",
        conn_type="hive_cli",
        host="impala-proxy-beeline.host",
        login="user",
        password="pass",
        schema="hello",
        port=7777,
        extra=None,
        uri=None,
    ),
)
def test_beeline_command_impala(connection_mock):
    """
    Tests whether the expected beeline command is generated from CDHHook's parameters.
    CdwHook will force the following by default in case of impala:
    port: 443 (regardless of setting)
    AuthMech: should be present, default 3
    """

    hook = CdwHook(cli_conn_id="anything")
    connection_mock.assert_called()
    beeline_command = hook.get_cli_cmd()
    assert (
        " ".join(beeline_command) == "beeline -d com.cloudera.impala.jdbc41.Driver "
        "-u jdbc:impala://impala-proxy-beeline.host:443/hello;AuthMech=3;"
        "transportMode=http;httpPath=cliservice;ssl=1 -n user -p pass"
    ), "invalid beeline command"


@mock.patch.object(
    BaseHook,
    "get_connection",
    return_value=Connection(
        conn_id="fake",
        conn_type="hive_cli",
        host="impala-proxy-beeline.host",
        login="user",
        password="pass",
        schema="hello",
        port=7777,
        extra=None,
        uri=None,
    ),
)
def test_beeline_command_impala_custom_driver(connection_mock):
    """
    Tests whether the expected beeline command is generated from CDHHook's
    parameters with custom impala driver.
    """

    custom_driver = "com.impala.another.driver"
    hook = CdwHook(cli_conn_id="anything", jdbc_driver=custom_driver)
    connection_mock.assert_called()
    beeline_command = hook.get_cli_cmd()
    assert (
        " ".join(beeline_command) == "beeline -d " + custom_driver + " "
        "-u jdbc:impala://impala-proxy-beeline.host:443/hello;AuthMech=3;"
        "transportMode=http;httpPath=cliservice;ssl=1 -n user -p pass"
    ), "invalid beeline command"


@mock.patch.object(
    BaseHook,
    "get_connection",
    return_value=Connection(
        conn_id="fake",
        conn_type="hive_cli",
        host="beeline.host",
        login="user",
        password="pass",
        schema="hello",
        port=10001,
        extra=None,
        uri=None,
    ),
)
def test_beeline_command_non_isolation(connection_mock):
    """
    Tests whether the expected beeline command is generated from CDHHook's parameters without isolation.
    """

    hook = CdwHook(cli_conn_id="anything", query_isolation=False)
    connection_mock.assert_called()
    beeline_command = hook.get_cli_cmd()
    assert (
        " ".join(beeline_command) == "beeline -u jdbc:hive2://beeline.host/hello;"
        "transportMode=http;httpPath=cliservice;ssl=true -n user -p pass"
    ), "invalid beeline command"
