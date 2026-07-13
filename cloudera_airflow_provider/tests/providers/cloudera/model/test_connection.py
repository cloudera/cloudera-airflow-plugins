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

"""Tests related to the CDE connection"""

from __future__ import annotations

import json
import unittest

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session

# workaround for https://github.com/apache/airflow/issues/34109
# TaskInstanceNote has a foreign key `user_id` (and thus a dependency) to User, but it
# seems `airflow.models.taskinstance` gets loaded first resulting in an error. Manually
# importing User here fixes the issue.
try:
    from airflow.auth.managers.fab.models import User  # noqa pylint: disable=unused-import
except ImportError:
    pass
try:
    from airflow.providers.fab.auth_manager.models import User  # noqa pylint: disable=unused-import
except ImportError:
    pass
from cloudera.airflow.providers.model.connection import (
    AuthMode,
    CdeConnection,
)


class CdeConnectionTest(unittest.TestCase):
    """Test cases for CDE connection"""

    engine: Engine
    session: Session

    @classmethod
    def setUpClass(cls):
        cls.engine = create_engine("sqlite:///:memory:")
        session = sessionmaker(bind=cls.engine)
        cls.session = session()
        CdeConnection.metadata.bind = cls.engine

    def setUp(self):
        CdeConnection.metadata.create_all(self.engine)

    def tearDown(self):
        CdeConnection.metadata.drop_all(self.engine)

    def test_save_region_no_extra(self):
        """Test saving region and making sure that the extras are populated correctly"""
        cde_connection = self.create_test_connection()
        self.session.add(cde_connection)
        self.session.commit()
        cde_connection = CdeConnection.from_airflow_connection(cde_connection)
        cde_connection.save_region("eu-1", session=self.session)

        connection = self.session.query(CdeConnection).filter_by(conn_id=cde_connection.conn_id).one_or_none()
        self.assertTrue(connection is not None)
        self.assertTrue(connection.extra is not None)
        self.assertEqual(json.loads(connection.extra), {"region": "eu-1"})

    def test_save_region_with_extra(self):
        """Test saving region and making sure that the extras are not corrupted by a modification"""
        cde_connection = self.create_test_connection()
        cde_connection.set_extra(json.dumps({"foo": "bar"}))
        self.session.add(cde_connection)
        self.session.commit()
        cde_connection.save_region("ap-1", session=self.session)

        connection = self.session.query(CdeConnection).filter_by(conn_id=cde_connection.conn_id).one_or_none()
        self.assertTrue(connection is not None)
        self.assertTrue(connection.extra is not None)
        self.assertEqual(json.loads(connection.extra), {"region": "ap-1", "foo": "bar"})

    def test_default_auth_mode_is_cdp(self):
        """Test that default auth_mode is CDP when extra is absent"""
        cde_connection = self.create_test_connection()
        self.session.add(cde_connection)
        self.session.commit()
        cde_connection = CdeConnection.from_airflow_connection(cde_connection)

        self.assertEqual(cde_connection.auth_mode, AuthMode.CDP)
        self.assertFalse(cde_connection.is_awc_auth())

    def test_awc_auth_mode_parsed(self):
        """Test that AWC auth_mode and awc_console_url are parsed from connection extra"""
        cde_connection = self.create_test_connection()
        cde_connection.set_extra(
            json.dumps(
                {
                    "auth_mode": "awc",
                    "awc_console_url": "https://console.example.com",
                }
            )
        )
        self.session.add(cde_connection)
        self.session.commit()
        cde_connection = CdeConnection.from_airflow_connection(cde_connection)

        self.assertEqual(cde_connection.auth_mode, AuthMode.AWC)
        self.assertTrue(cde_connection.is_awc_auth())
        self.assertEqual(cde_connection.awc_console_url, "https://console.example.com")

    def test_explicit_cdp_auth_mode_parsed(self):
        """Test that auth_mode cdp is parsed explicitly from connection extra"""
        cde_connection = self.create_test_connection()
        cde_connection.set_extra(json.dumps({"auth_mode": "cdp"}))
        self.session.add(cde_connection)
        self.session.commit()
        cde_connection = CdeConnection.from_airflow_connection(cde_connection)

        self.assertEqual(cde_connection.auth_mode, AuthMode.CDP)
        self.assertFalse(cde_connection.is_awc_auth())

    def test_invalid_auth_mode_raises(self):
        """Test that unknown auth_mode values raise ValueError during connection parsing"""
        cde_connection = self.create_test_connection()
        cde_connection.set_extra(json.dumps({"auth_mode": "invalid"}))
        self.session.add(cde_connection)
        self.session.commit()

        with self.assertRaises(ValueError):
            CdeConnection.from_airflow_connection(cde_connection)

    @classmethod
    def create_test_connection(cls):
        """Create a test connection"""
        return CdeConnection(
            connection_id="id",
            host="4spn57c6.cde-lvj5dccb.dex-dev.xcu2-8y8x.dev.cldr.work",
            api_base_route="/dex/api/v1",
            scheme="https",
            access_key="access_key",
            private_key="private_key",
        )


if __name__ == "__main__":
    unittest.main()
