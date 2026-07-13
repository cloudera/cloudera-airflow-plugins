#  Cloudera Airflow Provider
#  (C) Cloudera, Inc. 2021-2026
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

"""Tests related to AWC OAuth client-credentials authentication"""

import logging
import tempfile
import time
from unittest import TestCase, main
from unittest.mock import patch

from tenacity import wait_none

from cloudera.cdp.security import ClientError, ServerError, submit_request
from cloudera.cdp.security.awc_security import AwcCredentials, AwcPlatformTokenAuth, MissingAwcConsoleUrlError
from cloudera.cdp.security.cde_security import CdeTokenAuthResponse, GetAuthTokenError
from cloudera.cdp.security.token_cache import EncryptedFileTokenCacheStrategy, FetchAuthTokenError
from tests.utils import _get_call_arguments, _make_response

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)

# Speed up tests when retry mechanism is used in failing requests
submit_request.retry.wait = wait_none()  # type: ignore

TEST_AWC_CONSOLE_URL = "https://console.example.com"
TEST_AWC_ACCESS_KEYS_TOKEN_ROUTE = "/api/v0/auth/access-keys/token"
TEST_AWC_CLIENT_ID = "client-id"
TEST_AWC_CLIENT_SECRET = "client-secret"
TEST_AWC_CRED = AwcCredentials(TEST_AWC_CLIENT_ID, TEST_AWC_CLIENT_SECRET)
TEST_AWC_TOKEN_URL = f"{TEST_AWC_CONSOLE_URL}{TEST_AWC_ACCESS_KEYS_TOKEN_ROUTE}"
TEST_AWC_CACHE_KEY = f"awc____{TEST_AWC_CLIENT_ID}____console.example.com"
VALID_AWC_TOKEN = "token-value"
TEST_AWC_TOKEN_EXPIRES_AT_MS = int((time.time() + 3600) * 1000)
VALID_AWC_TOKEN_RESPONSE_BODY = {
    "access_token": VALID_AWC_TOKEN,
    "expires_in": TEST_AWC_TOKEN_EXPIRES_AT_MS,
}
VALID_AWC_TOKEN_AUTH_REQUEST_RESPONSE = _make_response(200, VALID_AWC_TOKEN_RESPONSE_BODY, "OK")
AWC_AUTH_TEST = AwcPlatformTokenAuth(
    TEST_AWC_CONSOLE_URL,
    TEST_AWC_ACCESS_KEYS_TOKEN_ROUTE,
    TEST_AWC_CRED,
)


def _build_awc_auth_with_cache(cache_dir: str) -> AwcPlatformTokenAuth:
    """Mirror CDEHook AWC composition: credentials, cache strategy, then auth."""
    cache_strategy = EncryptedFileTokenCacheStrategy(
        CdeTokenAuthResponse,
        encryption_key=AwcPlatformTokenAuth.derive_auth_secret(
            TEST_AWC_CLIENT_ID,
            TEST_AWC_CLIENT_SECRET,
            TEST_AWC_CONSOLE_URL,
        ),
        cache_dir=cache_dir,
    )
    return AwcPlatformTokenAuth(
        TEST_AWC_CONSOLE_URL,
        TEST_AWC_ACCESS_KEYS_TOKEN_ROUTE,
        TEST_AWC_CRED,
        cache_strategy,
    )


class AwcAuthTestCase(TestCase):
    """Test cases related to AWC platform token authentication"""

    def test_missing_console_url(self):
        """AWC auth requires a console URL, validated when auth is constructed"""
        for console_url in ("", None):
            with self.subTest(console_url=console_url):
                with self.assertRaises(MissingAwcConsoleUrlError):
                    AwcPlatformTokenAuth(
                        console_url,
                        TEST_AWC_ACCESS_KEYS_TOKEN_ROUTE,
                        TEST_AWC_CRED,
                    )


    @patch('cloudera.cdp.security.awc_security.submit_request', return_value=VALID_AWC_TOKEN_AUTH_REQUEST_RESPONSE)
    def test_fetch_auth_token(self, submit_mock):
        """Token can be acquired successfully on regular cases, with valid responses"""
        awc_token = AWC_AUTH_TEST.fetch_authentication_token()
        self.assertEqual(awc_token.access_token, VALID_AWC_TOKEN)
        self.assertEqual(awc_token.expires_in, TEST_AWC_TOKEN_EXPIRES_AT_MS)
        submit_mock.assert_called_once_with(
            "POST",
            TEST_AWC_TOKEN_URL,
            data={"grant_type": "client_credentials"},
            auth=(TEST_AWC_CLIENT_ID, TEST_AWC_CLIENT_SECRET),
            verify=True,
            timeout=30,
        )

    @patch('cloudera.cdp.security.awc_security.submit_request', return_value=VALID_AWC_TOKEN_AUTH_REQUEST_RESPONSE)
    def test_fetch_auth_token_insecure(self, submit_mock):
        """Insecure mode (no certs check) for request is taken into account"""
        awc_auth_insecure = AwcPlatformTokenAuth(
            TEST_AWC_CONSOLE_URL,
            TEST_AWC_ACCESS_KEYS_TOKEN_ROUTE,
            TEST_AWC_CRED,
            insecure=True,
        )
        awc_token = awc_auth_insecure.fetch_authentication_token()
        self.assertEqual(awc_token.access_token, VALID_AWC_TOKEN)
        called_args = _get_call_arguments(submit_mock.call_args)
        self.assertEqual(called_args['verify'], False)

    @patch('cloudera.cdp.security.awc_security.submit_request', return_value=VALID_AWC_TOKEN_AUTH_REQUEST_RESPONSE)
    def test_fetch_auth_token_with_custom_ca(self, submit_mock):
        """Check that custom ca is used when specified"""
        awc_auth_ca = AwcPlatformTokenAuth(
            TEST_AWC_CONSOLE_URL,
            TEST_AWC_ACCESS_KEYS_TOKEN_ROUTE,
            TEST_AWC_CRED,
            custom_ca_path="/tmp/ca.pem",
        )
        awc_token = awc_auth_ca.fetch_authentication_token()
        self.assertEqual(awc_token.access_token, VALID_AWC_TOKEN)
        called_args = _get_call_arguments(submit_mock.call_args)
        self.assertEqual(called_args['verify'], "/tmp/ca.pem")

    @patch('cloudera.cdp.security.awc_security.submit_request')
    def test_fetch_auth_token_unauthorized(self, submit_mock):
        """Check error handling when token request is not authorized"""
        submit_mock.side_effect = ClientError("401:Unauthorized")
        with self.assertRaises(FetchAuthTokenError):
            AWC_AUTH_TEST.fetch_authentication_token()

    @patch('cloudera.cdp.security.awc_security.submit_request')
    def test_get_auth_token_unauthorized_raises_get_auth_token_error(self, submit_mock):
        """FetchAuthTokenError from the token request is wrapped as GetAuthTokenError by @Cache"""
        submit_mock.side_effect = ClientError("401:Unauthorized")
        with self.assertRaises(GetAuthTokenError) as err:
            AWC_AUTH_TEST.get_cde_authentication_token()
        self.assertIsInstance(err.exception.raised_from, FetchAuthTokenError)

    @patch('cloudera.cdp.security.awc_security.submit_request', return_value=VALID_AWC_TOKEN_AUTH_REQUEST_RESPONSE)
    def test_get_auth_token_returns_token(self, submit_mock):
        """Token can be acquired through the cache decorator"""
        with tempfile.TemporaryDirectory() as cache_dir:
            awc_auth = _build_awc_auth_with_cache(cache_dir)
            awc_token = awc_auth.get_cde_authentication_token()

        self.assertIsInstance(awc_token, CdeTokenAuthResponse)
        self.assertEqual(awc_token.access_token, VALID_AWC_TOKEN)
        submit_mock.assert_called()

    @patch('cloudera.cdp.security.awc_security.submit_request', return_value=VALID_AWC_TOKEN_AUTH_REQUEST_RESPONSE)
    def test_get_auth_token_without_cache_dir(self, submit_mock):
        """Token can be acquired when no cache strategy is configured"""
        awc_token = AWC_AUTH_TEST.get_cde_authentication_token()
        self.assertEqual(awc_token.access_token, VALID_AWC_TOKEN)
        submit_mock.assert_called()

    def test_auth_secret_length(self):
        """Derived cache encryption key must be Fernet-compatible length"""
        secret = AwcPlatformTokenAuth.derive_auth_secret("id", "secret", TEST_AWC_CONSOLE_URL)
        self.assertEqual(len(secret), 32)

    def test_auth_secret_differs_by_console_url(self):
        """Different console URLs must produce different keys even when id+secret >= 32 chars"""
        long_id = "a" * 20
        long_secret = "b" * 20
        key1 = AwcPlatformTokenAuth.derive_auth_secret(
            long_id, long_secret, "https://console-a.example.com"
        )
        key2 = AwcPlatformTokenAuth.derive_auth_secret(
            long_id, long_secret, "https://console-b.example.com"
        )
        self.assertNotEqual(key1, key2)

    def test_cache_key(self):
        """Check that cache key encodes console host and client id"""
        self.assertEqual(AWC_AUTH_TEST.get_cache_key(), TEST_AWC_CACHE_KEY)

    def test_credentials_api(self):
        """Check auth identifier and derived cache secret accessors"""
        self.assertEqual(AWC_AUTH_TEST.get_auth_identifier(), TEST_AWC_CLIENT_ID)
        self.assertEqual(
            AWC_AUTH_TEST.get_auth_secret(),
            AwcPlatformTokenAuth.derive_auth_secret(
                TEST_AWC_CLIENT_ID,
                TEST_AWC_CLIENT_SECRET,
                TEST_AWC_CONSOLE_URL,
            ),
        )

    @patch('cloudera.cdp.security.awc_security.submit_request', return_value=VALID_AWC_TOKEN_AUTH_REQUEST_RESPONSE)
    def test_valid_cache_auth(self, submit_mock):
        """When cache already exists and is valid, no new HTTP request is made"""
        with tempfile.TemporaryDirectory() as cache_dir:
            awc_auth = _build_awc_auth_with_cache(cache_dir)
            awc_auth.get_cde_authentication_token()
            awc_auth.get_cde_authentication_token()

        submit_mock.assert_called_once()

    @patch('cloudera.cdp.security.awc_security.submit_request')
    def test_get_auth_token_raises_get_auth_token_error_on_failure(self, submit_mock):
        """GetAuthTokenError is raised when token acquisition fails through the cache decorator"""
        submit_mock.side_effect = ClientError("401:Unauthorized")
        with self.assertRaises(GetAuthTokenError):
            AWC_AUTH_TEST.get_cde_authentication_token()


if __name__ == '__main__':
    main()
