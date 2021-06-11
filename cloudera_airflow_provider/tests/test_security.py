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

"""Tests related to the API Token Authentication feature"""

import logging
import os
from datetime import datetime, timedelta
from json import JSONDecodeError, dump, dumps
from unittest import TestCase, main
from unittest.mock import Mock, patch

import requests
from cryptography.fernet import Fernet
from tenacity import wait_none
from airflow.utils.log.logging_mixin import LoggingMixin
from tests.utils import _get_call_arguments, _make_response, iter_len_plus_one
from cloudera.cdp.model.cde import VirtualCluster
from cloudera.cdp.security import ClientError, ServerError, submit_request
from cloudera.cdp.security.cde_security import (CDEAPITokenAuth,
                                                CDETokenAuthResponse,
                                                GetAuthTokenError)
from cloudera.cdp.security.cdp_requests.cdpcurl import make_request
from cloudera.cdp.security.cdp_security import (CDPAccessKeyCredentials,
                                                CDPAccessKeyV2TokenAuth,
                                                CDPAPIAError, CDPSecurityError,
                                                CDPTokenAuthResponse,
                                                GetCRNError)
from cloudera.cdp.security.token_cache import (CacheError,
                                               EncryptedFileTokenCacheStrategy,
                                               TokenCacheStrategy)

# Speed up tests when retry mechanism is used in failing requests
submit_request.retry.wait = wait_none()

LOG = LoggingMixin().log
LOG.setLevel(logging.DEBUG)

TEST_SERVICE_ID = "cluster-5f95z6zc"
TEST_AK = "access_key"
TEST_PK = "private_key_xxxxx_xxxxx_xxxxx_xx"
TEST_ENC_KEY = Fernet(TokenCacheStrategy.get_fernet_encryption_key(TEST_PK))
CDP_AUTH_AKV2_TEST: CDPAccessKeyV2TokenAuth = CDPAccessKeyV2TokenAuth(TEST_SERVICE_ID,
            CDPAccessKeyCredentials(TEST_AK,TEST_PK))
TEST_VC_HOST = "k7s2ktbd.cde-5f95z6zc.dex-dev.xcu2-8y8x.dev.cldr.work"
TEST_VC = VirtualCluster(f"https://{TEST_VC_HOST}/dex")
TEST_CACHE_KEY = f"{TEST_AK}____{TEST_VC_HOST}"
TEST_CACHE_KEY_PATH=f"{EncryptedFileTokenCacheStrategy.CACHE_SUB_DIR}/{TEST_CACHE_KEY}"
TEST_CDE_AUTH_CACHE_STRATEGY = EncryptedFileTokenCacheStrategy(
    CDETokenAuthResponse, encryption_key=TEST_PK, cache_dir=".")
CDE_AUTH_AKV2_TEST = CDEAPITokenAuth(TEST_VC,CDP_AUTH_AKV2_TEST, TEST_CDE_AUTH_CACHE_STRATEGY)
VALID_CDE_TOKEN = "my_cde_token"
# needs to multply by 1000 to simulate more precision on the epoch time
# since the CDE API gives back in this format
VALID_CDE_TOKEN_RESPONSE_BODY = {"access_token": VALID_CDE_TOKEN,
    "expires_in": (datetime.now() + timedelta(hours=1)).timestamp() * 1000}
VALID_CDE_TOKEN_AUTH_REQUEST_RESPONSE = _make_response(200, VALID_CDE_TOKEN_RESPONSE_BODY, "")
VALID_CDE_TOKEN_AUTH_RESPONSE = CDETokenAuthResponse.from_response(
    VALID_CDE_TOKEN_AUTH_REQUEST_RESPONSE)
VALID_CDP_TOKEN = "my_cdp_token"
VALID_CDP_TOKEN_AUTH_REQUEST_RESPONSE = _make_response(200,
    {"token": VALID_CDP_TOKEN, "expiresAt": 2345 }, "")
VALID_CDP_TOKEN_AUTH_RESPONSE = CDPTokenAuthResponse(VALID_CDP_TOKEN_AUTH_REQUEST_RESPONSE)

class CDPRequestsTestCase(TestCase):
    """Tests Related to the requests issues to the CDP API"""

    @patch('cloudera.cdp.security.cdp_requests.cdpcurl.make_signature_header',
        return_value="signature")
    @patch('cloudera.cdp.security.requests.request',
            side_effect=[requests.exceptions.Timeout,
                        _make_response(500, None, "Internal Server Error"),
                        _make_response(401, None, "Unauthorized"),
                        ])
    def test_make_request_with_client_issue(self, make_request_mock: Mock, make_sig_mock: Mock):
        """Checks only ClientError are raised in case of client side (4xx) issues."""
        with self.assertRaises(ClientError):
            headers = {'Content-Type': 'application/json'}
            request_body = ""
            make_request( "POST",
                            "altus_iam_gen_workload_auth_endpoint",
                            headers,
                            request_body,
                            TEST_AK,
                            TEST_PK,
                            False,
                            True)
        make_request_mock.assert_called()
        make_sig_mock.assert_called()

    @patch('cloudera.cdp.security.cdp_requests.cdpcurl.make_signature_header',
        return_value="signature")
    @patch('cloudera.cdp.security.requests.request',
            side_effect=[_make_response(500, None, "Internal Server Error"),
                        requests.exceptions.Timeout,
                        _make_response(500, None, "Internal Server Error"),
                        ])
    def test_make_request_with_server_issue(self, make_request_mock: Mock, make_sig_mock: Mock):
        """Checks that ServerError is raised when the request reaches the max retry count
        and a 5xx error is returned"""
        with self.assertRaises(ServerError):
            headers = {'Content-Type': 'application/json'}
            request_body = ""
            make_request( "POST",
                            "altus_iam_gen_workload_auth_endpoint",
                            headers,
                            request_body,
                            TEST_AK,
                            TEST_PK,
                            False,
                            True)
        make_request_mock.assert_called()
        make_sig_mock.assert_called()

    @patch('cloudera.cdp.security.cdp_requests.cdpcurl.make_signature_header',
        return_value="signature")
    @patch('cloudera.cdp.security.requests.request',
            side_effect=[_make_response(500, None, "Internal Server Error"),
                        requests.exceptions.Timeout,
                        requests.exceptions.Timeout,
                        ])
    def test_make_request_with_request_issue(self, make_request_mock: Mock, make_sig_mock: Mock):
        """Checks that ServerError is raised when the request reaches the max retry count
        and a Timeout error is returned"""
        with self.assertRaises(requests.exceptions.Timeout):
            headers = {'Content-Type': 'application/json'}
            request_body = ""
            make_request( "POST",
                            "altus_iam_gen_workload_auth_endpoint",
                            headers,
                            request_body,
                            TEST_AK,
                            TEST_PK,
                            False,
                            True)
        make_request_mock.assert_called()
        make_sig_mock.assert_called()

class CDPAUthTokenV2TestCase(TestCase):
    """Tests related to CDP auth token v2 acquisition"""
    def test_get_auth_identifier(self):
        """Identifier must be the access key"""
        self.assertEqual(CDP_AUTH_AKV2_TEST.get_auth_identifier(), TEST_AK)

    def test_get_auth_secret(self):
        """Secret must be the private key"""
        self.assertEqual(CDP_AUTH_AKV2_TEST.get_auth_secret(), TEST_PK)

    @patch('cloudera.cdp.security.cdp_security.make_request',
        return_value=_make_response(200, {"service": {"environmentCrn" : "my_env_crn"} }, ""))
    def test_get_env_crn(self, make_request_mock: Mock):
        """Correct environment shall be returned"""
        env_crn = CDP_AUTH_AKV2_TEST.get_env_crn()
        self.assertEqual("my_env_crn", env_crn)
        make_request_mock.assert_called()


    @patch('cloudera.cdp.security.cdp_security.make_request',
        side_effect=[requests.exceptions.Timeout,
                        ServerError,
                        ClientError])
    def test_get_env_crn_with_issue_in_request(self, make_request_mock: Mock):
        """Check error handling of various issues when trying to get CRN"""
        for _ in range(iter_len_plus_one(make_request_mock.side_effect)):
            with self.assertRaises(GetCRNError):
                CDP_AUTH_AKV2_TEST.get_env_crn()
            make_request_mock.assert_called()

    @patch.object(CDPAccessKeyV2TokenAuth, 'get_env_crn', return_value="my_env_crn")
    @patch('cloudera.cdp.security.cdp_security.make_request',
        return_value=VALID_CDP_TOKEN_AUTH_REQUEST_RESPONSE)
    def test_generate_workload_auth_token(self, make_request_mock: Mock, env_crn_mock: Mock):
        """Check that token is obtained in case of valid API response"""
        cdp_token = CDP_AUTH_AKV2_TEST.generate_workload_auth_token("DE")
        self.assertEqual("my_cdp_token", cdp_token.token)
        env_crn_mock.assert_called()
        self.assertEqual(make_request_mock.call_count, 1)

    @patch.object(CDPAccessKeyV2TokenAuth, 'get_env_crn', return_value="my_env_crn")
    @patch('cloudera.cdp.security.cdp_security.make_request',
        side_effect=[requests.exceptions.RequestException,
                        ServerError,
                        ClientError])
    def test_generate_workload_auth_token_with_issue_in_request(self,
                                        make_request_mock: Mock, env_crn_mock: Mock):
        """Check error handling of various issues when trying to get CDP Auth token"""
        for i in range(1, iter_len_plus_one(make_request_mock.side_effect),1):
            with self.assertRaises(CDPSecurityError) as err:
                CDP_AUTH_AKV2_TEST.generate_workload_auth_token("DE")
                self.assertEqual(env_crn_mock.call_count, 5)
            self.assertEqual(env_crn_mock.call_count, i)
            self.assertEqual(make_request_mock.call_count, i)
            self.assertEqual(type(err.exception), CDPAPIAError)


class CDETestCase(TestCase):
    """Tests for CDE Model related objects"""
    def test_get_cache_key(self):
        """Check that cache key for a VC is <Access Key>____<VC endpoint>"""
        self.assertEqual(CDE_AUTH_AKV2_TEST.get_cache_key(), TEST_CACHE_KEY)

    def test_get_service_id_from_valid_url(self):
        """Check that service id can be properly extracted from a valid vc endpoint"""
        valid_url="https://k7s2ktbd.cde-5f95z6zc.dex-dev.xcu2-8y8x.dev.cldr.work/dex/api/v1"
        self.assertEqual("cluster-5f95z6zc", VirtualCluster(valid_url).get_service_id())

    def test_get_cluster_id_from_invalid_url(self):
        """Check error handling when trying to extract service id from invalid VC endpoints"""
        invalid_urls= [
            "",
            "invalid_url"
        ]
        for url in invalid_urls:
            with self.subTest(url):
                with self.assertRaises(ValueError):
                    VirtualCluster(url).get_service_id()

    def test_get_auth_endpoint(self):
        """Check that the auth endpoint can be obtained from the VC Endpoint"""
        valid_endpoint = "https://k7s2ktbd.cde-5f95z6zc.dex-dev.xcu2-8y8x.dev.cldr.work/dex/api/v1"
        expected = "https://service.cde-5f95z6zc.dex-dev.xcu2-8y8x.dev.cldr.work"\
            f"{VirtualCluster.ACCESS_KEY_AUTH_ENDPOINT_PATH}"
        self.assertEqual(expected, VirtualCluster(valid_endpoint).get_auth_endpoint())

    def test_get_auth_endpoint_invalid_inputs(self):
        """Check error handling when trying to obtain auth endpoint from invalid VC endpoints"""
        invalid_endpoints = [
            "",
            "invalid_url",
            "http://xn-?-vbb/",
            "http://k7s2ktbd.cde-5f95z6zc.dex-dev.xcu2-8y8x.dev.cldr.work/dex/api/v1",
            "https://service.cde-5f95z6zc.dex-dev.xcu2-8y8x.dev.cldr.work/dex/api/v1"
        ]
        for url in invalid_endpoints:
            with self.subTest(url):
                with self.assertRaises(ValueError):
                    VirtualCluster(url).get_auth_endpoint()

class CDEAuthTestCase(TestCase):
    """Test cases related to CDE Auth based on CDP Auth TokenV2"""
    @patch.object(CDPAccessKeyV2TokenAuth, 'generate_workload_auth_token',
        return_value=VALID_CDP_TOKEN_AUTH_RESPONSE)
    @patch.object(requests, 'request',
        return_value=VALID_CDE_TOKEN_AUTH_REQUEST_RESPONSE)
    def test_fetch_auth_token(self, get_mock, cdp_mock):
        """Token can be acquired successuffully on regular cases, with valid responses"""
        cde_token = CDE_AUTH_AKV2_TEST.fetch_authentication_token()
        self.assertEqual(cde_token.access_token, VALID_CDE_TOKEN)
        get_mock.assert_called()
        cdp_mock.assert_called()

    @patch.object(CDPAccessKeyV2TokenAuth, 'generate_workload_auth_token',
        return_value=VALID_CDP_TOKEN_AUTH_RESPONSE)
    @patch.object(requests, 'request',
        return_value=VALID_CDE_TOKEN_AUTH_REQUEST_RESPONSE)
    def test_fetch_auth_token_insecure(self, get_mock: Mock, cdp_mock):
        """Insecure mode (no certs check) for request is taken into account"""
        cde_auth_akv2_test_insecure = CDEAPITokenAuth(
            TEST_VC,CDP_AUTH_AKV2_TEST, TEST_CDE_AUTH_CACHE_STRATEGY, insecure=True)
        cde_token = cde_auth_akv2_test_insecure.fetch_authentication_token()
        self.assertEqual(cde_token.access_token, VALID_CDE_TOKEN)
        cdp_mock.assert_called()
        get_mock.assert_called()
        called_args = _get_call_arguments(get_mock.call_args)
        self.assertEqual(called_args['verify'], False)

    @patch.object(CDPAccessKeyV2TokenAuth, 'generate_workload_auth_token',
        return_value=VALID_CDP_TOKEN_AUTH_RESPONSE)
    @patch.object(requests, 'request',
        return_value=VALID_CDE_TOKEN_AUTH_REQUEST_RESPONSE)
    def test_fetch_auth_token_with_custom_ca(self, get_mock, cdp_mock):
        """Check that custom ca is used when specified"""
        cde_auth_akv2_test_ca = CDEAPITokenAuth(TEST_VC,CDP_AUTH_AKV2_TEST,
                                TEST_CDE_AUTH_CACHE_STRATEGY, "ca")
        cde_token = cde_auth_akv2_test_ca.fetch_authentication_token()
        self.assertEqual(cde_token.access_token, VALID_CDE_TOKEN)
        get_mock.assert_called()
        cdp_mock.assert_called()
        called_args = _get_call_arguments(get_mock.call_args)
        self.assertEqual(called_args['verify'], "ca")

    @patch.object(CDPAccessKeyV2TokenAuth, 'generate_workload_auth_token',
        return_value=VALID_CDP_TOKEN_AUTH_RESPONSE)
    def test_fetch_auth_token_issue_in_request(self, cdp_mock):
        """Check error handling when request to knox fails"""
        with self.assertRaises(GetAuthTokenError) as err:
            CDE_AUTH_AKV2_TEST.fetch_authentication_token()
        cdp_mock.assert_called()
        self.assertIsInstance(err.exception.raised_from, requests.RequestException)

    @patch.object(CDPAccessKeyV2TokenAuth, 'generate_workload_auth_token',
        return_value=VALID_CDP_TOKEN_AUTH_RESPONSE)
    @patch.object(requests, 'request',
        return_value=_make_response(401, {}, "Unauthorized"))
    def test_fetch_auth_token_unauthorized(self, get_mock: Mock, cdp_mock: Mock):
        """Check error handling when request to knox is not authorized"""
        with self.assertRaises(GetAuthTokenError) as err:
            CDE_AUTH_AKV2_TEST.fetch_authentication_token()
        get_mock.assert_called()
        self.assertIsInstance(err.exception.raised_from, ClientError)
        cdp_mock.assert_called()

    @patch.object(CDPAccessKeyV2TokenAuth, 'generate_workload_auth_token',
        return_value=VALID_CDP_TOKEN_AUTH_RESPONSE)
    def test_fetch_auth_token_bad_vc_endpoint(self, cdp_mock):
        """Check error handling when VC endpoint is wrong"""
        cde_auth_akv2_test_bad_vc = CDEAPITokenAuth(VirtualCluster("bad"),
            CDP_AUTH_AKV2_TEST, TEST_CDE_AUTH_CACHE_STRATEGY)
        with self.assertRaises(GetAuthTokenError) as err:
            cde_auth_akv2_test_bad_vc.fetch_authentication_token()
        cdp_mock.assert_called()
        self.assertIsInstance(err.exception.raised_from, ValueError)

    def test_fetch_auth_token_with_cdp_issue(self):
        """Check error handling when CDP related operations fail"""
        with self.assertRaises(GetAuthTokenError) as err:
            CDE_AUTH_AKV2_TEST.fetch_authentication_token()
        self.assertIsInstance(err.exception.raised_from, GetCRNError)

    @patch.object(CDPAccessKeyV2TokenAuth, 'generate_workload_auth_token',
        return_value=VALID_CDP_TOKEN_AUTH_RESPONSE)
    @patch.object(requests, 'request',
        return_value=VALID_CDE_TOKEN_AUTH_REQUEST_RESPONSE)
    def test_regular_auth_when_no_or_invalid_cache(self, get_mock, cdp_mock):
        """Test when no cache"""
        cache_key = CDE_AUTH_AKV2_TEST.get_cache_key()
        CDE_AUTH_AKV2_TEST.token_cache_strategy.clear_cached_auth_token(cache_key)
        cde_token = CDE_AUTH_AKV2_TEST.get_cde_authentication_token()

        self.assertEqual(cde_token.access_token, VALID_CDE_TOKEN)
        get_mock.assert_called()
        cdp_mock.assert_called()

    @patch.object(CDPAccessKeyV2TokenAuth, 'generate_workload_auth_token',
        return_value=VALID_CDP_TOKEN_AUTH_RESPONSE)
    def test_valid_cache_auth(self, cdp_mock):
        """When cache already exists and is valid"""
        token_cache_path = EncryptedFileTokenCacheStrategy.CACHE_SUB_DIR + \
                "/" + CDE_AUTH_AKV2_TEST.get_cache_key()
        with open(token_cache_path, 'w') as cache_file:
            cache_file.write(TEST_ENC_KEY.encrypt(
                dumps(VALID_CDE_TOKEN_RESPONSE_BODY).encode('utf-8')).decode('utf-8'))
        cde_token = CDE_AUTH_AKV2_TEST.get_cde_authentication_token()

        self.assertEqual(cde_token.access_token, VALID_CDE_TOKEN)
        self.assertEqual(cdp_mock.call_count, 0)

        os.remove(token_cache_path)


class TokenCacheTestCase(TestCase):
    """Test related to generic operations for tokens / caches"""
    def test_filetoken_cache_no_dir(self):
        """Error handling when invalid cache dirs are provided"""
        with self.assertRaises(CacheError):
            EncryptedFileTokenCacheStrategy(CDETokenAuthResponse, TEST_PK, cache_dir=None)

        with self.assertRaises(CacheError):
            EncryptedFileTokenCacheStrategy(CDETokenAuthResponse, TEST_PK, cache_dir=" ")

    def test_get_cache(self):
        """Cache can be retrieved properly and ensure that decrypting the content works
        as expected"""
        with open(TEST_CACHE_KEY_PATH, 'w') as cache_file:
            content = dumps(VALID_CDE_TOKEN_RESPONSE_BODY).encode('utf-8')
            dump(TEST_ENC_KEY.encrypt(content).decode('utf-8'), cache_file)

        expected_token = VALID_CDE_TOKEN_AUTH_RESPONSE
        actual_token = TEST_CDE_AUTH_CACHE_STRATEGY.get_cached_auth_token(TEST_CACHE_KEY)
        self.assertEqual(expected_token, actual_token)
        self.assertNotEqual(VALID_CDE_TOKEN_RESPONSE_BODY, actual_token)

        os.remove(TEST_CACHE_KEY_PATH)


    def test_cache_token(self):
        """Test that token can be cached and written properly, in an encrpyted manner"""
        cde_token = VALID_CDE_TOKEN_AUTH_RESPONSE
        ftcs = TEST_CDE_AUTH_CACHE_STRATEGY
        ftcs.cache_auth_token(TEST_CACHE_KEY, cde_token)
        actual_token = ftcs.get_cached_auth_token(TEST_CACHE_KEY)
        self.assertEqual(cde_token, actual_token)

        os.remove(TEST_CACHE_KEY_PATH)

    def test_cannot_get_cache(self):
        """Error handling in various situations when trying to obtain cache"""
        with open(TEST_CACHE_KEY_PATH, 'w') as cache_file:
            cache_file.write('wrong')
        with self.assertRaises(CacheError) as err:
            TEST_CDE_AUTH_CACHE_STRATEGY.get_cached_auth_token(TEST_CACHE_KEY)
            self.assertIsInstance(err.exception.raised_from, JSONDecodeError)

        with open(TEST_CACHE_KEY_PATH, 'w') as cache_file:
            cache_file.write('{"xyz": "my_cde_token", "expires_in": 123}')
        with self.assertRaises(CacheError):
            TEST_CDE_AUTH_CACHE_STRATEGY.get_cached_auth_token(TEST_CACHE_KEY)
            self.assertIsInstance(err.exception.raised_from, TypeError)

        os.remove(TEST_CACHE_KEY_PATH)
        with self.assertRaises(CacheError) as err:
            TEST_CDE_AUTH_CACHE_STRATEGY.get_cached_auth_token(TEST_CACHE_KEY)
            self.assertIsInstance(err.exception.raised_from, FileNotFoundError)

    def test_cannot_cache_token(self):
        """Error handling in various situations when trying to write cache"""
        cde_token = VALID_CDE_TOKEN_AUTH_RESPONSE
        with open(TEST_CACHE_KEY_PATH, 'w') as cache_file:
            cache_file.write('wrong')
        os.chmod(TEST_CACHE_KEY_PATH, 0o400)
        ftcs = TEST_CDE_AUTH_CACHE_STRATEGY
        with self.assertRaises(CacheError) as err:
            ftcs.cache_auth_token(TEST_CACHE_KEY, cde_token)
        self.assertIsInstance(err.exception.raised_from, PermissionError)

        os.remove(TEST_CACHE_KEY_PATH)

if __name__ == '__main__':
    main()
