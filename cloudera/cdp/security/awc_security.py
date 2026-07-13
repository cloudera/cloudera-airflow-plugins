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

"""AWC OAuth client-credentials authentication for external CDE API access."""
from __future__ import annotations

import hashlib
import logging
from datetime import datetime
from typing import NamedTuple
from urllib.parse import urlparse

import requests

from cloudera.cdp.security import ClientError, SecurityError, submit_request
from cloudera.cdp.security.cde_security import CdeAuth, CdeTokenAuthResponse
from cloudera.cdp.security.token_cache import (
    Cache,
    CacheableTokenAuth,
    FetchAuthTokenError,
    TokenCacheStrategy,
)

LOG = logging.getLogger(__name__)


class AwcSecurityError(SecurityError):
    """Root exception for AWC authentication issues"""


class MissingAwcConsoleUrlError(AwcSecurityError):
    """Exception used when AWC console URL is missing and cannot be determined"""


class AwcCredentials(NamedTuple):
    """Represent OAuth client credentials for AWC platform authentication."""

    client_id: str
    client_secret: str


class AwcPlatformTokenAuth(CdeAuth, CacheableTokenAuth):
    """Fetch and cache AWC platform bearer tokens for external CDE API calls."""

    def __init__(
        self,
        console_url: str | None,
        awc_access_keys_token_route: str,
        awc_cred: AwcCredentials,
        token_cache_strategy: TokenCacheStrategy | None = None,
        insecure: bool = False,
        custom_ca_path: str | None = None,
        timeout: int = 30,
    ) -> None:
        self.console_url = self.normalize_console_url(console_url)
        self.awc_access_keys_token_route = awc_access_keys_token_route
        self.awc_cred = awc_cred
        self.timeout = timeout
        self.verify = True
        if insecure:
            self.verify = False
        elif custom_ca_path is not None:
            self.verify = custom_ca_path
        if token_cache_strategy:
            super().__init__(token_cache_strategy)
        else:
            self.token_cache_strategy = None

    @staticmethod
    def normalize_console_url(console_url: str | None) -> str:
        """Return a normalized AWC console URL or raise if missing."""
        normalized = (console_url or "").strip().rstrip("/")
        if not normalized:
            raise MissingAwcConsoleUrlError()
        return normalized

    @staticmethod
    def derive_auth_secret(client_id: str, client_secret: str, console_url: str) -> str:
        """Derive a Fernet-compatible encryption key for AWC token cache files.

        All three inputs are hashed together so that different console URLs always
        produce distinct keys even when client_id+client_secret is >=32 chars (which
        would otherwise cause console_url to be silently truncated away).
        """
        material = f"{client_id}:{client_secret}:{console_url}"
        return hashlib.sha256(material.encode()).hexdigest()[:32]

    def get_auth_identifier(self) -> str:
        """Return the OAuth client id used as the cache/auth identifier."""
        return self.awc_cred.client_id

    def get_auth_secret(self) -> str:
        """Return the derived secret used to encrypt cached AWC tokens."""
        return self.derive_auth_secret(
            self.awc_cred.client_id,
            self.awc_cred.client_secret,
            self.console_url,
        )

    @Cache(token_response_type=CdeTokenAuthResponse)
    def get_cde_authentication_token(self) -> CdeTokenAuthResponse:
        return self.fetch_authentication_token()

    def fetch_authentication_token(self) -> CdeTokenAuthResponse:
        """Obtains a fresh token directly from the AWC console."""
        response = self._submit_token_request()
        try:
            token = CdeTokenAuthResponse.from_response(response)
        except ValueError as err:
            raise FetchAuthTokenError(
                err,
                msg=f"AWC token response invalid: {err}",
            ) from err

        LOG.info(
            "Acquired AWC platform token expiring at %s",
            datetime.fromtimestamp(token.expires_in / 1000),
        )

        return token

    def get_cache_key(self) -> str:
        netloc = urlparse(self.console_url).netloc
        return f"awc____{self.get_auth_identifier()}____{netloc}"

    def _submit_token_request(self) -> requests.Response:
        token_url = f"{self.console_url}{self.awc_access_keys_token_route}"
        try:
            return submit_request(
                "POST",
                token_url,
                data={"grant_type": "client_credentials"},
                auth=(self.awc_cred.client_id, self.awc_cred.client_secret),
                verify=self.verify,
                timeout=self.timeout,
            )
        except ClientError as err:
            if "401" in str(err):
                raise FetchAuthTokenError(
                    ValueError("invalid credentials"),
                    msg="AWC token request failed: invalid credentials",
                ) from err
            LOG.error("Could not execute AWC auth request: %s", repr(err))
            raise FetchAuthTokenError(err, msg=f"AWC token request failed: {err}") from err
        except Exception as err:
            LOG.error("Could not execute AWC auth request: %s", repr(err))
            raise FetchAuthTokenError(err, msg=f"AWC token request failed: {err}") from err
