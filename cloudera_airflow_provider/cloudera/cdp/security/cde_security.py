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

"""Handles CDE authentication"""
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Optional
from urllib.parse import urlparse

import requests
from airflow.utils.log.logging_mixin import LoggingMixin  # type: ignore
from cloudera.cdp.model.cde import VirtualCluster
from cloudera.cdp.security import TokenResponse
from cloudera.cdp.security.cdp_security import CDPAuth, CDPSecurityError
from cloudera.cdp.security.token_cache import (CacheableTokenAuth,
                                               GetAuthTokenError,
                                               TokenCacheStrategy, Cache)
from cloudera.cdp.security import submit_request
LOG = LoggingMixin().log

class BearerAuth(requests.auth.AuthBase):
    """
    Helper class for defining the Bearer token bases authentication mechanism.
    """
    def __init__(self, token: str) -> None:
        self.token = token

    def __call__(self, r: requests.PreparedRequest) -> requests.PreparedRequest: # pragma: no cover
        #( since it is executed in  the requests.get call)
        r.headers["authorization"] = f"Bearer {self.token}"
        return r

class CDETokenAuthResponse(TokenResponse):
    """CDE Token Response object"""
    def __init__(self, access_token: str, expires_in: int):
        self.access_token = access_token
        self.expires_in = expires_in

    @classmethod
    def from_response(cls, response: requests.Response) -> "CDETokenAuthResponse":
        """Factory method for creating a new instance of CDETokenAuthResponse
        from a json formatted valid request response

        Args:
            response: Response obtained from the Knox endpoint

        Returns:
            New, corresponding instance of CDETokenAuthResponse
        """
        response_json = response.json()
        return cls(response_json.get('access_token'), response_json.get('expires_in'))

    def is_valid(self) -> bool:
        current_time = datetime.utcnow()
        token_time = datetime.utcfromtimestamp(self.expires_in/1000)
        # Making the token invalid earlier to avoid edge cases when current time is too close
        # from the token expiry time (it would cause that the token would become invalid by the
        # time the CDEHook would use it)
        max_valid_time = token_time - timedelta(minutes=5)
        LOG.debug("Current time is : %s. Token expires at: %s. Token must be renewed from: %s"
            , current_time, token_time, max_valid_time)
        return current_time <= max_valid_time


    def __eq__(self, other: object) -> bool:
        if not isinstance(other, CDETokenAuthResponse):
            return False
        # Following line for autocompletion
        other_response : CDETokenAuthResponse = other
        return self.access_token == other_response.access_token \
                and self.expires_in == other_response.expires_in

    def __repr__(self) -> str:
        return (f"{CDETokenAuthResponse.__name__}"
                f"{{Token: {self.access_token}, Expires In: {self.expires_in}}}")

class CDEAuth(ABC):
    """Interface for CDE Authentication"""

    @abstractmethod
    def get_cde_authentication_token(self) -> CDETokenAuthResponse:
        """Obtains a CDE access token.

        Returns:
            CDE JWT token Response

        Raises:
            GetAuthTokenError if it is not possible to retrieve the CDE token
        """
        raise NotImplementedError

class CDEAPITokenAuth(CDEAuth, CacheableTokenAuth):
    """Authentication class for obtaining CDE token from CDP API token"""

    def __init__(self, cde_vcluster: VirtualCluster,
                cdp_auth: CDPAuth,
                token_cache_strategy: TokenCacheStrategy = None,
                custom_ca_certificate_path: Optional[str] = None,
                insecure: Optional[bool] = False) -> None:
        self.cde_vcluster = cde_vcluster
        self.cdp_auth = cdp_auth
        super().__init__(token_cache_strategy)
        self.custom_ca_certificate_path = custom_ca_certificate_path
        self.insecure = insecure

    @Cache(token_response_type=CDETokenAuthResponse)
    def get_cde_authentication_token(self) -> CDETokenAuthResponse:
        return self.fetch_authentication_token()

    def fetch_authentication_token(self) ->  CDETokenAuthResponse:
        """Obtains a fresh token directly from the target system

        Returns:
            valid fresh token

        Raises:
            GetAuthTokenError if there was an issue while retrieving the token
        """
        try:
            workload_name = "DE"
            cdp_token = self.cdp_auth.generate_workload_auth_token(workload_name)
        except CDPSecurityError as err:
            LOG.error("Could not obtain CDP token %s", err)
            raise GetAuthTokenError(err) from err

        # Exchange the CDP access token for a CDE/CDW access token
        try:
            auth_endpoint = self.cde_vcluster.get_auth_endpoint()
        except ValueError as err:
            LOG.error("Could not determine authentication endpoint: %s", err)
            raise GetAuthTokenError(err) from err

        try:
            kw_args = {
                'auth' : BearerAuth(cdp_token.token),
            }

            if self.insecure:
                kw_args={**kw_args, 'verify': False}
            elif self.custom_ca_certificate_path is not None:
                kw_args={**kw_args, 'verify': self.custom_ca_certificate_path}

            response = submit_request("GET", auth_endpoint, **kw_args)
        except Exception as err:
            LOG.error("Could not execute auth request: %s", repr(err))
            raise GetAuthTokenError(err) from err

        cde_token = CDETokenAuthResponse.from_response(response)

        LOG.info("Acquired CDE token expiring at %s",
            datetime.fromtimestamp(cde_token.expires_in / 1000))

        return cde_token

    def get_cache_key(self) -> str:
        vcluster_url = urlparse(self.cde_vcluster.vcluster_endpoint)
        vcluster_host = vcluster_url.netloc
        return f"{self.cdp_auth.get_auth_identifier()}____{vcluster_host}"
