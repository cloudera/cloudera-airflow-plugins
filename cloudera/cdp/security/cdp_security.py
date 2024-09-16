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

"""Handles CDP authentication"""

import json
import enum
import re
from abc import ABC, abstractmethod
from typing import NamedTuple, Optional

import logging
import requests

from cloudera.cdp.security import SecurityError, TokenResponse
from cloudera.cdp.security.cdp_requests.cdpcurl import make_request

LOG = logging.getLogger(__name__)


class CdpSecurityError(SecurityError):
    """Root exception for CDP authentication issues"""


class GetCrnError(CdpSecurityError):
    """Exception used when there is an issue while retrieving the environment CRN"""


class InferRegionError(CdpSecurityError):
    """Exception used when there is an issue while inferring the CDE CP region"""


class MissingCDPEndpointError(CdpSecurityError):
    """Exception used when CDP endpoint is missing and cannot be determined"""


class CdpApiAError(CdpSecurityError):
    """Exception used when there is an issue while interacting with CDP API"""


class GetWorkloadAuthTokenError(CdpSecurityError):
    """Exception used when there is an issue while retrieving the workload token"""


class FormFactor(str, enum.Enum):
    """FormFactor enum private or public"""

    PRIVATE_CLOUD = 'private'
    PUBLIC_CLOUD = 'public'


def get_form_factor(endpoint_url: str) -> FormFactor:
    """
    # Determines the form factor based on the CDP endpoint URL

    :param endpoint_url: str The CDP endpoint URL to use to determine the form factor.
    """

    if re.match('(.+(altus|cdp|dev|int|stage).(cloudera|clouderagovt).com)', endpoint_url):
        return FormFactor.PUBLIC_CLOUD
    else:
        return FormFactor.PRIVATE_CLOUD


class CdpTokenAuthResponse(TokenResponse):
    """CDP Token Response object"""

    def __init__(self, response: requests.Response):
        response_dict = json.loads(response.content)
        self.token = response_dict.get("token")
        self.expire_at = response_dict.get("expireAt")

    def is_valid(self) -> bool:
        raise NotImplementedError

    def __repr__(self) -> str:
        return f"{CdpTokenAuthResponse.__name__}" f"{{Token: {self.token}, Expire At: {self.expire_at}}}"


class CdpAuth(ABC):
    """Interface for CDP Authentication"""

    @abstractmethod
    def get_auth_identifier(self) -> str:
        """Gets the identifier of the connection

        Returns:
            identifier of the connection
        """
        raise NotImplementedError

    @abstractmethod
    def get_auth_secret(self) -> str:
        """Gets the secret of the connection

        Returns:
            secret of the connection
        """
        raise NotImplementedError

    @abstractmethod
    def generate_workload_auth_token(self, workload_name: str) -> CdpTokenAuthResponse:
        """Obtains a CDP access token.

        Args:
            workload_name: kind of workload for which we request the CDP token

        Returns:
            CDP JWT token Response

        Raises:
            CdpApiAError if it is not possible to retrieve the CDP token
        """
        raise NotImplementedError


class CdpAccessKeyCredentials(NamedTuple):
    """Represent access/private key pair for CDP Access Key V2 authentication"""

    access_key: str
    private_key: str


class CdpAccessKeyV2TokenAuth(CdpAuth):
    """Authentication class for obtaining CDP token from access key/private key credentials"""

    CDP_DESCRIBE_SERVICE_ROUTE = "/api/v1/de/describeService"
    GENERATE_WORKLOAD_AUTH_TOKEN_ROUTE = "/api/v1/iam/generateWorkloadAuthToken"
    CDP_DEFAULT_ENDPOINTS = {
        "us-west-1": "https://api.us-west-1.cdp.cloudera.com",
        "eu-1": "https://api.eu-1.cdp.cloudera.com",
        "ap-1": "https://api.ap-1.cdp.cloudera.com"
    }
    ALTUS_IAM_DEFAULT_ENDPOINTS = {
        "us-west-1": "https://iamapi.us-west-1.altus.cloudera.com",
        "eu-1": "https://api.eu-1.cdp.cloudera.com",
        "ap-1": "https://api.ap-1.cdp.cloudera.com"
    }
    ALTUS_IAM_GEN_WORKLOAD_AUTH_TOKEN_ROUTES = {
        "us-west-1": "/iam/generateWorkloadAuthToken",
        "eu-1": GENERATE_WORKLOAD_AUTH_TOKEN_ROUTE,
        "ap-1": GENERATE_WORKLOAD_AUTH_TOKEN_ROUTE,
    }

    def __init__(
        self,
        service_id: str,
        cdp_cred: CdpAccessKeyCredentials,
        region: Optional[str] = None,
        cdp_endpoint: Optional[str] = None,
        altus_iam_endpoint: Optional[str] = None,
        insecure: Optional[bool] = False,
        custom_ca_path: Optional[str] = None,
        form_factor: Optional[FormFactor] = None,
        env_crn: Optional[str] = None,
    ) -> None:
        self.service_id = service_id
        self.cdp_cred = cdp_cred
        self.region = region
        self.cdp_describe_service_endpoint = cdp_endpoint
        self.altus_iam_gen_workload_auth_endpoint = altus_iam_endpoint

        # FormFactor
        self.form_factor = form_factor
        if self.form_factor is None:
            if cdp_endpoint is not None:
                self.form_factor = get_form_factor(cdp_endpoint)
            else:
                self.form_factor = FormFactor.PUBLIC_CLOUD

        if self.form_factor == FormFactor.PRIVATE_CLOUD and not self.cdp_describe_service_endpoint:
            raise MissingCDPEndpointError()

        # SSL Verification
        self.verify = True
        if insecure:
            self.verify = False
        elif custom_ca_path is not None:
            self.verify = custom_ca_path

        # Environment CRN
        self.env_crn = env_crn

    def generate_workload_auth_token(self, workload_name: str) -> CdpTokenAuthResponse:
        LOG.debug("Authenticating with access key: %s", self.cdp_cred.access_key)
        LOG.debug("Using Cluster ID: %s", self.service_id)

        # Infer the region
        if not self.region and self.form_factor == FormFactor.PUBLIC_CLOUD:
            self.region = self._infer_region()

        # Configure endpoints
        if self.form_factor == FormFactor.PUBLIC_CLOUD:
            if not self.cdp_describe_service_endpoint:
                self.cdp_describe_service_endpoint = self.CDP_DEFAULT_ENDPOINTS[self.region]
            self.cdp_describe_service_endpoint += self.CDP_DESCRIBE_SERVICE_ROUTE
            if not self.altus_iam_gen_workload_auth_endpoint:
                self.altus_iam_gen_workload_auth_endpoint = self.ALTUS_IAM_DEFAULT_ENDPOINTS[self.region]
            self.altus_iam_gen_workload_auth_endpoint \
                += self.ALTUS_IAM_GEN_WORKLOAD_AUTH_TOKEN_ROUTES[self.region]
        else:
            if not self.altus_iam_gen_workload_auth_endpoint:
                self.altus_iam_gen_workload_auth_endpoint = self.cdp_describe_service_endpoint
            self.cdp_describe_service_endpoint += self.CDP_DESCRIBE_SERVICE_ROUTE
            self.altus_iam_gen_workload_auth_endpoint += self.GENERATE_WORKLOAD_AUTH_TOKEN_ROUTE

        # Get the environment-crn
        env_crn = self.env_crn or self.get_env_crn()
        LOG.debug("Using environment-crn %s", env_crn)

        # Exchange the access key for a CDP access token
        cdp_token = self._generate_workload_auth_token(env_crn, workload_name)
        LOG.debug("Exchanged access key for CDP access token")

        return cdp_token

    def get_env_crn(self) -> str:
        """
        Gets the associated environment CRN of the given cluster

        Returns:
            environment Cloudera Resource Name

        Raises:
            GetCrnError if it is not possible to retrieve the environment CRN
        """
        try:
            response = self._describe_service(self.cdp_describe_service_endpoint)
        except Exception as err:
            LOG.error("Issue while performing request to fetch environment-crn: %s", repr(err))
            raise GetCrnError(err) from err

        environment_crn = response.json().get("service").get("environmentCrn")
        return environment_crn

    def get_auth_identifier(self) -> str:
        return self.cdp_cred.access_key

    def get_auth_secret(self) -> str:
        return self.cdp_cred.private_key

    def _generate_workload_auth_token(self, env_crn: str, workload_name: str) -> CdpTokenAuthResponse:
        headers = {"Content-Type": "application/json"}
        # make_request only accepts a string for the request body
        request_body = f'{{"workloadName": "{workload_name}", "environmentCRN": "{env_crn}"}}'
        try:
            response = make_request(
                "POST",
                self.altus_iam_gen_workload_auth_endpoint,
                headers,
                request_body,
                self.cdp_cred.access_key,
                self.cdp_cred.private_key,
                False,
                self.verify,
            )
        except Exception as err:
            LOG.error("Could not exchange cdp token with access key %s", repr(err))
            raise CdpApiAError(err) from err

        cdp_token = CdpTokenAuthResponse(response)
        return cdp_token

    def _describe_service(self, cdp_describe_service_endpoint) -> str:
        headers = {"Content-Type": "application/json"}
        # make_request only accepts a string for the request body
        request_body = f'{{"clusterId": "{self.service_id}"}}'
        response = make_request(
            "POST",
            cdp_describe_service_endpoint,
            headers,
            request_body,
            self.cdp_cred.access_key,
            self.cdp_cred.private_key,
            False,
            self.verify,
        )
        return response

    def _infer_region(self) -> str:
        LOG.debug("Inferring region for Cluster ID: %s", self.service_id)
        regions = []
        errors = []
        for region, cdp_endpoint in self.CDP_DEFAULT_ENDPOINTS.items():
            cdp_describe_service_endpoint = cdp_endpoint + self.CDP_DESCRIBE_SERVICE_ROUTE
            try:
                self._describe_service(cdp_describe_service_endpoint)
                regions.append(region)
            except Exception as err:
                if "NOT_FOUND" in str(err):
                    LOG.debug("Cluster with ID %s doesn't present in region %s. Error: %s",
                              self.service_id, region, err)
                    continue

                msg = f"Issue while performing request to infer region for Cluster with ID " \
                      f"{self.service_id} in region {region}. Error: {repr(err)}"
                LOG.warning(msg)
                errors.append(repr(err))

        if len(regions) > 1:
            # Generally, this should not happen, however, there is no guarantee for access key, private key
            # and cluster ID to be unique across regions
            msg = f"Cluster with ID {self.service_id} is present in multiple regions {regions}. " \
                  f"Please specify the correct region via 'region' connection extra parameter (e.g. " \
                  f"{{\"region\": \"eu-1\"}}). Supported regions are 'us-west-1', 'eu-1', and 'ap-1'"
            raise InferRegionError(msg)
        elif len(regions) == 0:
            msg = f"Can not infer region for Cluster ID {self.service_id}. "
            if len(errors) == 0:
                msg += "Please make sure that CDP access and private keys are correct. "
            else:
                msg += f"Errors: {errors}. "
            msg += "Consider specifying the correct region via 'region' connection extra parameter (e.g. " \
                   "{{\"region\": \"eu-1\"}}). Supported regions are 'us-west-1', 'eu-1', and 'ap-1'"
            raise InferRegionError(msg)
        return regions[0]
