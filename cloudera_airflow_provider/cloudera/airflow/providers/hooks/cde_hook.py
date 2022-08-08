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
"""
Holds airflow hook functionalities for CDE clusters like submitting a CDE job,
checking its status or killing it.
"""

import os
from typing import Any, Dict, Optional, Set

import requests
import tenacity  # type: ignore

from airflow.exceptions import AirflowException  # type: ignore
from airflow.hooks.base import BaseHook  # type: ignore
from airflow.providers.http.hooks.http import HttpHook  # type: ignore

from cloudera.airflow.providers.hooks import CdpHookException
from cloudera.airflow.providers.model.connection import CdeConnection
from cloudera.cdp.model.cde import VirtualCluster
from cloudera.cdp.security import SecurityError
from cloudera.cdp.security.cde_security import BearerAuth, CdeApiTokenAuth, CdeTokenAuthResponse
from cloudera.cdp.security.cdp_security import CdpAccessKeyCredentials, CdpAccessKeyV2TokenAuth
from cloudera.cdp.security.token_cache import EncryptedFileTokenCacheStrategy


class CdeHookException(CdpHookException):
    """Root exception for the CdeHook which is used to handle any known exceptions"""


class CdeHook(BaseHook):  # type: ignore
    """A wrapper around the CDE Virtual Cluster REST API."""

    conn_name_attr = "cde_conn_id"
    conn_type = "cloudera_data_engineering"
    hook_name = "Cloudera Data Engineering"

    @staticmethod
    def get_ui_field_behaviour() -> Dict:  # pragma: no cover , since it is an Airflow-related feature
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ["schema", "port"],
            "relabeling": {
                "host": "Virtual Cluster API endpoint",
                "login": "CDP Access Key",
                "password": "CDP Private Key",
            },
        }

    DEFAULT_CONN_ID = "cde_runtime_api"
    # Gives a total of at least 2^8+2^7+...2=510 seconds of retry with exponential backoff
    DEFAULT_NUM_RETRIES = 9
    DEFAULT_API_TIMEOUT = 30

    def __init__(
        self,
        connection_id: str = DEFAULT_CONN_ID,
        num_retries: int = DEFAULT_NUM_RETRIES,
        api_timeout: int = DEFAULT_API_TIMEOUT,
    ) -> None:
        """
        Create a new CdeHook. The connection parameters are eagerly validated to highlight
        any problems early.

        :param connection_id: The connection name for the target virtual cluster API
            (default: {CdeHook.DEFAULT_CONN_ID}).
        :param num_retries: The number of times API requests should be retried if a server-side
            error or transport error is encountered (default: {CdeHook.DEFAULT_NUM_RETRIES}).
        :param api_timeout: The timeout in seconds after which, if no response has been received
            from the API, a request should be abandoned and retried
            (default: {CdeHook.DEFAULT_API_TIMEOUT}).
        """
        super().__init__(connection_id)
        self.cde_conn_id = connection_id
        airflow_connection = self.get_connection(self.cde_conn_id)
        self.connection = CdeConnection.from_airflow_connection(airflow_connection)
        self.num_retries = num_retries
        self.api_timeout = api_timeout

    def _do_api_call(
        self, method: str, endpoint: str, params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Execute the API call. Requests are retried for connection errors and server-side errors
        using an exponential backoff.

        :param method: HTTP method
        :param endpoint: URL path of REST endpoint, excluding the API prefix, e.g "/jobs/myjob/run".
            If the endpoint does not start with '/' this will be added
        :param params: A dictionary of parameters to send in either HTTP body as a JSON document
            or as URL parameters for GET requests
        :param body: A dictionary to send in the HTTP body as a JSON document
        :return: The API response converted to a Python dictionary
            or an AirflowException if the API returns an error
        """

        if self.connection.proxy:
            self.log.debug("Setting up proxy environment variables")
            os.environ["HTTPS_PROXY"] = self.connection.proxy
            os.environ["https_proxy"] = self.connection.proxy

        if self.connection.is_external():
            cde_token = self.get_cde_token()
        else:
            self.log.info("Using internal authentication mechanisms.")

        endpoint = endpoint if endpoint.startswith("/") else f"/{endpoint}"
        if self.connection.is_internal():
            endpoint = self.connection.api_base_route + endpoint

        self.log.debug(
            "Executing API call: (Method: %s, Endpoint: %s, Parameters: %s, Timeout: %s, Retries: %s)",
            method,
            endpoint,
            params,
            self.api_timeout,
            self.num_retries
        )
        http = HttpHook(method.upper(), http_conn_id=self.cde_conn_id)
        retry_handler = RetryHandler()

        try:
            extra_options: Dict[str, Any] = dict(
                timeout=self.api_timeout,
                # we check the response ourselves in RetryHandler
                check_response=False,
            )

            if self.connection.insecure:
                self.log.debug("Setting session verify to False")
                extra_options = {**extra_options, "verify": False}
            else:
                ca_cert = self.connection.ca_cert_path
                self.log.debug("ca_cert is %s", ca_cert)
                if ca_cert:
                    self.log.debug("Setting session verify to %s", ca_cert)
                    extra_options = {**extra_options, "verify": ca_cert}
                else:
                    # Ensures secure connection by default, it is False in Airflow 1
                    extra_options = {**extra_options, "verify": True}

            # Small hack to override the insecure header property passed from the
            # extra in HTTPHook, which is a boolean but must be a string to be part
            # of the headers
            request_extra_headers = {"insecure": str(self.connection.insecure)}

            common_kwargs: Dict[str, Any] = dict(
                _retry_args=dict(
                    wait=tenacity.wait_exponential(),
                    stop=tenacity.stop_after_attempt(self.num_retries),
                    retry=retry_handler,
                ),
                endpoint=endpoint,
                extra_options=extra_options,
                headers=request_extra_headers,
            )

            if self.connection.is_external():
                common_kwargs = {**common_kwargs, "auth": BearerAuth(cde_token)}

            if method.upper() == "GET":
                response = http.run_with_advanced_retry(data=params, **common_kwargs)
            else:
                response = http.run_with_advanced_retry(json=params, **common_kwargs)
            return response.json()
        except Exception as err:
            msg = "API call returned error(s)"
            msg = f"{msg}:[{','.join(retry_handler.errors)}]" if retry_handler.errors else msg
            self.log.error(msg)
            raise CdeHookException(err) from err

    def get_cde_token(self) -> str:
        """
        Obtains valid CDE token through CDP access token

        Returns:
            cde_token: a valid token for submitting request to the CDE Cluster
        """
        self.log.debug("Starting CDE token acquisition")
        access_key, private_key = (
            self.connection.access_key,
            self.connection.private_key,
        )
        vcluster_endpoint = self.connection.get_vcluster_jobs_api_url()
        try:
            cdp_cred = CdpAccessKeyCredentials(access_key, private_key)
            cde_vcluster = VirtualCluster(vcluster_endpoint)
            cdp_auth = CdpAccessKeyV2TokenAuth(
                cde_vcluster.get_service_id(),
                cdp_cred,
                cdp_endpoint=self.connection.cdp_endpoint,
                altus_iam_endpoint=self.connection.altus_iam_endpoint,
            )

            cache_mech_extra_kw = {}
            cache_dir = self.connection.cache_dir
            if cache_dir:
                cache_mech_extra_kw = {"cache_dir": cache_dir}

            cache_mech = EncryptedFileTokenCacheStrategy(
                CdeTokenAuthResponse,
                encryption_key=cdp_auth.get_auth_secret(),
                **cache_mech_extra_kw,
            )

            cde_auth = CdeApiTokenAuth(
                cde_vcluster,
                cdp_auth,
                cache_mech,
                custom_ca_certificate_path=self.connection.ca_cert_path,
                insecure=self.connection.insecure,
            )
            cde_token = cde_auth.get_cde_authentication_token().access_token
            self.log.debug("CDE token successfully acquired")

        except SecurityError as err:
            self.log.error(
                "Failed to get the cde auth token for the connection %s, error: %s",
                self.cde_conn_id,
                err,
            )
            raise CdeHookException(err) from err

        return cde_token

    def submit_job(
        self,
        job_name: str,
        variables: Optional[Dict[str, Any]] = None,
        overrides: Optional[Dict[str, Any]] = None,
        proxy_user: Optional[str] = None,
    ) -> int:
        """
        Submit a job run request

        :param job_name: The name of the job definition to run (should already be
            defined in the virtual cluster).
        :param variables: Runtime variables to pass to job run
        :param overrides: Overrides of job parameters for this run
        :return: the job run ID for a successful submission or an AirflowException
        :rtype: int
        """
        if proxy_user:
            # Not tested because it should have never been introduced in the first place
            self.log.warning("Proxy user is not yet supported. Setting it to None.")  # pragma: no cover

        body = dict(
            variables=variables,
            overrides=overrides,
            # Shall be updated to proxy_user when we support this feature
            user=None,
        )
        response = self._do_api_call("POST", f"/jobs/{job_name}/run", body)
        return response["id"]

    def kill_job_run(self, run_id: int) -> None:
        """
        Kill a running job

        :param run_id: the run ID of the job run
        """
        self._do_api_call("POST", f"/job-runs/{run_id}/kill")

    def check_job_run_status(self, run_id: int) -> str:
        """
        Check and return the status of a job run

        :param run_id: the run ID of the job run
        :return: the job run status
        :rtype: str
        """
        response = self._do_api_call("GET", f"/job-runs/{run_id}")
        response_status: str = response["status"]
        return response_status

    def get_conn(self): # pylint: disable=missing-function-docstring; not required for CdeHook
        raise NotImplementedError

    def get_pandas_df(self, sql): # pylint: disable=missing-function-docstring; not required for CdeHook
        raise NotImplementedError

    def get_records(self, sql): # pylint: disable=missing-function-docstring; not required for CdeHook
        raise NotImplementedError


class RetryHandler:
    """
    Retry strategy for tenacity that retries if a 5xx response
    or certain exceptions are encountered.
    Client error (4xx) responses are considered fatal.
    """

    ALWAYS_RETRY_EXCEPTIONS = (
        requests.exceptions.ConnectionError,
        requests.exceptions.Timeout,
    )

    def __init__(self) -> None:
        self._errors: Set[Any] = set()

    @property
    def errors(self) -> Set[Any]:
        """The set of unique API call error messages if any."""
        return self._errors

    def __call__(self, retry_state: tenacity.RetryCallState) -> bool:
        future_outcome = retry_state.outcome
        if future_outcome.failed:
            return isinstance(future_outcome.exception(), self.ALWAYS_RETRY_EXCEPTIONS)
        else:
            if isinstance(future_outcome.result(), requests.Response):
                response : requests.Response = future_outcome.result()
                status = str(response.status_code) + ":" + response.reason
                error_msg = (status + ":" + response.text.rstrip()) if response.text else status
                self._errors.add(error_msg)
                if response.status_code < 400:
                    return False
                elif response.status_code >= 500 and response.status_code < 600:
                    return True
                else:
                    raise AirflowException(error_msg)
            return False  # pragma: no cover , cannot think about a use case now.
