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

"""Security module for handling authentication to Cloudera Services"""
import logging

from abc import ABC, abstractmethod
from http import HTTPStatus
from typing import Any, Optional

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

LOG = logging.getLogger(__name__)


class SecurityError(Exception):
    """Root security exception, to be used to catch any security issue"""

    def __init__(self, raised_from: Optional[Exception] = None, msg: Optional[str] = None) -> None:
        super().__init__(raised_from, msg)
        self.raised_from = raised_from

    def __str__(self) -> str:
        return self.__repr__()


class TokenResponse(ABC):
    """Base class for token responses"""

    @abstractmethod
    def is_valid(self) -> bool:
        """Check if token is still valid

        Returns: True if token is valid, false otherwise
        """
        raise NotImplementedError


class ClientError(requests.exceptions.HTTPError):
    """When request fails because of a Client side error"""


class ServerError(requests.exceptions.HTTPError):
    """When request fails because of an Internal/Server side error"""


ALWAYS_RETRY_EXCEPTIONS = (
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    ServerError,
)


@retry(
    wait=wait_exponential(),
    stop=stop_after_attempt(3),
    retry=retry_if_exception_type(ALWAYS_RETRY_EXCEPTIONS),
    reraise=True,
)
def submit_request(method, uri, *args: Any, **kw_args) -> requests.Response:
    """
    Helper method for submitting HTTP request and handling common errors
    Args:
    method: http method, GET, POST, etc.
    uri: endpoint of the requests
    args: arguments given to the function
    kw_args: keyword arguments given to the function

    Returns:
    Response of the http request

    Raises:
    ClientError if response status code is 4xx
    ServerError if response status code is 5xx
    corresponding issued requests.exceptions.RequestException if requests throws an error and
    cannot complete successfully
    """
    try:
        LOG.debug("Issuing request: %s %s", method, uri)
        response = requests.request(method, uri, *args, **kw_args)
    except requests.exceptions.RequestException as err:
        LOG.debug("Failed to query endpoint %s %s, error: %s", method, uri, repr(err))
        raise

    if response.status_code >= HTTPStatus.BAD_REQUEST:
        status = str(response.status_code) + ":" + response.reason
        error_msg = (status + ":" + response.text.rstrip()) if response.text else status
        LOG.debug("Failed to query endpoint %s %s, error: %s", method, uri, error_msg)
        if response.status_code < HTTPStatus.INTERNAL_SERVER_ERROR:
            raise ClientError(error_msg)
        if response.status_code >= HTTPStatus.INTERNAL_SERVER_ERROR:
            raise ServerError(error_msg)

    return response
