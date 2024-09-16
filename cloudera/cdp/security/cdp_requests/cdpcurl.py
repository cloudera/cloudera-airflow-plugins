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

"""cdpcurl implementation."""
import datetime
import pprint
import sys
from email.utils import formatdate

from cloudera.cdp.security import submit_request
from cloudera.cdp.security.cdp_requests.cdpv1sign import make_signature_header

__author__ = "cloudera"

IS_VERBOSE = False


def __log(*args, **kwargs):
    if not IS_VERBOSE:
        return
    stderr_pp = pprint.PrettyPrinter(stream=sys.stderr)
    stderr_pp.pprint(*args, **kwargs)


def __now():
    return datetime.datetime.now(datetime.timezone.utc)


# pylint: disable=too-many-arguments,too-many-locals
def make_request(method, uri, headers, data, access_key, private_key, data_binary, verify=True):
    """
    # Make HTTP request with CDP request signing

    :return: http request object
    :param method: str
    :param uri: str
    :param headers: dict
    :param data: str
    :param profile: str
    :param access_key: str
    :param private_key: str
    :param data_binary: bool
    :param verify: bool
    """

    if "x-altus-auth" in headers:
        raise Exception("x-altus-auth found in headers!")
    if "x-altus-date" in headers:
        raise Exception("x-altus-date found in headers!")
    headers["x-altus-date"] = formatdate(timeval=__now().timestamp(), usegmt=True)
    headers["x-altus-auth"] = make_signature_header(method, uri, headers, access_key, private_key)

    if data_binary:
        return __send_request(uri, data, headers, method, verify)
    return __send_request(uri, data.encode("utf-8"), headers, method, verify)


def __send_request(uri, data, headers, method, verify):
    __log("\nHEADERS++++++++++++++++++++++++++++++++++++")
    __log(headers)

    __log("\nBEGIN REQUEST++++++++++++++++++++++++++++++++++++")
    __log("Request URL = " + uri)

    response = submit_request(method, uri, headers=headers, data=data, verify=verify)

    __log("\nRESPONSE++++++++++++++++++++++++++++++++++++")
    __log(f"Response code: {response.status_code}\n")

    return response
