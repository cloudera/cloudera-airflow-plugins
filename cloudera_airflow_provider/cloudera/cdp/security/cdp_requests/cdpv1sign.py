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

"""Implementation of the CDP API signature specification, V1."""
import json
from base64 import b64decode, urlsafe_b64encode
from collections import OrderedDict
from urllib.parse import urlparse

from pure25519 import eddsa    # type: ignore


def create_canonical_request_string(method, uri, headers, auth_method):
    """Create a canonical request string from aspects of the request."""
    headers_of_interest = []
    for header_name in ["content-type", "x-altus-date"]:
        found = False
        for key in headers:
            key_lc = key.lower()
            if headers[key] is not None and key_lc == header_name:
                headers_of_interest.append(headers[key].strip())
                found = True
        if not found:
            headers_of_interest.append("")

    # Our signature verification with treat a query with no = as part of the
    # path, so we do as well. It appears to be a behavior left to the server
    # implementation, and python and our java servlet implementation disagree.
    uri_components = urlparse(uri)
    path = uri_components.path
    if not path:
        path = "/"
    if uri_components.query and "=" not in uri_components.query:
        path += "?" + uri_components.query

    canonical_string = method.upper() + "\n"
    canonical_string += "\n".join(headers_of_interest) + "\n"
    canonical_string += path + "\n"
    canonical_string += auth_method

    return canonical_string


def create_signature_string(canonical_string, private_key):
    """
    Create the string form of the digital signature of the canonical request
    string.
    """
    seed = b64decode(private_key)
    if len(seed) != 32:
        raise Exception("Not an Ed25519 private key!")
    public_key = eddsa.publickey(seed)
    signature = eddsa.signature(canonical_string.encode("utf-8"), seed, public_key)
    return urlsafe_b64encode(signature).strip().decode("utf-8")


def create_encoded_authn_params_string(access_key, auth_method):
    """Create the base 64 encoded string of authentication parameters."""
    auth_params = OrderedDict()
    auth_params["access_key_id"] = access_key
    auth_params["auth_method"] = auth_method
    encoded_json = json.dumps(auth_params).encode("utf-8")
    return urlsafe_b64encode(encoded_json).strip()


def create_signature_header(encoded_authn_params, signature):
    """
    Combine the encoded authentication parameters string and signature string
    into the signature header value.
    """
    return f"{encoded_authn_params.decode('utf-8')}.{signature}"


def make_signature_header(method, uri, headers, access_key, private_key):
    """
    Generates the value to be used for the x-altus-auth header in the service
    call.
    """
    if len(private_key) != 44:
        raise Exception("Only ed25519v1 keys are supported!")

    auth_method = "ed25519v1"

    canonical_string = create_canonical_request_string(method, uri, headers, auth_method)
    signature = create_signature_string(canonical_string, private_key)
    encoded_authn_params = create_encoded_authn_params_string(access_key, auth_method)
    signature_header = create_signature_header(encoded_authn_params, signature)
    return signature_header
