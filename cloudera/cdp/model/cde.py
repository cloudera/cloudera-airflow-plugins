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
"""Holds model objects related to CDE Environments."""

import re
from urllib.parse import urlparse


class VirtualCluster:
    """Represents a CDE Virtual cluster and hold various helper methods."""

    ACCESS_KEY_AUTH_ENDPOINT_PATH = "/gateway/cdptkn/knoxtoken/api/v1/token"

    def __init__(self, vcluster_endpoint: str) -> None:
        """
        Args:
            vcluster_endpoint: the endpoint of the Virtual Cluster corresponding to the
                               Job API URL
        """
        self.vcluster_endpoint = vcluster_endpoint
        auth_endpoint_url = urlparse(vcluster_endpoint)
        self.host = auth_endpoint_url.hostname

    def get_service_id(self) -> str:
        """
        Obtains cluster id from the virtual cluster endpoint.

        Returns:
            cluster id string in the form 'cluster-<cluster_id>'

        Raises:
            ValueError: When the given url is not in the expected format
        """
        pattern = re.compile("cde-[a-zA-Z0-9]*")
        try:
            first_match = pattern.findall(self.vcluster_endpoint)[0]
            return re.sub("^cde-", "cluster-", first_match, 1)
        except IndexError as err:
            raise ValueError(f"Cluster ID not found in {self.vcluster_endpoint}") from err

    def get_auth_endpoint(self) -> str:
        """
        Derive the authentication endpoint from the virtual cluster cluster endpoint

        Returns:
            Endpoint of the authentication service

        Raises:
            ValueError of the input has incorrect form
        """
        auth_endpoint = re.sub("^https://[a-zA-Z0-9]*", "https://service", self.vcluster_endpoint, 1)

        if auth_endpoint == self.vcluster_endpoint:
            raise ValueError(
                f"Invalid vcluster endpoint given: {self.vcluster_endpoint}",
            )

        auth_endpoint_url = urlparse(auth_endpoint)
        auth_endpoint_url = auth_endpoint_url._replace(path=self.ACCESS_KEY_AUTH_ENDPOINT_PATH)
        return auth_endpoint_url.geturl()
