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

"""Holds connections for the Cloudera Products"""

import json
from json.decoder import JSONDecodeError
from typing import Optional
from urllib.parse import urlparse

from airflow.models.connection import Connection

class CDEConnection(Connection):
    """Connection details to the Cloudera Data Engineering product"""

    CDE_API_PREFIX = '/api/v1'

    def __init__(
            self,
            connection_id: str,
            scheme: str,
            host: str,
            api_base_route: str,
            access_key: str,
            private_key: str,
            port: Optional[int] = None,
            cache_dir: Optional[str] = None,
            ca_cert_path: Optional[str] = None,
            proxy: Optional[str] = None,
            cdp_endpoint: Optional[str] = None,
            altus_iam_endpoint: Optional[str] = None,
            insecure : bool = False
            ) -> None:
        super().__init__(conn_id=connection_id, host=host,
            login=access_key, password=private_key, port=port)
        self.conn_type = 'cloudera_data_engineering'
        self.scheme = scheme
        self.api_base_route = api_base_route
        self.cache_dir = cache_dir
        self.ca_cert_path = ca_cert_path
        self.proxy = proxy
        self.cdp_endpoint = cdp_endpoint
        self.altus_iam_endpoint = altus_iam_endpoint
        self.insecure = insecure

    def is_external(self) -> bool:
        """Checks if connection is external. External connections
        are tyically cross-services connections or connection defined
        in an external Airflow instance.

        Returns:
            True of connection is external, false otherwise
        """
        return not self.is_internal()

    def is_internal(self) -> bool:
        """Checks if connection is internal. Internal connections
        are only meant to be used within a CDE service and are managed
        automatically by the Virtual Cluster.

        Returns:
            True of connection is internal, false otherwise
        """
        return self.__internal_connection(self.host)

    def get_vcluster_jobs_api_url(self) -> str:
        """Constructs the jobs api url from the elements defined in the connection.

        Returns:
            vcluster_jobs_api_url: the jobs api url
        """

        vcluster_jobs_api_url = f"{self.scheme}://{self.host}"
        if self.port:
            vcluster_jobs_api_url += ":" + str(self.port)
        vcluster_jobs_api_url += self.api_base_route
        return vcluster_jobs_api_url

    @property
    def access_key(self) -> str:
        """CDP Access key

        Returns:
            the access key associated to the connection
        """
        return self.login

    @property
    def private_key(self) -> str:
        """CDP Private key

        Returns:
            the private key associated to the connection
        """
        # Relies on Airflow Connection password getter,
        # so that the password is not stored in clear in the memory
        return self.password

    @classmethod
    def __internal_connection(cls, hostname: str) -> bool:
        return hostname.endswith(".svc") or hostname.endswith(".svc.cluster.local")

    @classmethod
    def from_airflow_connection(cls, conn: Connection) -> "CDEConnection":
        """Factory method for constructing a CDE connection from an Airflow Connection.

        Args:
            conn: an Airflow Connection instance

        Returns:
            A new CDE connection with the parameters derived from the Airflow connection
        """
        try:
            if conn.extra:
                extra = json.loads(conn.extra)
            else:
                extra = {}
        except JSONDecodeError as err:
            raise ValueError(f"Invalid extra property: {repr(err)}" ) from err
        if conn.host and "://" in conn.host:
            conn_uri = conn.host
        else:
            conn_uri = conn.get_uri()
        connection_url = urlparse(conn_uri)

        # Internal endpoints have base prefix
        api_base_route = cls.CDE_API_PREFIX \
                            if cls.__internal_connection(connection_url.hostname) \
                            else connection_url.path

        return cls(
            conn.conn_id,
            connection_url.scheme,
            connection_url.hostname,
            api_base_route,
            conn.login,
            conn.password,
            port=conn.port,
            cache_dir=extra.get('cache_dir'),
            ca_cert_path=extra.get('ca_cert_path'),
            proxy=extra.get('proxy'),
            cdp_endpoint=extra.get('cdp_endpoint'),
            altus_iam_endpoint=extra.get('altus_iam_endpoint'),
            insecure=extra.get('insecure', False)
            )

    def __repr__(self) -> str:
        return repr(self.__dict__)
