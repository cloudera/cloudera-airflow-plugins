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

from __future__ import annotations

from airflow.providers.apache.hive.sensors.hive_partition import HivePartitionSensor  # type: ignore
from cloudera.airflow.providers.hooks.cdw import CdwHiveMetastoreHook


# pylint: disable=too-many-ancestors; HivePartitionSensor is external
class CdwHivePartitionSensor(HivePartitionSensor):
    """
    CdwHivePartitionSensor is a subclass of HivePartitionSensor and supposed to implement
    the same logic by delegating the actual work to a CdwHiveMetastoreHook instance.
    """

    template_fields = (
        "schema",
        "table",
        "partition",
    )
    ui_color = "#C5CAE9"

    def __init__(
        self,
        table,
        partition="ds='{{ ds }}'",
        cli_conn_id="metastore_default",
        schema="default",
        poke_interval=60 * 3,
        *args,
        **kwargs,
    ):
        super().__init__(table=table, poke_interval=poke_interval, *args, **kwargs)
        if not partition:
            partition = "ds='{{ ds }}'"
        self.cli_conn_id = cli_conn_id
        self.table = table
        self.partition = partition
        self.schema = schema
        self.hook = None

    # pylint: disable=unused-argument,missing-function-docstring; poke is overridden from the parent class
    def poke(self, context):
        if "." in self.table:
            self.schema, self.table = self.table.split(".")
        self.log.info(
            "Poking for table %s.%s, partition %s",
            self.schema,
            self.table,
            self.partition,
        )
        if self.hook is None:
            self.hook = CdwHiveMetastoreHook(cli_conn_id=self.cli_conn_id)
        return self.hook.check_for_partition(self.schema, self.table, self.partition)
