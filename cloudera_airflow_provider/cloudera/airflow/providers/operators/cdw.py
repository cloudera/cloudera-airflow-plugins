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

import re

from airflow.models import BaseOperator
from airflow.utils.operator_helpers import context_to_airflow_vars
from cloudera.airflow.providers.hooks.cdw import CdwHook


class CdwExecuteQueryOperator(BaseOperator):
    """
    Executes hql code in CDW. This class inherits behavior
    from HiveOperator, and instantiates a CdwHook to do the work.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CdwExecuteQueryOperator`
    """

    template_fields = ("hql", "schema", "hiveconfs")
    template_ext = (
        ".hql",
        ".sql",
    )
    ui_color = "#522a9f"
    ui_fgcolor = "#fff"

    def __init__(
        self,
        hql,
        schema="default",
        hiveconfs=None,
        hiveconf_jinja_translate=False,
        cli_conn_id="hive_cli_default",
        jdbc_driver=None,
        # new CDW args
        use_proxy_user=False,  # pylint: disable=unused-argument
        query_isolation=True,  # TODO: implement
        *args,
        **kwargs,
    ):

        super().__init__(*args, **kwargs)
        self.hql = hql
        self.schema = schema
        self.hiveconfs = hiveconfs or {}
        self.hiveconf_jinja_translate = hiveconf_jinja_translate
        self.run_as = None
        self.cli_conn_id = cli_conn_id
        self.jdbc_driver = jdbc_driver
        self.query_isolation = query_isolation
        # assigned lazily - just for consistency we can create the attribute with a
        # `None` initial value, later it will be populated by the execute method.
        # This also makes `on_kill` implementation consistent since it assumes `self.hook`
        # is defined.
        self.hook = None

    def get_hook(self):
        """Simply returns a CdwHook with the provided hive cli connection."""
        return CdwHook(
            cli_conn_id=self.cli_conn_id,
            query_isolation=self.query_isolation,
            jdbc_driver=self.jdbc_driver,
        )

    def prepare_template(self):
        if self.hiveconf_jinja_translate:
            self.hql = re.sub(r"(\$\{(hiveconf:)?([ a-zA-Z0-9_]*)\})", r"{{ \g<3> }}", self.hql)

    def execute(self, context):
        self.log.info("Executing: %s", self.hql)
        self.hook = self.get_hook()

        if self.hiveconf_jinja_translate:
            self.hiveconfs = context_to_airflow_vars(context)
        else:
            self.hiveconfs.update(context_to_airflow_vars(context))

        self.log.info("Passing HiveConf: %s", self.hiveconfs)
        self.hook.run_cli(hql=self.hql, schema=self.schema, hive_conf=self.hiveconfs)

    def dry_run(self):
        self.hook = self.get_hook()
        self.hook.test_hql(hql=self.hql)

    def on_kill(self):
        if self.hook:
            self.hook.kill()
