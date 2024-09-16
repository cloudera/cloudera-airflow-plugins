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

from airflow.decorators import dag, task
from pendulum import datetime
from typing import Any, Dict, Tuple, Union

default_args = {
    'owner': 'airflow',
}

@dag(
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    schedule=None,
    description="TaskFlow CDERunJobOperator examples"
)
def cde_taskflow_example():
    @task.cde(connection_id="default-vc", job_name="example-scala-pi")
    def run_scala_pi() -> Union[None, str, Dict[str, Any]]:
        """
        Runs a CDE Job with the given name, decorator's argument.

        :return: None
        """
        return None

    @task.cde(connection_id="default-vc")
    def run_scala_pi_job_name() -> Union[None, str, Dict[str, Any]]:
        """
        Runs a CDE job with the given name, return value.
        The CDERunJobOperator parameters can be passed in the decorator's arguments.

        :return: the job_name
        """
        return "example-scala-pi"

    @task
    def get_number_of_executors():
        return 4

    @task.cde(
        connection_id="default-vc",
    )
    def run_scala_pi_parameters(num_executors: int) -> Union[None, str, Dict[str, Any]]:
        """
        Runs a CDE job with the given name and overrides.
        The CDERunJobOperator parameters can be passed in the decorator's arguments,
        the parameters returned in the dict takes precedence over the decorator's arguments.

        :param num_executors: number of executors parameter
        :return: the job_name and the overrides
        """
        return {
            "job_name": "example-scala-pi",
            "overrides": {
                "spark": {
                    "numExecutors": num_executors
                }
            }
        }

    [run_scala_pi(), run_scala_pi_job_name(), run_scala_pi_parameters(get_number_of_executors())]


cde_taskflow_example()
