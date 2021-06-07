#  CLOUDERA CDP
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


import time

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from cloudera.cdp.airflow.hooks.cde_hook import CDEHook, CDEHookException


class CDEJobRunOperator(BaseOperator):
    """
    Runs a job in a CDE Virtual Cluster. The ``CDEJobRunOperator`` runs the
    named job with optional variables and overrides. The job and its resources
    must have already been created via the specified virtual cluster jobs API.

    The virtual cluster API endpoint is specified by setting the
    ``connection_id`` parameter. The "local" virtual cluster jobs API is the
    default and has a special value of ``cde_runtime_api``. Authentication to
    the API is handled automatically and any jobs in the DAG will run as the
    user who submitted the DAG.

    Jobs can be defined in a virtual cluster with variable placeholders,
    e.g. ``{{ inputdir }}``. Currently the fields supporting variable expansion
    are Spark application name, Spark arguments, and Spark configurations.
    Variables can be passed to the operator as a dictionary of key-value string
    pairs. In addition to any user variables passed via the ``variables``
    parameter, the following standard Airflow macros are automatically
    populated as variables by the operator (see
    https://airflow.apache.org/docs/stable/macros-ref):

    * ``ds``: the execution date as ``YYYY-MM-DD``
    * ``ds_nodash``: the execution date as ``YYYYMMDD``
    * ``ts``: execution date in ISO 8601 format
    * ``ts_nodash``: execution date in ISO 8601 format without '-', ':' or
          timezone information
    * ``run_id``: the run_id of the current DAG run

    If a CDE job needs to run with a different configuration, a task can be
    configured with runtime overrides. For example to override the Spark
    executor memory and cores for a task and to supply an additional config
    parameter you could supply the following dictionary can be supplied to
    the ``overrides`` parameter::

        {
            'spark': {
                'executorMemory': '8g',
                'executorCores': '4',
                'conf': {
                    'spark.kubernetes.memoryOverhead': '2048'
                }
            }
        }

    See the CDE Jobs API documentation for the full list of parameters that
    can be overridden.

    Via the ``wait`` parameter, jobs can either be submitted asynchronously to
    the API (``wait=False``) or the task can wait until the job is complete
    before exiting the task (default is ``wait=True``). If ``wait`` is
    ``True``, the task exit status will reflect the final status of the
    submitted job (or the task will fail on timeout if specified). If ``wait``
    is ``False`` the task status will reflect whether the job was successfully
    submitted to the API or not.

    Note: all parameters below can also be provided through the
    ``default_args`` field of the DAG.

    :param job_name: the name of the job in the target cluster, required
    :type job_name: str
    :param connection_id: the Airflow connection id for the target API
        endpoint, default value ``'cde_runtime_api'``
    :type connection_id: str
    :param variables: a dictionary of key-value pairs to populate in the
        job configuration, default empty dict.
    :type variables: dict
    :param overrides: a dictionary of key-value pairs to override in the
        job configuration, default empty dict.
    :type overrides: dict
    :param wait: if set to true, the operator will wait for the job to
        complete in the target cluster. The task exit status will reflect the
        status of the completed job. Default ``True``
    :type wait: bool
    :param timeout: the maximum time to wait in seconds for the job to
        complete if ``wait=True``. If set to ``None``, 0 or a negative number,
        the task will never be timed out. Default ``0``.
    :type timeout: int
    :param job_poll_interval: the interval in seconds at which the target API
        is polled for the job status. Default ``10``.
    :type job_poll_interval: int
    :param api_retries: the number of times to retry an API request in the event
        of a connection failure or non-fatal API error. Default ``9``.
    :type job_poll_interval: int
    """
    template_fields = ('variables', 'overrides')
    ui_color = '#385f70'
    ui_fgcolor = '#fff'

    DEFAULT_WAIT = True
    DEFAULT_POLL_INTERVAL = 10
    DEFAULT_TIMEOUT = 0
    DEFAULT_RETRIES = 9
    DEFAULT_CONNECTION_ID = 'cde_runtime_api'

    @apply_defaults
    def __init__(
            self,
            job_name=None,
            variables=None,
            overrides=None,
            connection_id=DEFAULT_CONNECTION_ID,
            wait=DEFAULT_WAIT,
            timeout=DEFAULT_TIMEOUT,
            job_poll_interval=DEFAULT_POLL_INTERVAL,
            api_retries=DEFAULT_RETRIES,
            user=None,
            **kwargs):
        super().__init__(**kwargs)
        self.job_name = job_name
        self.variables = variables or {}
        self.overrides = overrides or {}
        self.connection_id = connection_id
        self.wait = wait
        self.timeout = timeout
        self.job_poll_interval = job_poll_interval
        if user:
            self.log.warning("Proxy user is not yet supported. Setting it to None.")
        self.user = None
        self.api_retries = api_retries
        if not self.job_name:
            raise ValueError('job_name required')
        # Set internal state
        self._hook = self.get_hook()
        self._job_run_id = -1

    def execute(self, context):
        self._job_run_id = self.submit_job(context)
        if self.wait:
            self.wait_for_job()

    def on_kill(self):
        if self._hook and self._job_run_id > 0:
            self.log.info("Task killed, cancelling job run: %d", self._job_run_id)
            try:
                self._hook.kill_job_run(self._job_run_id)
            except CDEHookException as err:
                msg = f"Issue while killing CDE job. Exiting. Error details: {err}"
                self.log.error(msg)
                raise AirflowException(msg) from err
            except Exception as err:
                msg = ("Most probably unhandled error in CDE Airflow plugin."
                    f" Please report this issue to Cloudera. Details: {err}")
                self.log.error(msg)
                raise AirflowException(msg) from err

    def get_hook(self):
        """Return CDEHook using specified connection"""
        return CDEHook(connection_id=self.connection_id, num_retries=self.api_retries)

    def submit_job(self, context):
        """Submit a job run request to CDE via the hook"""
        # merge user-supplied variables and airflow variables
        user_vars = self.variables or {}
        airflow_vars = {
            'ds': context['ds'],
            'ds_nodash': context['ds_nodash'],
            'ts': context['ts'],
            'ts_nodash': context['ts_nodash'],
            'run_id': context['run_id'],
        }
        merged_vars = {**airflow_vars, **user_vars}

        try:
            job_run_id = self._hook.submit_job(
                self.job_name,
                variables=merged_vars,
                overrides=self.overrides
                )
        except CDEHookException as err:
            msg = f"Issue while submitting job. Exiting. Error details: {err}"
            self.log.error(msg)
            raise AirflowException(msg) from err
        except Exception as err:
            msg = ("Most probably unhandled error in CDE Airflow plugin."
                f" Please report this issue to Cloudera. Details: {err}")
            self.log.error(msg)
            raise AirflowException(msg) from err
        self.log.info("Job submitted with run id: %s", job_run_id)

        return job_run_id

    def wait_for_job(self):
        """Wait for a submitted job run to complete and raise exception if failed"""
        self.log.info(
            "Waiting for job completion, job run id: %s", self._job_run_id)
        end_time = None
        if self.timeout > 0:
            self.log.info("Wait timeout set to %d seconds", self.timeout)
            end_time = int(time.time()) + self.timeout

        check_time = int(time.time())
        while not end_time or end_time > check_time:
            try:
                job_status = self._hook.check_job_run_status(self._job_run_id)
            except CDEHookException as err:
                msg = f"Issue while checking job status. Exiting. Error details: {err}"
                self.log.error(msg)
                raise AirflowException(msg) from err
            except Exception as err:
                msg = ("Most probably unhandled error in CDE Airflow plugin."
                    f" Please report this issue to Cloudera. Details: {err}")
                self.log.error(msg)
                raise AirflowException(msg) from err
            if job_status in ('starting', 'running'):
                msg = f'Job run in {job_status} status,' \
                      f' checking again in {self.job_poll_interval} seconds'
                self.log.info(msg)
            elif job_status == 'succeeded':
                msg = f'Job run completed with {job_status} status'
                self.log.info(msg)
                return
            elif job_status in ('failed', 'killed', 'unknown'):
                msg = f'Job run exited with {job_status} status'
                self.log.error(msg)
                raise AirflowException(msg)
            else:
                msg = f'Got unexpected status when polling for job: {job_status}'
                self.log.error(msg)
                raise AirflowException(msg)
            time.sleep(self.job_poll_interval)
            check_time = int(time.time())

        raise TimeoutError(
            f'Job run did not complete in {self.timeout} seconds')
