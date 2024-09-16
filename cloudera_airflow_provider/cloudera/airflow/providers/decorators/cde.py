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

import warnings
from typing import Any, Callable, Collection, Mapping, Sequence

from airflow.decorators.base import DecoratedOperator, TaskDecorator, task_decorator_factory
from airflow.utils.context import Context, context_merge
from airflow.utils.operator_helpers import determine_kwargs
from airflow.utils.types import NOTSET
from cloudera.airflow.providers.operators.cde import CdeRunJobOperator


class _CDERunJobDecoratedOperator(DecoratedOperator, CdeRunJobOperator):
    """
    Wraps a Python callable and uses the callable return value to set the CdeRunJobOperator's parameters.
    The return value must be either None, a string (job_name) or a dictionary where the keys are the names of
    the CdeRunJobOperator's arguments. (def my_callable(): -> Union[None, str, Dict[str, Any]])
    The callable's return value takes precedence over the decorator's arguments.

    :param python_callable: A reference to an object that is callable.
    :param op_kwargs: A dictionary of keyword arguments that will get unpacked
        in your function (templated).
    :param op_args: A list of positional arguments that will get unpacked when
        calling your callable (templated).
    """

    template_fields: Sequence[str] = (*DecoratedOperator.template_fields, *CdeRunJobOperator.template_fields)
    template_fields_renderers: dict[str, str] = {
        **DecoratedOperator.template_fields_renderers,
    }

    custom_operator_name: str = "@task.cde"

    def __init__(
        self,
        *,
        python_callable: Callable,
        op_args: Collection[Any] | None = None,
        op_kwargs: Mapping[str, Any] | None = None,
        **kwargs,
    ) -> None:
        if kwargs.pop("multiple_outputs", None):
            warnings.warn(
                f"`multiple_outputs=True` is not supported in {self.custom_operator_name} tasks. Ignoring.",
                UserWarning,
                stacklevel=3,
            )

        job_name = kwargs.pop("job_name", NOTSET)
        super().__init__(
            python_callable=python_callable,
            op_args=op_args,
            op_kwargs=op_kwargs,
            job_name=job_name,
            multiple_outputs=False,
            **kwargs,
        )

    def execute(self, context: Context) -> Any:
        context_merge(context, self.op_kwargs)
        kwargs = determine_kwargs(self.python_callable, self.op_args, context)

        parameters = self.python_callable(*self.op_args, **kwargs)

        if parameters is None:
            pass
        elif isinstance(parameters, str):
            self.job_name = parameters
        elif isinstance(parameters, dict):

            def pop_validate_set(param_name: str, cls: type):
                value = parameters.pop(param_name, None)
                if value is not None:
                    if not isinstance(value, cls):
                        raise TypeError(
                            f"The returned parameter='{param_name}' from the TaskFlow callable "
                            f"must be type={cls}. Got {type(value)}"
                        )
                    setattr(self, param_name, value)

            pop_validate_set("job_name", str)
            pop_validate_set("variables", dict)
            pop_validate_set("overrides", dict)
            pop_validate_set("connection_id", str)
            pop_validate_set("wait", bool)
            pop_validate_set("timeout", int)
            pop_validate_set("job_poll_interval", int)
            pop_validate_set("api_retries", int)
            pop_validate_set("api_timeout", int)
            # `user` is not supported

            if not bool(parameters):
                self.log.warning(
                    f"The returned parameters from the TaskFlow callable "
                    f"contain unsupported keys, ignoring: {parameters.keys()}"
                )
        else:
            raise TypeError(
                f"The returned parameters from the TaskFlow callable must be "
                f"a Union[None, str, Dict[str, Any]]. Got: {type(parameters)}"
            )

        if self.job_name.strip() == "":
            raise ValueError("job_name is required")

        return super().execute(context)


def cde_task(
    python_callable: Callable | None = None,
    **kwargs,
) -> TaskDecorator:
    """
    Wrap a function into a CdeRunJobOperator.

    Accepts kwargs for operator kwargs. Can be reused in a single DAG. This function is only used
    during type checking or auto-completion.

    :param python_callable: Function to decorate.

    :meta private:
    """
    return task_decorator_factory(
        python_callable=python_callable,
        decorated_operator_class=_CDERunJobDecoratedOperator,
        **kwargs,
    )
