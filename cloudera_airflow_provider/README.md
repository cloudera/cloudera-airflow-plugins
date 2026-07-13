![Cloudera logo](../docs/images/Cloudera.png)

Cloudera provider for data orchestration with Apache Airflow.

## Overview

[Apache Airflow](https://github.com/apache/airflow) provider for **[Cloudera Data Engineering](https://docs.cloudera.com/data-engineering/cloud/index.html)** (CDE).

Python package: [cloudera-airflow-provider](https://pypi.org/project/cloudera-airflow-provider/)

Change log: [can be found here](CHANGELOG.md)

## Getting started

### Installation

[Cloudera Airflow Provider](https://pypi.org/project/cloudera-airflow-provider/) is a python package that can be installed:

through pip:
```shell script
pip install cloudera-airflow-provider
```
or using uv:

```shell script
uv pip install cloudera-airflow-provider
```
### Setting up the Airflow Connections

#### Cloudera Data Engineering connection

Connection contain parameters:

| Parameter | Description | Value |
| :--- | :---- | :--- |
| ID | Id of the Airflow connection of the target CDE Virtual cluster | Simple string making the Virtual cluster identifiable |
| Type | Airflow connection type | 'cloudera_data_engineering' Note: you need to set up operator package first |
| Host | [Virtual Cluster](https://docs.cloudera.com/data-engineering/cloud/manage-clusters/topics/cde-create-cluster.html) Jobs Api URL | From the CDE home page, go to Overview > Virtual Clusters > Cluster Details of the Virtual Cluster (VC) where you want the CDE job to run. Click JOBS API URL to copy the URL |
| Login | CDP access key (`auth_mode: cdp`) or AWC OAuth client_id (`auth_mode: awc`) | Customer credentials |
| Password | CDP private key (`auth_mode: cdp`) or AWC OAuth client_secret (`auth_mode: awc`) | Customer credentials |
| Extra | Optional settings in JSON format | Please refer table below |

Extra parameters for Cloudera Data Engineering connection:

| Parameter | Description | Default value |
| :--- | :---- | :--- |
| auth_mode | Authentication method: `cdp` (CDP access key) or `awc` (AWC OAuth) | `cdp` |
| awc_console_url | AWC console URL for the CDE service (needed for AWC token exchange) | None |
| proxy | Optional, translates to `https_proxy`/`HTTPS_PROXY` env. variables | None |
| cache_dir | Optional, to replace default cache_directory e.g. if insufficient access rights | `token_cache` |
| region | Optional, CDP Control Plane region ("us-west-1", "eu-1" or "ap-1") | Will be inferred automatically for Airflow 2, if not specified. Recommended to be configured for Airflow 3 for improved performance. |

Extra parameters for Cloudera Data Engineering connection for development use only (do not use them unless you know what you are doing) :

| Parameter | Description | Default value |
| :--- | :---- | :--- |
| altus_iam_endpoint | Optional | https://iamapi.us-west-1.altus.cloudera.com |
| ca_cert_path | Optional, custom ca certificates path used by CDE authentication | None |
| cdp_endpoint | CDP service endpoint | https://api.us-west-1.cdp.cloudera.com |
| insecure | Optional, insecure mode (no certs check) | False |
| form_factor | Optional, ("public" or "private")| None |
| ca_cert_path_access_key_auth | Optional, custom ca certificates path used by CDP workload and AWC console token access generation | None |


#### CDP connection (`auth_mode: cdp`)

Default path for CDE with CDP. Uses CDP access key + private key to obtaina workload token.

You can set up a connection according to the following snippet:

```bash

airflow connections add '{your connection id}' \
    --conn-type 'cloudera_data_engineering' \
    --conn-host '{Jobs API URL from Virtal Cluster}' \
    --conn-login '{your key id}' \
    --conn-password '{your key secret}'  \
    --conn-extra '{"region": "us-west-1"}'
```

#### AWC connection (`auth_mode: awc`)

Use this authentication mode when connecting from CWO to a CDE Virtual Cluster on AWC.

1. In the CDE instance's AWC console, authenticate and obtain a session cookie:
* Open `https://console.<domain>/` in your browser
* Log in with your credentials
* Open browser developer tools (F12) > Application > Cookies
* Copy the value of the `hadoop-jwt` cookie
2. Create an Access Key:
Call the credential provisioning endpoint with your session cookie. The access key is bound to your authenticated user identity.
```bash
curl -sk \
  -H "Cookie: hadoop-jwt=${HADOOP_JWT}" \
  -H "Content-Type: application/json" \
  -X POST "https://console.${DOMAIN}/api/v0/auth/access-keys/credentials" \
  -d '{"description": "my automation key"}'
```
3. Using the `client_id` and `client_secret` from step #2, configure the Airflow connection with the Virtual Cluster Jobs API URL as `host`, `client_id` / `client_secret` as login/password, and `awc_console_url` in extra field.
4. At runtime the hook exchanges credentials for a 1-hour Bearer JWT at
   `POST {awc_console_url}/api/v0/auth/access-keys/token` and uses it directly on the
   CDE Jobs API. Tokens are cached and renewed automatically before expiry.

```bash
airflow connections add '{your connection id}' \
    --conn-type 'cloudera_data_engineering' \
    --conn-host '{Jobs API URL from Virtal Cluster}' \
    --conn-login '<client_id>' \
    --conn-password '<client_secret>' \
    --conn-extra '{"auth_mode":"awc","awc_console_url":"https://console.<domain>"}'
```

The `auth_mode: cdp` path remains for CDP Public Cloud CDE. Do not mix CDP and AWC-specific extra fields on the same connection.

### Usage details

#### CdeRunJobOperator

Runs a [job](https://docs.cloudera.com/data-engineering/cloud/cli-access/topics/cde-cli-manage-jobs.html) in a CDE [Virtual Cluster](https://docs.cloudera.com/data-engineering/cloud/manage-clusters/topics/cde-create-cluster.html). The `CdeRunJobOperator` runs the
named job with optional variables and overrides. The job and its resources
must have already been created via the specified virtual cluster jobs API.

| Arguments | Type | Description |
| :--- | :---- | :--- |
| job_name | str | The name of the job on the target virtual cluster, required |
| connection_id | str | The Airflow connection id for the target API endpoint, default value `'cde_runtime_api'`. Please note that in CDE Airflow all of the connections of the Virtual Clusters within a CDE Service are available out of the box |
| variables | dict | A dictionary of key-value pairs to populate in the job configuration, default empty dict |
| overrides | dict | A dictionary of key-value pairs to override in the job configuration, default empty dict |
| wait | bool | If set to true, the operator will wait for the job to complete in the target cluster. The task exit status will reflect the  status of the completed job. Default `True` |
| timeout | int | The maximum time to wait in seconds for the job to complete if `wait=True`. If set to `None`, 0 or a negative number, the task will never time out. Default `0` |
| job_poll_interval | int | The interval in seconds at which the target API is polled for the job status. Default `10` |
| api_retries | int | The number of times to retry an API request in the event of a connection failure or non-fatal API error. The parameter can be used to overwrite the value used by the cde hook used by the operator. The value precedence is 'parameter' > 'env var' > 'airflow.cfg' > 'default'. The `AIRFLOW__CDE__DEFAULT_NUM_RETRIES` environmemt variable can be used to set the value. Default value in the cde hook: `9` |
| api_timeout | int | The timeout in seconds after which, if no response has been received from the API, a request should be abandoned and retried. The parameter can be used to overwrite the value used by the cde hook. The value precedence is 'parameter' > 'env var' > 'airflow.cfg' > 'default'. The `AIRFLOW__CDE__DEFAULT_API_TIMEOUT` environmemt variable can be used to set the value. The timeout value for the job run status check is calculated separately. The tenth of the `api_timeout` value is used if it is not less than `CdeHook.DEFAULT_API_TIMEOUT // 10`. If it is less the `CdeHook.DEFAULT_API_TIMEOUT // 10` value will be used. Default value in the cde hook: `300` |

##### Retries
The `api_retries` parameter from the above table specifies the number of times to retry an API request.
However, in case of [HTTP 429 Too Many Requests](https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/429),
the retry logic is different, and it is working as follows:

The rate limited retries are stopped either if we reached one of:
1. More than 2 hours of time frame of retrying
2. 1800 retries, 4 seconds between retries gives 2 hours of retrying.
No more retries will be performed if any of these conditions is satisfied.

In case of normal retries, retrying will be performed with exponential backoff.
In case of rate-limited retries, a fixed wait will be performed.
To be more precise, the wait time between retries is parsed from the server's [Retry-After](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Retry-After) HTTP response header.
So essentially it is up to the server to control the waiting time of the client.
If the Retry-After header is not found or cannot be parsed, the default retry interval of 4 seconds will be used.


`CdeRunJobOperator` DAG snippet:
```python
cde_task = CdeRunJobOperator(
    connection_id='my_vc_name',
    task_id='cde_task',
    dag=example_dag,
    job_name='example-scala-pi'
)
```
Please refer for complete [example DAG](../docs/examples/cde_operator_example.py).

`@task.cde` DAG snippet:
```python
@task.cde
def run_scala_pi() -> Union[None, str, Dict[str, Any]]:
    return "example-scala-pi"
```
Please refer for complete [example DAG](../docs/examples/cde_taskflow_example.py).

## Next steps

You can learn more about CDE concepts [here](https://docs.cloudera.com/data-engineering/cloud/cli-access/topics/cde-cli-concepts.html).

More examples you can find with [example DAGs](../docs/examples/README.md).

Please refer to the [official documentation](https://docs.cloudera.com/data-engineering/cloud/orchestrate-workflows/topics/cde-airflow-dag-pipeline.html) for how to integrate these operators into your pipelines.
