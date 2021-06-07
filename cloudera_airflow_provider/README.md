# Cloudera Airflow Provider
This component provides two Cloudera job operators to be integrated in your dags:
* CDEJobRunOperator, for triggering Cloudera Data Engineering jobs through API tokens
* CDWOperator, for triggering Cloudera Data Warehouse jobs through username/password as of today

## Installation
Run the following snippet on your airflow server:
```
git clone https://github.com/cloudera/cloudera-airflow-plugins.git
cd cloudera-airflow-plugins/cloudera_airflow_provider 
pip install .
```

Notes: 
* Python 3.6 or later is required to run this provider.
* cryptography >= 3.3.2 is required for this plugin to handle CVE-2020-36242. If an older version is available, the plugin will update the cryptography library
* Depending on your python installation policies, you may need to issue ```pip install --user .``` instead of ```pip install .```

## Getting started
Once installed as described above, please refer the the [official documentation](https://docs.cloudera.com/data-engineering/cloud/manage-jobs/topics/cde-airflow-dag-pipeline.html) for how to integrate these operators into your pipelines.