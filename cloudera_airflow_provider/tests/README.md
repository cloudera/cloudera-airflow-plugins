#### General description.
This directory contain tests for the `cloudera_airflow_provider`.

#### Pre requisites for local test run.
1. Installed dependencies installed e.g. '{repo_root}/cloudera/scripts/ci/docker/requirements_all_versions'
2. Installed Airflow & `airflow db init`
3. Installed `cloudera_airflow_provider` in dev mode:
 `cd cloudera_airflow_plugin && pip install .`

Such steps also can be done in container environment like:
1. Run CI tool shell
`/cloudera/scripts/ci/citool shell`
2. in container run `./cloudera/scripts/ci/tests/upstream_tests_manual.sh`

#### About imports and namespaces.
Package use namespacing configured  in `setup.cfg` of the operator package: 

```python
[options.packages.find]
include =
  cloudera*
```
 
 Related documentation:
 https://packaging.python.org/en/latest/guides/packaging-namespace-packages/
 
 #### How to be sure that not only code tests are OK but also imports are clear.
 Please check, that all tested code are working OK without import tricks.
 It means that package should works like regular `pip install .` and accessible for imports outside package directory.
 After each build cache should be deleted:
 - `build` directory
 - `*.egg-info` directory 
 - clear pip cache: `pip cache purge`.
 Without such cleaning results can be wrong.