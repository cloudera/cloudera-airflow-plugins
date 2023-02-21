# Change Log
All notable changes to this project will be documented in this file.
 
The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## [2.1.1] - 2023-02-21

Added handling of HTTP 429 on job submission for retries.

## [2.1.0] - 2023-02-01

New environment variables allow to modify the `api_retries` and
`api_timeout` values used by the cde hook:
- `AIRFLOW__CDE__DEFAULT_NUM_RETRIES` to set the `api_retries` value.
- `AIRFLOW__CDE__DEFAULT_API_TIMEOUT` to set the `api_timeout` value.

The value precedence is 'parameter' > 'env var' > 'airflow.cfg' > 'default'.

With the environemt variables it is easier to fine tune the values without the
need to modify the existing DAG files.

## [2.0.1] - 2023-01-02

Synchronise version settings for public release.

## [2.0.0] - 2022-12-20

Changed import paths. Applied current Airflow community coding standards and linters. Fixes.

### Changed
   
New import paths.
```python
cloudera.airflow.providers.operators.cde
cloudera.airflow.providers.operators.cdw
cloudera.airflow.providers.hooks.cde
cloudera.airflow.providers.hooks.cdw
```

### Fixed

- Improved Handling CDE API calls.
- Fixed duplicate calls on job cancellation.

## [1.3.0] - 2022-12-15
### Changed
   
Make Airflow job run operator robust to retries.

## [1.2.0] - 2022-12-02
### Changed
   
Package distribution improvements.

## [1.1.0] - 2022-07-12
### Added
   
Added support of EU/AP regions.

## [1.0.2] - 2022-05-09
### Fixed

Fixed Сloudera Airflow plugin metadata. 

## [1.0.1] - 2022-05-05
### Fixed

Fixes for CDW Operator

## [1.0.0] - 2021-03-14
 
 Initial release.
 
