# OpenDataDiscovery dbt tests metadata collecting
[![PyPI version](https://badge.fury.io/py/odd-dbt.svg)](https://badge.fury.io/py/odd-dbt)

CLI tool helps automatically parse and ingest DBT test results to OpenDataDiscovery Platform.
It can be used as separated CLI tool or within [ODD CLI](https://github.com/opendatadiscovery/odd-cli) package which provides some useful additional features.

## Installation
```pip install odd-dbt```

## Command options
```
╭─ Options ─────────────────────────────────────────────────────────────╮
│    --project-dir                 PATH  [default: Path().cwd()odd-dbt] │
│    --target                      TEXT  [default:None]                 │
│    --profile-name                TEXT  [default:None]                 │
│ *  --host    -h                  TEXT  [env var: ODD_PLATFORM_HOST]   │
│ *  --token   -t                  TEXT  [env var: ODD_PLATFORM_TOKEN]  │
│    --dbt-host                    TEXT  [default: localhost]           │
│    --help                              Show this message and exit.    │
╰───────────────────────────────────────────────────────────────────────╯
```


## Command run example
How to create [collector token](https://docs.opendatadiscovery.org/configuration-and-deployment/trylocally#create-collector-entity)?
```bash
odd_dbt_test --host http://localhost:8080 --token <COLLECTOR_TOKEN>
```



## Supported data sources
| Source    |       |
| --------- | ----- |
| Snowflake | 1.4.1 |
| Redshift  | 1.4.0 |
| Postgres  | 1.4.5 |
| MSSQL     |       | 

## Requirements
Library to inject Quality Tests entities requires presence of corresponding with them datasets entities in the platform.
For example: if you want to inject data quality test of Snowflake table, you need to have entity of that table present in the platform.

## Supported tests
Library supports for basics tests provided by dbt.
- `unique`: values in the column should be unique
- `not_null`: values in the column should not contain null values
- `accepted_values`: column should only contain values from list specified in the test config
- `relationships`: each value in the select column of the model exists as a specified field in the reference table (also known as referential integrity)

## ODDRN generation for datasets
`host_settings` of ODDRN generators required for source datasets are loaded from `.dbt/profiles.yml`.

Profiles inside the file looks different for each type of data source.

**Snowflake** host_settings value is created from field `account`. Field value should be `<account_identifier>`
For example the URL for an account uses the following format: `<account_identifier>`.snowflakecomputing.com
Example Snowflake account identifier `hj1234.eu-central-1`.

**Redshift** and **Postgres** host_settings are loaded from field `host` field.

Example Redshift host: `redshift-cluster-example.123456789.eu-central-1.redshift.amazonaws.com`
