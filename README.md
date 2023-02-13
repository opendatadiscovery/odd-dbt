# OpenDataDiscovery dbt tests metadata collecting
[![PyPI version](https://badge.fury.io/py/odd-dbt.svg)](https://badge.fury.io/py/odd-dbt)

Library used for running dbt tests and injecting them as entities to ODD platform. 

## Supported data sources
| Source    |
|-----------| 
| Snowflake | 
| Redshift  |
| Postgres  |

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
host_settings of ODDRN generators required for source datasets are loaded from `.dbt/profiles.yml`.  
Profiles inside the file looks different for each type of data source.  
**Snowflake** host_settings value is created from field `account`. Field value should be `<account_identifier>`  
For example the URL for an account uses the following format: `<account_identifier>`.snowflakecomputing.com  
Example Snowflake account identifier `hj1234.eu-central-1`.  
**Redshift** and **Postgres** host_settings are loaded from field `host` field.  
Example Redshift host: `redshift-cluster-example.123456789.eu-central-1.redshift.amazonaws.com`  