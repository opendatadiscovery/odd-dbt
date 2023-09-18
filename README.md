# OpenDataDiscovery dbt tests metadata collecting

[![PyPI version](https://badge.fury.io/py/odd-dbt.svg)](https://badge.fury.io/py/odd-dbt)

CLI tool helps run and ingest dbt test to platform.

It can be used as separated CLI tool or within [ODD CLI](https://github.com/opendatadiscovery/odd-cli) package which
provides some useful additional features for working with OpenDataDiscovery.

## Supported adapters

| Adapter   | version |
|-----------|---------|
| Snowflake | ^1.6    |
| Postgres  | ^1.6    |

Profiles inside the file looks different for each type of data source.

**Snowflake** host_settings value is created from field `account`. Field value should be `<account_identifier>`
For example the URL for an account uses the following format: `<account_identifier>`.snowflakecomputing.com
Example Snowflake account identifier `hj1234.eu-central-1`.

## Supported tests types

1. [x]  Generic tests
2. [ ] Singular tests. Currently Singular tests are not supported.

## Installation
```pip install odd-dbt```

## To see all available commands
```
odd_dbt_test --help
```

## Example
For each command that involves sending information to OpenDataDiscovery platform exists set of env variables:
1. `ODD_PLATFORM_HOST` - Where you platform is
2. `ODD_PLATFORM_TOKEN` - Token for ingesting data to platform (How to create [token](https://docs.opendatadiscovery.org/configuration-and-deployment/trylocally#create-collector-entity)?)
3. `DBT_DATA_SOURCE_ODDRN` - Unique oddrn string describes dbt source, i.e '//dbt/host/localhost'

It is recommended to add them as ENV variables or provide as flags to each command
```
export ODD_PLATFORM_HOST=http://localhost:8080
export ODD_PLATFORM_TOKEN=token***
export DBT_DATA_SOURCE_ODDRN=//dbt/host/localhost
```

### Commands
`create-datasource` - helps to register dbt as data source at OpenDataDiscovery platform. User later for ingesting metadata.
```commandline
odd_dbt_test create-datasource --name=my_local_dbt --dbt-host=localhost
```

`ingest-test` - Read results_run file under the target folder to parse and ingest metadata.
```commandline
odd_dbt_test ingest-test --profile=my_profile
```

`test` - Proxy command to `dbt test`, then reads results_run file under the target folder to parse and ingest metadata.
```commandline
odd_dbt_test test --profile=my_profile
```

### Run commands programmatically
You could run that scrip to read, parse and ingest test results to the platform.

```python
# ingest_test_result.py

from odd_dbt import config
from odd_dbt.domain.cli_args import CliArgs
from odd_dbt.libs.dbt import get_context
from odd_dbt.libs.odd import create_dbt_generator_from_oddrn
from odd_dbt.service.odd import ingest_entities
from odd_dbt.mapper.test_results import DbtTestMapper
from odd_dbt.mapper.lineage import DbtLineageMapper

cfg = config.Config()  # All fields can be set manually or read from ENV variables
client = config.create_odd_client(host=cfg.odd_platform_host, token=cfg.odd_platform_token)
generator = create_dbt_generator_from_oddrn(oddrn=cfg.dbt_data_source_oddrn)

cli_args = CliArgs.default()
context = get_context(cli_args=cli_args)

# Ingest lineage
data_entities = DbtLineageMapper(context=context, generator=generator).map()
ingest_entities(data_entities, client)

# Or ingest test results
data_entities = DbtTestMapper(context=context, generator=generator).map()
ingest_entities(data_entities, client)
```