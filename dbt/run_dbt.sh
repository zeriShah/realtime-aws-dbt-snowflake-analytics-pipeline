#!/bin/bash

cd /home/ubuntu/ais-maritime-pipeline/dbt

docker run --rm \
--network host \
-v /home/ubuntu/ais-maritime-pipeline/dbt/ais_maritime_dbt:/usr/app \
-v /home/ubuntu/ais-maritime-pipeline/dbt/profiles:/root/.dbt \
-w /usr/app \
ghcr.io/dbt-labs/dbt-snowflake:1.8.0 \
run >> /home/ubuntu/ais-maritime-pipeline/dbt/dbt_run.log 2>&1
