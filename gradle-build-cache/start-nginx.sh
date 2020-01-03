#!/bin/bash

base_path=$(python3 -c 'from hailtop.config import get_deploy_config; print(get_deploy_config().base_path("shuffler.shuffler-0"))')

sed -e "s,@base_path@,${base_path:-/},g" \
    < /nginx.conf.in > /etc/nginx/conf.d/gradle-build-cache.conf
