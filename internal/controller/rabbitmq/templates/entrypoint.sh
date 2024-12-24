#!/bin/bash

# In case this was built into the container
rm -rf /cert_rabbitmq/

# redis user (999) needs to own these
cp -R /mummi_operator/ /cert_rabbitmq/
chown -R rabbitmq /cert_rabbitmq/

# Wrap the existing entrypoint (in /usr/local/bin)
exec docker-entrypoint.sh rabbitmq-server $@
