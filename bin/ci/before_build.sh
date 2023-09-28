#!/bin/sh

CTL=${OMQ_RABBITMQCTL:="rabbitmqctl"}
PLUGINS=${OMQ_RABBITMQ_PLUGINS:="rabbitmq-plugins"}

case $CTL in
        DOCKER*)
          PLUGINS="docker exec ${CTL##*:} rabbitmq-plugins"
          CTL="docker exec ${CTL##*:} rabbitmqctl";;
esac

echo "Will use rabbitmqctl at ${CTL}"
echo "Will use rabbitmq-plugins at ${PLUGINS}"

$PLUGINS enable rabbitmq_management

sleep 3

# guest:guest has full access to /

$CTL add_vhost /
$CTL add_user guest guest
$CTL set_permissions -p / guest ".*" ".*" ".*"

# set cluster name
$CTL set_cluster_name rabbitmq@localhost

$CTL enable_feature_flag all

# Enable shovel plugin
$PLUGINS enable rabbitmq_prometheus rabbitmq_amqp1_0 rabbitmq_mqtt rabbitmq_stomp

true
