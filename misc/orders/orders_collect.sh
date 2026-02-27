#!/bin/bash
/usr/bin/flock -n /var/lock/orders_collect.lock -c '
    cd $REPO_DIR || {
        echo "Failed to change directory" >> /var/log/orders_collect.error.log
        exit 1
    }
    task run-orders || {
        echo "Orders collector task failed" >> /var/log/orders_collect.error.log
        exit 1
    }
' 2>&1 | tee /var/log/orders_collect.log
