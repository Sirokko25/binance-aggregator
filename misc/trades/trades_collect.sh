#!/bin/bash
/usr/bin/flock -n /var/lock/trades_collect.lock -c '
    cd $REPO_DIR || {
        echo "Failed to change directory" >> /var/log/trades_collect.error.log
        exit 1
    }
    task run-trades || {
        echo "Trades collector task failed" >> /var/log/trades_collect.error.log
        exit 1
    }
' 2>&1 | tee /var/log/trades_collect.log