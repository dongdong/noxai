#!/bin/sh

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin"; pwd`

language="$1"
date_str=`date "+%Y%m%d%H%M%S"`
log_path="${bin}/logs/train_pipeline_${language}_${date_str}.log"

if [ -z '$language' ]; then
    echo 'Usage: run_train_pipeline.sh language'
    exit 1
fi

cmd="python ${bin}/train_pipeline.py --language $language --date $date_str"

$cmd > $log_path 2>&1
