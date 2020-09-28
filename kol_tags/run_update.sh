#!/bin/sh

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin"; pwd`

date_str=`date "+%Y%m%d%H%M%S"`
log_path="${bin}/logs/update_${date_str}.log"

python ${bin}/update_tags.py > $log_path 2>&1
