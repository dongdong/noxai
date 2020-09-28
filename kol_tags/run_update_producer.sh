bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin"; pwd`

date_str=`date "+%Y%m%d%H%M%S"`
log_path="${bin}/logs/write_id_to_redis_${date_str}.log"

echo "start producer"
nohup python ${bin}/redis_utils.py > $log_path 2>&1 &
sleep 1
tail -f $log_path
