bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin"; pwd`


date_str=`date "+%Y%m%d%H%M%S"`

#producer_log_path="${bin}/logs/write_id_to_redis_${date_str}.log"
#echo "start producer"
#python ${bin}/redis_utils.py > $log_path 2>&1 &
#sleep 3

num_processors=4
for processor_id in $(seq 1 $num_processors) 
do
    echo "start consumer $processor_id"
    log_path="${bin}/logs/update_batch_${date_str}_${processor_id}.log"
    nohup python ${bin}/update_tags.py > $log_path 2>&1 &
    sleep 1
done
