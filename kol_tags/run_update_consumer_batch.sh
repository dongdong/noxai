bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin"; pwd`


date_str=`date "+%Y%m%d%H%M%S"`
num_processors=$1
if [ -z "$num_processors" ]; then
    num_processors=4
fi
echo "num jobs: $num_processors"

for processor_id in $(seq 1 $num_processors) 
do
    echo "start consumer $processor_id"
    log_path="${bin}/logs/update_batch_${date_str}_${processor_id}.log"
    nohup python ${bin}/main.py --cmd=update-channels-from-pipe > $log_path 2>&1 &
    sleep 1
done
