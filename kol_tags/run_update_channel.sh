bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin"; pwd`

channel_id=$1
if [ -z $channel_id ]; then
    echo "channel id required!"
    exit 1
fi
python ${bin}/main.py --cmd=update-channel --channel-id=${channel_id}
