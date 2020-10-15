#!/bin/sh

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin"; pwd`

#language='en'
#language='zh-Hans'
#language='zh-Hant'

nohup sh $bin/run_train_pipeline.sh 'en' &
nohup sh $bin/run_train_pipeline.sh 'zh-Hans' &
nohup sh $bin/run_train_pipeline.sh 'zh-Hant' &
