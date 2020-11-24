#!/bin/sh

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin"; pwd`

language='en'
##language='zh-Hans'
#language='zh-Hant'
#language='ko'

nohup sh $bin/run_train_pipeline.sh $language &
