#!/bin/bash

servers="istc13:0:0-5;istc12:1:6-11"
clients="istc9;istc5;istc11;istc10"
client_count=4
Q=1
#Q=3000
# alt: change threads_per_host to 11 and txnrate to $Q and warmup to 60000
# change threads_per_host to 16 and txnrate to -1 and warmup to 0

case $1 in 
## PREPARE
    "prepare")
	ant hstore-prepare -Dproject=ycsb -Dhosts="$servers"
	;;

## LOAD
    "load")
	ant hstore-benchmark -Dproject=ycsb -Dglobal.hasher_plan=plan.json -Dglobal.hasher_class=edu.brown.hashing.TwoTieredRangeHasher -Dnoshutdown=true -Dnoexecute=true -Dsite.txn_restart_limit_sysproc=100 -Dsite.jvm_asserts=false -Dsite.commandlog_enable=false -Dsite.exec_db2_redirects=false -Dsite.exec_early_prepare=false -Dsite.exec_force_singlepartitioned=true -Dsite.markov_fixed=false -Dsite.planner_caching=false -Dsite.specexec_enable=true -Dsite.reconfig_async_chunk_size_kb=125 -Dsite.reconfig_chunk_size_kb=125 -Dsite.reconfig_async_delay_ms=0 -Dsite.reconfig_auto_subplan_split=false -Dsite.reconfig_subplan_split=24006 -Dsite.reconfig_plan_delay=0 -Dclient.tick_interval=100 -Dsite.queue_profiling=true | tee out-load.log
	;;
## RUN
    "run")
	ant hstore-benchmark -Dproject=ycsb -Dglobal.hasher_plan=plan.json -Dglobal.hasher_class=edu.brown.hashing.TwoTieredRangeHasher -Dnostart=true -Dnoloader=true -Dnoshutdown=true -Dclient.duration=600000 -Dclient.interval=1000 -Dclient.txnrate=${Q} -Dclient.count=$client_count -Dclient.hosts="$clients" -Dclient.threads_per_host=16 -Dclient.blocking_concurrent=1000 -Dclient.output_results_csv=results.csv -Dclient.output_interval=true -Dsite.planner_caching=false -Dclient.txn_hints=false -Dsite.exec_early_prepare=false -Dclient.output_basepartitions=true -Dclient.warmup=30000 -Dclient.tick_interval=100 | tee out.log
	;;

## LOAD from plan_out.json
    "load_from_plan_out")
	ant hstore-benchmark -Dproject=ycsb -Dglobal.hasher_plan=plan_out.json -Dglobal.hasher_class=edu.brown.hashing.TwoTieredRangeHasher -Dnoshutdown=true -Dnoexecute=true -Dsite.txn_restart_limit_sysproc=100 -Dsite.jvm_asserts=false -Dsite.commandlog_enable=false -Dsite.exec_db2_redirects=false -Dsite.exec_early_prepare=false -Dsite.exec_force_singlepartitioned=true -Dsite.markov_fixed=false -Dsite.planner_caching=false -Dsite.specexec_enable=true -Dclient.tick_interval=100 | tee out-load.log
	;;

## RUN from plan_out.json 
    "run_from_plan_out")
	ant hstore-benchmark -Dproject=ycsb -Dglobal.hasher_plan=plan_out.json -Dglobal.hasher_class=edu.brown.hashing.TwoTieredRangeHasher -Dnostart=true -Dnoloader=true -Dnoshutdown=true -Dclient.duration=60000 -Dclient.interval=1000 -Dclient.txnrate=${Q} -Dclient.count=$client_count -Dclient.hosts="$clients" -Dclient.threads_per_host=16 -Dclient.blocking_concurrent=1000 -Dclient.output_results_csv=results.csv -Dclient.output_interval=true -Dsite.planner_caching=false -Dclient.txn_hints=false -Dsite.exec_early_prepare=false -Dclient.output_basepartitions=true -Dclient.tick_interval=100 | tee out.log
;;

## MONITOR
    "monitor")
	ant hstore-benchmark -Dproject=ycsb -Dglobal.hasher_plan=plan.json -Dglobal.hasher_class=edu.brown.hashing.TwoTieredRangeHasher -Dnostart=true -Dnoloader=true -Dnoshutdown=true -Dclient.interval=1000 -Dclient.txnrate=${Q} -Dclient.count=$client_count -Dclient.hosts="$clients" -Dclient.threads_per_host=16 -Dclient.blocking_concurrent=1000 -Dclient.output_results_csv=results.csv -Dclient.output_interval=true -Dsite.planner_caching=false -Dclient.txn_hints=false -Dsite.exec_early_prepare=false -Delastic.run_monitoring=true -Delastic.update_plan=false -Delastic.exec_reconf=false -Delastic.delay=20000 -Dclient.duration=60000 -Dclient.tick_interval=100 | tee -a out.log
	;;

## RECONFIGURE
    "reconfig")
	ant hstore-benchmark -Dproject=ycsb -Dglobal.hasher_plan=plan.json -Dglobal.hasher_class=edu.brown.hashing.TwoTieredRangeHasher -Dnoshutdown=true -Dclient.duration=600000 -Dclient.interval=1000 -Dclient.txnrate=${Q} -Dclient.count=$client_count -Dclient.hosts="$clients" -Dclient.threads_per_host=16 -Dclient.blocking_concurrent=1000 -Dclient.output_results_csv=results.csv -Dclient.output_interval=true -Dsite.planner_caching=false -Dclient.txn_hints=false -Dsite.exec_early_prepare=false -Dclient.output_basepartitions=true -Delastic.run_monitoring=false -Delastic.update_plan=false -Delastic.exec_reconf=true -Delastic.delay=50000 -Dclient.warmup=30000 -Dsite.reconfig_async_chunk_size_kb=125 -Dsite.reconfig_chunk_size_kb=125 -Dsite.reconfig_async_delay_ms=0 -Dsite.reconfig_auto_subplan_split=false -Dsite.reconfig_subplan_split=24006 -Dsite.reconfig_plan_delay=0 -Dclient.tick_interval=100 | tee out.log
	;;

## RECONFIGURE NO LOAD
    "reconfig_no_load")
	ant hstore-benchmark -Dproject=ycsb -Dglobal.hasher_plan=plan.json -Dglobal.hasher_class=edu.brown.hashing.TwoTieredRangeHasher -Dnostart=true -Dnoloader=true -Dnoshutdown=true -Dclient.duration=600000 -Dclient.interval=1000 -Dclient.txnrate=${Q} -Dclient.count=$client_count -Dclient.hosts="$clients" -Dclient.threads_per_host=16 -Dclient.blocking_concurrent=1000 -Dclient.output_results_csv=results.csv -Dclient.output_interval=true -Dsite.planner_caching=false -Dclient.txn_hints=false -Dsite.exec_early_prepare=false -Dclient.output_basepartitions=true -Delastic.run_monitoring=false -Delastic.update_plan=false -Delastic.exec_reconf=true -Delastic.delay=30000 -Dclient.warmup=30000 -Dclient.tick_interval=100 | tee out.log
	;;
esac