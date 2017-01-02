#!/bin/bash

# Greedy

# copy plan_out.json
cp results/TPCC-high-paired/greedy-ext/1/plan_out.json .
./scripts/deploy-file.sh plan_out.json

# run from plan_out.json
ant hstore-benchmark -Dproject=tpcc -Dglobal.hasher_plan=plan_out.json -Dglobal.hasher_class=edu.brown.hashing.TwoTieredRangeHasher -Dsite.txn_restart_limit_sysproc=100 -Dsite.jvm_asserts=false -Dsite.commandlog_enable=false -Dsite.exec_db2_redirects=false -Dsite.exec_early_prepare=false -Dsite.exec_force_singlepartitioned=true -Dsite.markov_fixed=false -Dsite.planner_caching=false -Dsite.reconfig_async_chunk_size_kb=8048 -Dsite.reconfig_async_delay_ms=511 -Dsite.reconfig_chunk_size_kb=10048 -Dsite.reconfig_plan_delay=1511 -Dsite.reconfig_subplan_split=20 -Dsite.specexec_enable=true -Dpartitionplan=tpcc-plan-fine-grained.pplan  -Dclient.duration=60000 -Dclient.interval=1000 -Dclient.txnrate=1000 -Dclient.count=6 -Dclient.hosts="istc4;istc6;istc7;istc11;istc12;istc13" -Dclient.threads_per_host=16 -Dclient.blocking_concurrent=30 -Dclient.output_interval=true -Dsite.planner_caching=false -Dclient.txn_hints=false  -Dclient.skewfactor=0.65  -Dclient.output_results_csv=results.csv | tee out.log

# save results
./scripts/save_results.sh results/TPCC-high-paired/greedy-ext/1

# Metis-0.1

# copy plan_out.json
cp results/TPCC-high-paired/metis-0.1/1/plan_out.json .
./scripts/deploy-file.sh plan_out.json

# run from plan_out.json
ant hstore-benchmark -Dproject=tpcc -Dglobal.hasher_plan=plan_out.json -Dglobal.hasher_class=edu.brown.hashing.TwoTieredRangeHasher -Dsite.txn_restart_limit_sysproc=100 -Dsite.jvm_asserts=false -Dsite.commandlog_enable=false -Dsite.exec_db2_redirects=false -Dsite.exec_early_prepare=false -Dsite.exec_force_singlepartitioned=true -Dsite.markov_fixed=false -Dsite.planner_caching=false -Dsite.reconfig_async_chunk_size_kb=8048 -Dsite.reconfig_async_delay_ms=511 -Dsite.reconfig_chunk_size_kb=10048 -Dsite.reconfig_plan_delay=1511 -Dsite.reconfig_subplan_split=20 -Dsite.specexec_enable=true -Dpartitionplan=tpcc-plan-fine-grained.pplan  -Dclient.duration=60000 -Dclient.interval=1000 -Dclient.txnrate=1000 -Dclient.count=6 -Dclient.hosts="istc4;istc6;istc7;istc11;istc12;istc13" -Dclient.threads_per_host=16 -Dclient.blocking_concurrent=30 -Dclient.output_interval=true -Dsite.planner_caching=false -Dclient.txn_hints=false  -Dclient.skewfactor=0.65  -Dclient.output_results_csv=results.csv | tee out.log

# save results
./scripts/save_results.sh results/TPCC-high-paired/metis-0.1/1

# Metis-0.5

# copy plan_out.json
cp results/TPCC-high-paired/metis-0.5/plan_out.json .
./scripts/deploy-file.sh plan_out.json

# run from plan_out.json
ant hstore-benchmark -Dproject=tpcc -Dglobal.hasher_plan=plan_out.json -Dglobal.hasher_class=edu.brown.hashing.TwoTieredRangeHasher -Dsite.txn_restart_limit_sysproc=100 -Dsite.jvm_asserts=false -Dsite.commandlog_enable=false -Dsite.exec_db2_redirects=false -Dsite.exec_early_prepare=false -Dsite.exec_force_singlepartitioned=true -Dsite.markov_fixed=false -Dsite.planner_caching=false -Dsite.reconfig_async_chunk_size_kb=8048 -Dsite.reconfig_async_delay_ms=511 -Dsite.reconfig_chunk_size_kb=10048 -Dsite.reconfig_plan_delay=1511 -Dsite.reconfig_subplan_split=20 -Dsite.specexec_enable=true -Dpartitionplan=tpcc-plan-fine-grained.pplan  -Dclient.duration=60000 -Dclient.interval=1000 -Dclient.txnrate=1000 -Dclient.count=6 -Dclient.hosts="istc4;istc6;istc7;istc11;istc12;istc13" -Dclient.threads_per_host=16 -Dclient.blocking_concurrent=30 -Dclient.output_interval=true -Dsite.planner_caching=false -Dclient.txn_hints=false  -Dclient.skewfactor=0.65  -Dclient.output_results_csv=results.csv | tee out.log

# save results
./scripts/save_results.sh results/TPCC-high-paired/metis-0.5

# Metis-1

# copy plan_out.json
cp results/TPCC-high-paired/metis-1/1/plan_out.json .
./scripts/deploy-file.sh plan_out.json

# run from plan_out.json
ant hstore-benchmark -Dproject=tpcc -Dglobal.hasher_plan=plan_out.json -Dglobal.hasher_class=edu.brown.hashing.TwoTieredRangeHasher -Dsite.txn_restart_limit_sysproc=100 -Dsite.jvm_asserts=false -Dsite.commandlog_enable=false -Dsite.exec_db2_redirects=false -Dsite.exec_early_prepare=false -Dsite.exec_force_singlepartitioned=true -Dsite.markov_fixed=false -Dsite.planner_caching=false -Dsite.reconfig_async_chunk_size_kb=8048 -Dsite.reconfig_async_delay_ms=511 -Dsite.reconfig_chunk_size_kb=10048 -Dsite.reconfig_plan_delay=1511 -Dsite.reconfig_subplan_split=20 -Dsite.specexec_enable=true -Dpartitionplan=tpcc-plan-fine-grained.pplan  -Dclient.duration=60000 -Dclient.interval=1000 -Dclient.txnrate=1000 -Dclient.count=6 -Dclient.hosts="istc4;istc6;istc7;istc11;istc12;istc13" -Dclient.threads_per_host=16 -Dclient.blocking_concurrent=30 -Dclient.output_interval=true -Dsite.planner_caching=false -Dclient.txn_hints=false  -Dclient.skewfactor=0.65  -Dclient.output_results_csv=results.csv | tee out.log

# save results
./scripts/save_results.sh results/TPCC-high-paired/metis-1/1

# Metis-1.5

# copy plan_out.json
cp results/TPCC-high-paired/metis-1.5/plan_out.json .
./scripts/deploy-file.sh plan_out.json

# run from plan_out.json
ant hstore-benchmark -Dproject=tpcc -Dglobal.hasher_plan=plan_out.json -Dglobal.hasher_class=edu.brown.hashing.TwoTieredRangeHasher -Dsite.txn_restart_limit_sysproc=100 -Dsite.jvm_asserts=false -Dsite.commandlog_enable=false -Dsite.exec_db2_redirects=false -Dsite.exec_early_prepare=false -Dsite.exec_force_singlepartitioned=true -Dsite.markov_fixed=false -Dsite.planner_caching=false -Dsite.reconfig_async_chunk_size_kb=8048 -Dsite.reconfig_async_delay_ms=511 -Dsite.reconfig_chunk_size_kb=10048 -Dsite.reconfig_plan_delay=1511 -Dsite.reconfig_subplan_split=20 -Dsite.specexec_enable=true -Dpartitionplan=tpcc-plan-fine-grained.pplan  -Dclient.duration=60000 -Dclient.interval=1000 -Dclient.txnrate=1000 -Dclient.count=6 -Dclient.hosts="istc4;istc6;istc7;istc11;istc12;istc13" -Dclient.threads_per_host=16 -Dclient.blocking_concurrent=30 -Dclient.output_interval=true -Dsite.planner_caching=false -Dclient.txn_hints=false  -Dclient.skewfactor=0.65  -Dclient.output_results_csv=results.csv | tee out.log

# save results
./scripts/save_results.sh results/TPCC-high-paired/metis-1.5

# Metis-2

# copy plan_out.json
cp results/TPCC-high-paired/metis-2/1/plan_out.json .
./scripts/deploy-file.sh plan_out.json

# run from plan_out.json
ant hstore-benchmark -Dproject=tpcc -Dglobal.hasher_plan=plan_out.json -Dglobal.hasher_class=edu.brown.hashing.TwoTieredRangeHasher -Dsite.txn_restart_limit_sysproc=100 -Dsite.jvm_asserts=false -Dsite.commandlog_enable=false -Dsite.exec_db2_redirects=false -Dsite.exec_early_prepare=false -Dsite.exec_force_singlepartitioned=true -Dsite.markov_fixed=false -Dsite.planner_caching=false -Dsite.reconfig_async_chunk_size_kb=8048 -Dsite.reconfig_async_delay_ms=511 -Dsite.reconfig_chunk_size_kb=10048 -Dsite.reconfig_plan_delay=1511 -Dsite.reconfig_subplan_split=20 -Dsite.specexec_enable=true -Dpartitionplan=tpcc-plan-fine-grained.pplan  -Dclient.duration=60000 -Dclient.interval=1000 -Dclient.txnrate=1000 -Dclient.count=6 -Dclient.hosts="istc4;istc6;istc7;istc11;istc12;istc13" -Dclient.threads_per_host=16 -Dclient.blocking_concurrent=30 -Dclient.output_interval=true -Dsite.planner_caching=false -Dclient.txn_hints=false  -Dclient.skewfactor=0.65  -Dclient.output_results_csv=results.csv | tee out.log

# save results
./scripts/save_results.sh results/TPCC-high-paired/metis-2/1

# Metis-nobound

# copy plan_out.json
cp results/TPCC-high-paired/metis-nobound/1/plan_out.json .
./scripts/deploy-file.sh plan_out.json

# run from plan_out.json
ant hstore-benchmark -Dproject=tpcc -Dglobal.hasher_plan=plan_out.json -Dglobal.hasher_class=edu.brown.hashing.TwoTieredRangeHasher -Dsite.txn_restart_limit_sysproc=100 -Dsite.jvm_asserts=false -Dsite.commandlog_enable=false -Dsite.exec_db2_redirects=false -Dsite.exec_early_prepare=false -Dsite.exec_force_singlepartitioned=true -Dsite.markov_fixed=false -Dsite.planner_caching=false -Dsite.reconfig_async_chunk_size_kb=8048 -Dsite.reconfig_async_delay_ms=511 -Dsite.reconfig_chunk_size_kb=10048 -Dsite.reconfig_plan_delay=1511 -Dsite.reconfig_subplan_split=20 -Dsite.specexec_enable=true -Dpartitionplan=tpcc-plan-fine-grained.pplan  -Dclient.duration=60000 -Dclient.interval=1000 -Dclient.txnrate=1000 -Dclient.count=6 -Dclient.hosts="istc4;istc6;istc7;istc11;istc12;istc13" -Dclient.threads_per_host=16 -Dclient.blocking_concurrent=30 -Dclient.output_interval=true -Dsite.planner_caching=false -Dclient.txn_hints=false  -Dclient.skewfactor=0.65  -Dclient.output_results_csv=results.csv | tee out.log

# save results
./scripts/save_results.sh results/TPCC-high-paired/metis-nobound/1

