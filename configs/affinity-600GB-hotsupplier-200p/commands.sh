== PREPARE

ant hstore-prepare -Dproject=affinity -Dhosts="istc3:0:0-19;istc5:1:20-39;istc6:2:40-59;istc7:3:60-79;istc8:4:80-99;istc9:5:100-119;istc10:6:120-139;istc11:7:140-159;istc12:8:160-179;istc13:9:180-199"

== LOAD

ant hstore-benchmark -Dproject=affinity -Dglobal.hasher_plan=plan.json -Dglobal.hasher_class=edu.brown.hashing.TwoTieredRangeHasher -Dnoshutdown=true -Dnoexecute=true -Dsite.txn_restart_limit_sysproc=10000 -Dsite.jvm_asserts=false -Dsite.commandlog_enable=false -Dsite.exec_db2_redirects=false -Dsite.exec_early_prepare=false -Dsite.exec_force_singlepartitioned=true -Dsite.markov_fixed=false -Dsite.planner_caching=false -Dsite.specexec_enable=true -Dsite.memory=256000 | tee out-load.log

== RUN

ant hstore-benchmark -Dproject=affinity -Dglobal.hasher_plan=plan.json -Dglobal.hasher_class=edu.brown.hashing.TwoTieredRangeHasher -Dnostart=true -Dnoloader=true -Dnoshutdown=true -Dclient.duration=60000 -Dclient.interval=1000 -Dclient.txnrate=1000 -Dclient.count=10 -Dclient.hosts="istc3;istc5;istc6;istc7;istc8;istc9;istc10;istc11;istc12;istc13" -Dclient.threads_per_host=12 -Dclient.blocking_concurrent=30 -Dclient.output_results_csv=results.csv -Dclient.output_interval=true -Dsite.planner_caching=false -Dclient.txn_hints=false -Dsite.exec_early_prepare=false -Dclient.output_basepartitions=true | tee out.log

== LOAD from plan_out.json

ant hstore-benchmark -Dproject=affinity -Dglobal.hasher_plan=plan_out.json -Dglobal.hasher_class=edu.brown.hashing.TwoTieredRangeHasher -Dnoshutdown=true -Dnoexecute=true -Dsite.txn_restart_limit_sysproc=10000 -Dsite.jvm_asserts=false -Dsite.commandlog_enable=false -Dsite.exec_db2_redirects=false -Dsite.exec_early_prepare=false -Dsite.exec_force_singlepartitioned=true -Dsite.markov_fixed=false -Dsite.planner_caching=false -Dsite.specexec_enable=true  -Dsite.memory=256000 | tee out-load.log

== RUN from plan_out.json 

ant hstore-benchmark -Dproject=affinity -Dglobal.hasher_plan=plan_out.json -Dglobal.hasher_class=edu.brown.hashing.TwoTieredRangeHasher -Dnostart=true -Dnoloader=true -Dnoshutdown=true -Dclient.duration=60000 -Dclient.interval=1000 -Dclient.txnrate=1000 -Dclient.count=10 -Dclient.hosts="istc3;istc5;istc6;istc7;istc8;istc9;istc10;istc11;istc12;istc13" -Dclient.threads_per_host=12 -Dclient.blocking_concurrent=30 -Dclient.output_results_csv=results.csv -Dclient.output_interval=true -Dsite.planner_caching=false -Dclient.txn_hints=false -Dsite.exec_early_prepare=false -Dclient.memory=4096 -Dclient.output_basepartitions=true | tee out.log


== MONITOR

ant hstore-benchmark -Dproject=affinity -Dglobal.hasher_plan=plan.json -Dglobal.hasher_class=edu.brown.hashing.TwoTieredRangeHasher -Dnostart=true -Dnoloader=true -Dnoshutdown=true -Dclient.interval=1000 -Dclient.txnrate=1000 -Dclient.count=10 -Dclient.hosts="istc3;istc5;istc6;istc7;istc8;istc9;istc10;istc11;istc12;istc13" -Dclient.threads_per_host=12 -Dclient.blocking_concurrent=30 -Dclient.output_results_csv=results.csv -Dclient.output_interval=true -Dsite.planner_caching=false -Dclient.txn_hints=false -Dsite.exec_early_prepare=false -Delastic.run_monitoring=true -Delastic.update_plan=false -Delastic.exec_reconf=false -Delastic.delay=20000 -Dclient.duration=60000 | tee -a out.log

== RECONFIGURE

ant hstore-benchmark -Dproject=affinity -Dglobal.hasher_plan=plan.json -Dglobal.hasher_class=edu.brown.hashing.TwoTieredRangeHasher -Dnoshutdown=true -Dclient.duration=2400000 -Dclient.interval=1000 -Dclient.txnrate=1000 -Dclient.count=10 -Dclient.hosts="istc3;istc5;istc6;istc7;istc8;istc9;istc10;istc11;istc12;istc13" -Dsite.txn_restart_limit_sysproc=10000 -Dclient.threads_per_host=12 -Dclient.blocking_concurrent=30 -Dclient.output_results_csv=results.csv -Dclient.output_interval=true -Dsite.planner_caching=false -Dclient.txn_hints=false -Dsite.exec_early_prepare=false -Dclient.output_basepartitions=true -Delastic.run_monitoring=false -Delastic.update_plan=false -Delastic.exec_reconf=true -Delastic.delay=1200000 -Dclient.memory=4096 -Dsite.memory=256000 | tee out.log

== RECONFIGURE NO LOAD

ant hstore-benchmark -Dproject=affinity -Dglobal.hasher_plan=plan.json -Dglobal.hasher_class=edu.brown.hashing.TwoTieredRangeHasher -Dnostart=true -Dnoloader=true -Dnoshutdown=true -Dclient.duration=600000 -Dclient.interval=1000 -Dclient.txnrate=1000 -Dclient.count=10 -Dclient.hosts="istc3;istc5;istc6;istc7;istc8;istc9;istc10;istc11;istc12;istc13" -Dclient.threads_per_host=12 -Dclient.blocking_concurrent=30 -Dclient.output_results_csv=results.csv -Dclient.output_interval=true -Dsite.planner_caching=false -Dclient.txn_hints=false -Dsite.exec_early_prepare=false -Dclient.output_basepartitions=true -Delastic.run_monitoring=false -Delastic.update_plan=false -Delastic.exec_reconf=true -Delastic.delay=300000 -Dclient.memory=4096 | tee out.log
