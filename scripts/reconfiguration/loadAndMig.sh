ant hstore-benchmark -Dproject=tpcc -Dnosites=true -Dnoexecute=true -Dglobal.hasher_plan=test.json -Dglobal.hasherClass=edu.brown.hashing.PlannedHasher
ant hstore-invoke -Dproject=tpcc -Dproc=@Reconfiguration -Dparam0=0 -Dparam1=2 -Dparam2=livepull
