ant hstore-benchmark -Dproject=tpcc -Dnosites=true -Dnoexecute=true
ant hstore-invoke -Dproject=tpcc -Dproc=@Reconfiguration -Dparam0=0 -Dparam1=2 -Dparam2=livepull
