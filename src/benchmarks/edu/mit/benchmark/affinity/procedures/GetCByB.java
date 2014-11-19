package edu.mit.benchmark.affinity.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

@ProcInfo(
        partitionInfo = "TABLEBCMAP.B_KEY: 0",
        singlePartition = true
    )
public class GetCByB extends VoltProcedure {
//    private static final Logger LOG = Logger.getLogger(VoltProcedure.class);
//    private static final LoggerBoolean debug = new LoggerBoolean();
//    private static final LoggerBoolean trace = new LoggerBoolean();
//    static {
//        LoggerUtil.setupLogging();
//        LoggerUtil.attachObserver(LOG, debug, trace);
//    }
    
    
    //public final SQLStmt getCbyBStmt = new SQLStmt("SELECT * FROM TABLEC WHERE B_KEY = ? ");
    public final SQLStmt getCbyBStmt = new SQLStmt("SELECT C_KEY FROM TABLEBCMAP WHERE B_KEY = ? ");
    
    public VoltTable[] run(long b_key){
        voltQueueSQL(getCbyBStmt, b_key);
        return (voltExecuteSQL());
    }

}
