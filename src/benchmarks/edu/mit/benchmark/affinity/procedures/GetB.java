package edu.mit.benchmark.affinity.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

@ProcInfo(
        partitionInfo = "TABLEB.B_KEY: 0",
        singlePartition = true
    )
public class GetB extends VoltProcedure {
//    private static final Logger LOG = Logger.getLogger(VoltProcedure.class);
//    private static final LoggerBoolean debug = new LoggerBoolean();
//    private static final LoggerBoolean trace = new LoggerBoolean();
//    static {
//        LoggerUtil.setupLogging();
//        LoggerUtil.attachObserver(LOG, debug, trace);
//    }
    
    
    public final SQLStmt getBStmt = new SQLStmt("SELECT * FROM TABLEB WHERE B_KEY = ? ");
    
    public VoltTable[] run(long b_key){
        voltQueueSQL(getBStmt, b_key);
        return (voltExecuteSQL());
    }

}
