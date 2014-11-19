package edu.mit.benchmark.affinity.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

@ProcInfo(
        partitionInfo = "TABLEA.A_KEY: 0",
        singlePartition = true
    )
public class GetA extends VoltProcedure {
//    private static final Logger LOG = Logger.getLogger(VoltProcedure.class);
//    private static final LoggerBoolean debug = new LoggerBoolean();
//    private static final LoggerBoolean trace = new LoggerBoolean();
//    static {
//        LoggerUtil.setupLogging();
//        LoggerUtil.attachObserver(LOG, debug, trace);
//    }
    
    
    public final SQLStmt getAStmt = new SQLStmt("SELECT * FROM TABLEA WHERE A_KEY = ? ");
    
    public VoltTable[] run(long a_key){
        voltQueueSQL(getAStmt, a_key);
        return (voltExecuteSQL());
    }

}
