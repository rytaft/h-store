package edu.mit.benchmark.affinity.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

@ProcInfo(
        partitionInfo = "TABLEACMAP.A_KEY: 0",
        singlePartition = true
    )
public class GetCByA extends VoltProcedure {
//    private static final Logger LOG = Logger.getLogger(VoltProcedure.class);
//    private static final LoggerBoolean debug = new LoggerBoolean();
//    private static final LoggerBoolean trace = new LoggerBoolean();
//    static {
//        LoggerUtil.setupLogging();
//        LoggerUtil.attachObserver(LOG, debug, trace);
//    }
    
    
    //public final SQLStmt getCbyAStmt = new SQLStmt("SELECT * FROM TABLEC WHERE A_KEY = ? ");
    public final SQLStmt getCbyAStmt = new SQLStmt("SELECT C_KEY FROM TABLEACMAP WHERE A_KEY = ? ");
    
    
    public VoltTable[] run(long a_key){
        voltQueueSQL(getCbyAStmt, a_key);
        return (voltExecuteSQL());
    }

}
