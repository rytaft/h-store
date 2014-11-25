package edu.mit.benchmark.affinity.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

@ProcInfo(
        partitionInfo = "PARTS.PART_KEY: 0",
        singlePartition = true
    )
public class GetPart extends VoltProcedure {
//    private static final Logger LOG = Logger.getLogger(VoltProcedure.class);
//    private static final LoggerBoolean debug = new LoggerBoolean();
//    private static final LoggerBoolean trace = new LoggerBoolean();
//    static {
//        LoggerUtil.setupLogging();
//        LoggerUtil.attachObserver(LOG, debug, trace);
//    }
    
    
    public final SQLStmt getPartStmt = new SQLStmt("SELECT * FROM PARTS WHERE PART_KEY = ? ");
    
    public VoltTable[] run(long part_key){
        voltQueueSQL(getPartStmt, part_key);
        return voltExecuteSQL(true);
    }

}
