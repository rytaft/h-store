package edu.mit.benchmark.affinity.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

@ProcInfo(
        partitionInfo = "PRODUCTS.PRODUCT_KEY: 0",
        singlePartition = true
    )
public class GetProduct extends VoltProcedure {
//    private static final Logger LOG = Logger.getLogger(VoltProcedure.class);
//    private static final LoggerBoolean debug = new LoggerBoolean();
//    private static final LoggerBoolean trace = new LoggerBoolean();
//    static {
//        LoggerUtil.setupLogging();
//        LoggerUtil.attachObserver(LOG, debug, trace);
//    }
    
    
    public final SQLStmt getProductStmt = new SQLStmt("SELECT * FROM PRODUCTS WHERE PRODUCT_KEY = ? ");
    
    public VoltTable[] run(long product_key){
        voltQueueSQL(getProductStmt, product_key);
        return voltExecuteSQL(true);
    }

}
