package edu.mit.benchmark.b2w.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

@ProcInfo(
        partitionInfo = "STK_STOCK_TRANSACTION.TRANSACTION_ID: 0",
        singlePartition = true
    )
public class GetStockTransaction extends VoltProcedure {
//    private static final Logger LOG = Logger.getLogger(VoltProcedure.class);
//    private static final LoggerBoolean debug = new LoggerBoolean();
//    private static final LoggerBoolean trace = new LoggerBoolean();
//    static {
//        LoggerUtil.setupLogging();
//        LoggerUtil.attachObserver(LOG, debug, trace);
//    }
        
    public final SQLStmt getStockTxnStmt = new SQLStmt("SELECT * FROM STK_STOCK_TRANSACTION WHERE transaction_id = ? ");
        
    public VoltTable[] run(String transaction_id){
        voltQueueSQL(getStockTxnStmt, transaction_id);
        
        return voltExecuteSQL(true);
    }

}
