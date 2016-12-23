package edu.mit.benchmark.b2w.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import edu.mit.benchmark.b2w.B2WConfig;

@ProcInfo(
        partitionInfo = "STK_STOCK_TRANSACTION.partition_key: 0",
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
        
    public final SQLStmt getStockTxnStmt = new SQLStmt("SELECT * FROM STK_STOCK_TRANSACTION WHERE partition_key = ? AND transaction_id = ? ");
        
    public VoltTable[] run(int partition_key, String transaction_id){
        try {
            Thread.sleep(B2WConfig.sleep_time);
        } catch(InterruptedException e) {
            // do nothing
        }
        
        voltQueueSQL(getStockTxnStmt, partition_key, transaction_id);
        
        return voltExecuteSQL(true);
    }

}
