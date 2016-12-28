package edu.mit.benchmark.b2w.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import edu.mit.benchmark.b2w.B2WUtil;

@ProcInfo(
        partitionInfo = "CHECKOUT.partition_key: 0",
        singlePartition = true
    )
public class GetCheckout extends VoltProcedure {
//    private static final Logger LOG = Logger.getLogger(VoltProcedure.class);
//    private static final LoggerBoolean debug = new LoggerBoolean();
//    private static final LoggerBoolean trace = new LoggerBoolean();
//    static {
//        LoggerUtil.setupLogging();
//        LoggerUtil.attachObserver(LOG, debug, trace);
//    }
    
    public final SQLStmt getCheckoutStmt = new SQLStmt(
            "SELECT * FROM CHECKOUT WHERE partition_key = ? AND id = ? ");
    
    public final SQLStmt getCheckoutPaymentsStmt = new SQLStmt(
            "SELECT * FROM CHECKOUT_PAYMENTS WHERE partition_key = ? AND checkoutId = ? ");
    
    public final SQLStmt getCheckoutFreightDeliveryTimeStmt = new SQLStmt(
            "SELECT * FROM CHECKOUT_FREIGHT_DELIVERY_TIME WHERE partition_key = ? AND checkoutId = ? ");
    
    public final SQLStmt getCheckoutStockTransactionsStmt = new SQLStmt(
            "SELECT * FROM CHECKOUT_STOCK_TRANSACTIONS WHERE partition_key = ? AND checkoutId = ? ");

    public VoltTable[] run(int partition_key, String checkout_id, long sleep_time){
        B2WUtil.sleep(sleep_time);
        
        voltQueueSQL(getCheckoutStmt, partition_key, checkout_id);
        voltQueueSQL(getCheckoutPaymentsStmt, partition_key, checkout_id);
        voltQueueSQL(getCheckoutFreightDeliveryTimeStmt, partition_key, checkout_id);
        voltQueueSQL(getCheckoutStockTransactionsStmt, partition_key, checkout_id);
        
        return voltExecuteSQL(true);
    }

}
