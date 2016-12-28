package edu.mit.benchmark.b2w_sku_key.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import edu.mit.benchmark.b2w.B2WUtil;

@ProcInfo(
        partitionInfo = "CHECKOUT.partition_key: 0",
        singlePartition = true
    )
public class DeleteCheckout extends VoltProcedure {
//    private static final Logger LOG = Logger.getLogger(VoltProcedure.class);
//    private static final LoggerBoolean debug = new LoggerBoolean();
//    private static final LoggerBoolean trace = new LoggerBoolean();
//    static {
//        LoggerUtil.setupLogging();
//        LoggerUtil.attachObserver(LOG, debug, trace);
//    }
    
    public final SQLStmt deleteCheckoutStmt = new SQLStmt(
            "DELETE FROM CHECKOUT WHERE partition_key = ? AND id = ? ");
    
    public final SQLStmt deleteCheckoutPaymentsStmt = new SQLStmt(
            "DELETE FROM CHECKOUT_PAYMENTS WHERE partition_key = ? AND checkoutId = ? ");
    
    public final SQLStmt deleteCheckoutFreightDeliveryTimeStmt = new SQLStmt(
            "DELETE FROM CHECKOUT_FREIGHT_DELIVERY_TIME WHERE partition_key = ? AND checkoutId = ? ");
    
    public final SQLStmt deleteCheckoutStockTransactionsStmt = new SQLStmt(
            "DELETE FROM CHECKOUT_STOCK_TRANSACTIONS WHERE partition_key = ? AND checkoutId = ? ");

    public VoltTable[] run(int partition_key, String checkout_id, long sleep_time){
        B2WUtil.sleep(sleep_time);
        
        voltQueueSQL(deleteCheckoutPaymentsStmt, partition_key, checkout_id);
        voltQueueSQL(deleteCheckoutFreightDeliveryTimeStmt, partition_key, checkout_id);
        voltQueueSQL(deleteCheckoutStockTransactionsStmt, partition_key, checkout_id);
        voltQueueSQL(deleteCheckoutStmt, partition_key, checkout_id);
        
        return voltExecuteSQL(true);
    }

}
