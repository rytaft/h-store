package edu.mit.benchmark.b2w.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

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
            "DELETE FROM CHECKOUT WHERE id = ? ");
    
    public final SQLStmt deleteCheckoutPaymentsStmt = new SQLStmt(
            "DELETE FROM CHECKOUT_PAYMENTS WHERE checkoutId = ? ");
    
    public final SQLStmt deleteCheckoutFreightDeliveryTimeStmt = new SQLStmt(
            "DELETE FROM CHECKOUT_FREIGHT_DELIVERY_TIME WHERE checkoutId = ? ");
    
    public final SQLStmt deleteCheckoutStockTransactionsStmt = new SQLStmt(
            "DELETE FROM CHECKOUT_STOCK_TRANSACTIONS WHERE checkoutId = ? ");

    public VoltTable[] run(Integer partition_key, String checkout_id){
        voltQueueSQL(deleteCheckoutPaymentsStmt, checkout_id);
        voltQueueSQL(deleteCheckoutFreightDeliveryTimeStmt, checkout_id);
        voltQueueSQL(deleteCheckoutStockTransactionsStmt, checkout_id);
        voltQueueSQL(deleteCheckoutStmt, checkout_id);
        
        return voltExecuteSQL(true);
    }

}
