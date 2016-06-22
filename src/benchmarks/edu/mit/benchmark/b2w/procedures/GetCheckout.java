package edu.mit.benchmark.b2w.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

@ProcInfo(
        partitionInfo = "CART.ID: 0",
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
            "SELECT * FROM CHECKOUT WHERE id = ? ");
    
    public final SQLStmt getCheckoutPaymentsStmt = new SQLStmt(
            "SELECT * FROM CHECKOUT_PAYMENTS WHERE checkoutId = ? ");
    
    public final SQLStmt getCheckoutFreightDeliveryTimeStmt = new SQLStmt(
            "SELECT * FROM CHECKOUT_FREIGHT_DELIVERY_TIME WHERE checkoutId = ? ");
    
    public final SQLStmt getCheckoutStockTransactionsStmt = new SQLStmt(
            "SELECT * FROM CHECKOUT_STOCK_TRANSACTIONS WHERE checkoutId = ? ");

    public VoltTable[] run(String checkout_id){
        voltQueueSQL(getCheckoutStmt, checkout_id);
        voltQueueSQL(getCheckoutPaymentsStmt, checkout_id);
        voltQueueSQL(getCheckoutFreightDeliveryTimeStmt, checkout_id);
        voltQueueSQL(getCheckoutStockTransactionsStmt, checkout_id);
        
        return voltExecuteSQL(true);
    }

}
