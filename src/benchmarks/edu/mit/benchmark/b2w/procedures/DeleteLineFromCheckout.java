package edu.mit.benchmark.b2w.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;

import edu.mit.benchmark.b2w.B2WConfig;

@ProcInfo(
        partitionInfo = "CHECKOUT.partition_key: 0",
        singlePartition = true
    )
public class DeleteLineFromCheckout extends VoltProcedure {
//    private static final Logger LOG = Logger.getLogger(VoltProcedure.class);
//    private static final LoggerBoolean debug = new LoggerBoolean();
//    private static final LoggerBoolean trace = new LoggerBoolean();
//    static {
//        LoggerUtil.setupLogging();
//        LoggerUtil.attachObserver(LOG, debug, trace);
//    }
    
    public final SQLStmt deleteCheckoutFreightDeliveryTimeStmt = new SQLStmt(
            "DELETE FROM CHECKOUT_FREIGHT_DELIVERY_TIME WHERE partition_key = ? AND checkoutId = ? AND lineId = ?;");

    public final SQLStmt deleteCheckoutStockTransactionStmt = new SQLStmt(
            "DELETE FROM CHECKOUT_STOCK_TRANSACTIONS WHERE partition_key = ? AND checkoutId = ? AND lineId = ?;");

    public final SQLStmt getCheckoutStmt = new SQLStmt("SELECT amountDue, total, freightPrice FROM CHECKOUT WHERE partition_key = ? AND id = ?;");
    
    public final SQLStmt updateCheckoutStmt = new SQLStmt(
            "UPDATE CHECKOUT " +
            "   SET amountDue = ?, " +
            "       total = ?, " +
            "       freightContract = ?, " +
            "       freightPrice = ?, " +
            "       freightStatus = ? " +
            " WHERE partition_key = ? AND id = ?;"
        ); //amountDue, total, freightContract, freightPrice, freightStatus, partition_key, id


    public VoltTable[] run(int partition_key, String checkout_id, String line_id, double salesPrice, String freightContract, double freightPrice, String freightStatus){
        try {
            Thread.sleep(B2WConfig.sleep_time);
        } catch(InterruptedException e) {
            // do nothing
        }
        
        voltQueueSQL(getCheckoutStmt, partition_key, checkout_id);
        final VoltTable[] checkout_results = voltExecuteSQL();
        assert checkout_results.length == 1;
        
        double amountDue = 0;
        double total = 0;        
        double oldFreightPrice = 0;

        if (checkout_results[0].getRowCount() > 0) {
            final VoltTableRow checkout = checkout_results[0].fetchRow(0);
            final int AMOUNT_DUE = 0, TOTAL = 1, FREIGHT_PRICE = 2;
            amountDue = checkout.getDouble(AMOUNT_DUE);
            total = checkout.getDouble(TOTAL);
            oldFreightPrice = checkout.getDouble(FREIGHT_PRICE);
        } else {
            return null;
        }      
        
        voltQueueSQL(deleteCheckoutFreightDeliveryTimeStmt, partition_key, checkout_id, line_id);        
        voltQueueSQL(deleteCheckoutStockTransactionStmt, partition_key, checkout_id, line_id);        
        
        amountDue = amountDue - salesPrice - oldFreightPrice + freightPrice; // TODO: When would amountDue be different from total? Do we always subtract salesPrice here?
        total = total - salesPrice - oldFreightPrice + freightPrice;
        
        voltQueueSQL(updateCheckoutStmt, amountDue, total, freightContract, freightPrice, freightStatus, partition_key, checkout_id);
        
        return voltExecuteSQL(true);
    }

}
