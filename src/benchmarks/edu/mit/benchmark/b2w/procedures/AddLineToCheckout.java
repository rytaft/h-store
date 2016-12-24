package edu.mit.benchmark.b2w.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;

@ProcInfo(
        partitionInfo = "CHECKOUT.partition_key: 0",
        singlePartition = true
    )
public class AddLineToCheckout extends VoltProcedure {
//    private static final Logger LOG = Logger.getLogger(VoltProcedure.class);
//    private static final LoggerBoolean debug = new LoggerBoolean();
//    private static final LoggerBoolean trace = new LoggerBoolean();
//    static {
//        LoggerUtil.setupLogging();
//        LoggerUtil.attachObserver(LOG, debug, trace);
//    }
        
    public final SQLStmt createCheckoutFreightDeliveryTimeStmt = new SQLStmt(
            "INSERT INTO CHECKOUT_FREIGHT_DELIVERY_TIME (" +
                "partition_key, " +
                "checkoutId, " +
                "lineId, " +
                "deliveryTime" +
            ") VALUES (" +
                "?, " +   // partition_key
                "?, " +   // checkoutId
                "?, " +   // lineId
                "?"   +   // deliveryTime
            ");");

    public final SQLStmt createCheckoutStockTxnStmt = new SQLStmt(
            "INSERT INTO CHECKOUT_STOCK_TRANSACTIONS (" +
                "partition_key, " +
                "checkoutId, " +
                "id, " +
                "lineId" +
            ") VALUES (" +
                "?, " +   // partition_key
                "?, " +   // checkoutId
                "?, " +   // id
                "?"   +   // lineId
            ");");   

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
    
    public VoltTable[] run(int partition_key, String checkout_id, String line_id, double salesPrice, String transaction_id, int delivery_time,
            String freightContract, double freightPrice, String freightStatus, long sleep_time) {
        try {
            Thread.sleep(sleep_time);
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

        voltQueueSQL(createCheckoutStockTxnStmt,
                partition_key,
                checkout_id,
                transaction_id,
                line_id);           

        if(delivery_time != VoltType.NULL_INTEGER) {                
            voltQueueSQL(createCheckoutFreightDeliveryTimeStmt,
                    partition_key,
                    checkout_id,
                    line_id,
                    delivery_time);
        }
        
        amountDue = amountDue + salesPrice - oldFreightPrice + freightPrice; // TODO: When would amountDue be different from total? Do we always add salesPrice here?
        total = total + salesPrice - oldFreightPrice + freightPrice;
        
        voltQueueSQL(updateCheckoutStmt, amountDue, total, freightContract, freightPrice, freightStatus, partition_key, checkout_id);
        
        return voltExecuteSQL(true);
    }

}
