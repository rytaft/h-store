package edu.mit.benchmark.b2w.procedures;

import java.util.Arrays;

import org.apache.log4j.Logger;
import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.mit.benchmark.b2w.B2WUtil;

@ProcInfo(
        partitionInfo = "CHECKOUT.partition_key: 0",
        singlePartition = true
    )
public class CreateCheckout extends VoltProcedure {
    private static final Logger LOG = Logger.getLogger(VoltProcedure.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.setupLogging();
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    public final SQLStmt createCheckoutStmt = new SQLStmt(
            "INSERT INTO CHECKOUT (" +
                "partition_key, " +
                "id, " +
                "cartId, " +
                "deliveryAddressId, " +
                "billingAddressId, " +
                "amountDue, " +
                "total, " +
                "freightContract, " +
                "freightPrice, " +
                "freightStatus" +
            ") VALUES (" +
                "?, " +   // partition_key
                "?, " +   // id
                "?, " +   // cartId
                "?, " +   // deliveryAddressId
                "?, " +   // billingAddressId
                "?, " +   // amountDue
                "?, " +   // total
                "?, " +   // freightContract
                "?, " +   // freightPrice
                "?"   +   // freightStatus
            ");");
        
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
    
    public final SQLStmt getCheckoutStmt = new SQLStmt("SELECT * FROM CHECKOUT WHERE partition_key = ? AND id = ?;");
    
    public VoltTable[] run(int partition_key, String checkout_id, String cart_id, String deliveryAddressId, String billingAddressId,
            double amountDue, double total, String freightContract, double freightPrice, String freightStatus,
            String[] line_id, String[] transaction_id, int[] delivery_time, long sleep_time) {
        B2WUtil.sleep(sleep_time);
        
        voltQueueSQL(getCheckoutStmt, partition_key, checkout_id);
        final VoltTable[] checkout_results = voltExecuteSQL();
        assert checkout_results.length == 1;

        if (checkout_results[0].getRowCount() > 0) {
            return null; // checkout already exists
        } 

        if (trace.val) {
            LOG.trace("Creating checkout with params: " + checkout_id + ", " + cart_id + ", " + deliveryAddressId + ", " + billingAddressId + ", " +
                    amountDue + ", " + total + ", " + freightContract + ", " + freightPrice + ", " + freightStatus + ", " +
                    Arrays.asList(line_id).toString() + ", " + Arrays.asList(transaction_id).toString() + ", " + Arrays.asList(delivery_time).toString());
        }
        voltQueueSQL(createCheckoutStmt,
                partition_key,
                checkout_id,
                cart_id,
                deliveryAddressId,
                billingAddressId,
                amountDue,
                total,
                freightContract,
                freightPrice,
                freightStatus);

        assert(line_id.length == transaction_id.length);
        assert(line_id.length == delivery_time.length);

        for (int i = 0; i < line_id.length; ++i) {
            if(line_id[i] == null) continue;

            if(transaction_id[i] != null) {
                voltQueueSQL(createCheckoutStockTxnStmt,
                        partition_key,
                        checkout_id,
                        transaction_id[i],
                        line_id[i]);           
            }

            if(delivery_time[i] != VoltType.NULL_INTEGER) {                
                voltQueueSQL(createCheckoutFreightDeliveryTimeStmt,
                        partition_key,
                        checkout_id,
                        line_id[i],
                        delivery_time[i]);
            }        
        }
        
        return voltExecuteSQL(true);
    }

}
