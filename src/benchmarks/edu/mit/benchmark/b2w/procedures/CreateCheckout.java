package edu.mit.benchmark.b2w.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;

@ProcInfo(
        partitionInfo = "CART.ID: 0",
        singlePartition = true
    )
public class CreateCheckout extends VoltProcedure {
//    private static final Logger LOG = Logger.getLogger(VoltProcedure.class);
//    private static final LoggerBoolean debug = new LoggerBoolean();
//    private static final LoggerBoolean trace = new LoggerBoolean();
//    static {
//        LoggerUtil.setupLogging();
//        LoggerUtil.attachObserver(LOG, debug, trace);
//    }
    
    public final SQLStmt createCheckoutStmt = new SQLStmt(
            "INSERT INTO CHECKOUT (" +
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
    
    public final SQLStmt createCheckoutPaymentStmt = new SQLStmt(
            "INSERT INTO CHECKOUT_PAYMENTS (" +
                "checkoutId, " +
                "paymentOptionId, " +
                "paymentOptionType, " +
                "dueDays, " +
                "amount, " +
                "installmentQuantity, " +
                "interestAmount, " +
                "interestRate, " +
                "annualCET, " +
                "number, " +
                "criptoNumber, " +
                "holdersName, " +
                "securityCode, " +
                "expirationDate" +
            ") VALUES (" +
                "?, " +   // checkoutId
                "?, " +   // paymentOptionId
                "?, " +   // paymentOptionType
                "?, " +   // dueDays
                "?, " +   // amount
                "?, " +   // installmentQuantity
                "?, " +   // interestAmount
                "?, " +   // interestRate
                "?, " +   // annualCET
                "?, " +   // number
                "?, " +   // criptoNumber
                "?, " +   // holdersName
                "?, " +   // securityCode
                "?"   +   // expirationDate
            ");");
    
    public final SQLStmt createCheckoutFreightDeliveryTimeStmt = new SQLStmt(
            "INSERT INTO CHECKOUT_FREIGHT_DELIVERY_TIME (" +
                "checkoutId, " +
                "lineId, " +
                "deliveryTime" +
            ") VALUES (" +
                "?, " +   // checkoutId
                "?, " +   // lineId
                "?"   +   // deliveryTime
            ");");

    public final SQLStmt createCheckoutStockTxnStmt = new SQLStmt(
            "INSERT INTO CHECKOUT_STOCK_TRANSACTIONS (" +
                "checkoutId, " +
                "id, " +
                "lineId" +
            ") VALUES (" +
                "?, " +   // checkoutId
                "?, " +   // id
                "?"   +   // lineId
            ");");

    public final SQLStmt getCartStmt = new SQLStmt("SELECT total FROM CART WHERE id = ? ");
    public final SQLStmt getCartLinesStmt = new SQLStmt(
            "SELECT id, stockTransactionId FROM CART_LINES WHERE cartId = ?;");
    
    
    public VoltTable[] run(String checkout_id, String cart_id){
        voltQueueSQL(getCartStmt, cart_id);
        voltQueueSQL(getCartLinesStmt, cart_id);
        final VoltTable[] cart_results = voltExecuteSQL();
        assert cart_results.length == 2;
        
        double total = 0;
        
        if (cart_results[0].getRowCount() > 0) {
            final VoltTableRow cart = cart_results[0].fetchRow(0);
            final int TOTAL = 0;
            total = cart.getDouble(TOTAL);
        } else {
            return null;
        }

        String deliveryAddressId = null;
        String billingAddressId = null;
        double amountDue = 0;
        String freightContract = null;
        double freightPrice = 0;
        String freightStatus = null;
        
        total += freightPrice;
        amountDue = total;

        voltQueueSQL(createCheckoutStmt,
                checkout_id,
                cart_id,
                deliveryAddressId,
                billingAddressId,
                amountDue,
                total,
                freightContract,
                freightPrice,
                freightStatus);
        
        for (int i = 0; i < cart_results[1].getRowCount(); ++i) {
            final VoltTableRow cart_line = cart_results[1].fetchRow(i);
            final int LINE_ID = 0, TRANSACTION_ID = 1;
            String line_id = cart_line.getString(LINE_ID);
            String transaction_id = cart_line.getString(TRANSACTION_ID);
            int delivery_time = 0;
            
            voltQueueSQL(createCheckoutFreightDeliveryTimeStmt,
                    checkout_id,
                    line_id,
                    delivery_time);
            
            voltQueueSQL(createCheckoutStockTxnStmt,
                    checkout_id,
                    transaction_id,
                    line_id);           
        }        
        
        return voltExecuteSQL(true);
    }

}
