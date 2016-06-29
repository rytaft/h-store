package edu.mit.benchmark.b2w.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import edu.mit.benchmark.b2w.B2WConstants;

@ProcInfo(
        partitionInfo = "CHECKOUT.ID: 0",
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
    
    
    public VoltTable[] run(String checkout_id, String cart_id, String deliveryAddressId, String billingAddressId, 
            double amountDue, double total, String freightContract, double freightPrice, String freightStatus,
            String line_id[], String transaction_id[], int delivery_time[]) {
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
        
        assert(line_id.length == transaction_id.length);
        assert(line_id.length == delivery_time.length);
        
        for (int i = 0; i < line_id.length; ++i) {
            if(line_id[i] == null) continue;
            
            if(transaction_id[i] != null) {
                voltQueueSQL(createCheckoutStockTxnStmt,
                        checkout_id,
                        transaction_id,
                        line_id);           
            }

            if(delivery_time[i] != B2WConstants.NULL_DELIVERY_TIME) {                
                voltQueueSQL(createCheckoutFreightDeliveryTimeStmt,
                        checkout_id,
                        line_id,
                        delivery_time);
            }        
        }
        
        return voltExecuteSQL(true);
    }

}
