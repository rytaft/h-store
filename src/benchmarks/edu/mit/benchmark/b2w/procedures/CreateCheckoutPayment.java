package edu.mit.benchmark.b2w.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.types.TimestampType;

import edu.mit.benchmark.b2w.B2WConstants;

@ProcInfo(
        partitionInfo = "CART.ID: 0",
        singlePartition = true
    )
public class CreateCheckoutPayment extends VoltProcedure {
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

    public VoltTable[] run(String checkout_id, String cart_id){
        String paymentOptionId = null;
        String paymentOptionType = null;
        int dueDays = 0;
        double amount = 0;
        int installmentQuantity = 0;
        double interestAmount = 0;
        int interestRate = 0;
        int annualCET = 0;
        String number = null;
        long criptoNumber = 0;
        String holdersName = null;
        long securityCode = 0;
        String expirationDate = null;

        voltQueueSQL(createCheckoutPaymentStmt,
                checkout_id,
                paymentOptionId,
                paymentOptionType,
                dueDays,
                amount,
                installmentQuantity,
                interestAmount,
                interestRate,
                annualCET,
                number,
                criptoNumber,
                holdersName,
                securityCode,
                expirationDate);
        
        return voltExecuteSQL(true);
    }

}
