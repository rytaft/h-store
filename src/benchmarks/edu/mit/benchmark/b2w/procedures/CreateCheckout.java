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

    public VoltTable[] run(String checkout_id, String cart_id){
        String deliveryAddressId = null;
        String billingAddressId = null;
        double amountDue = 0;
        double total = 0;
        String freightContract = null;
        double freightPrice = 0;
        String freightStatus = null;

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
        
        return voltExecuteSQL(true);
    }

}
