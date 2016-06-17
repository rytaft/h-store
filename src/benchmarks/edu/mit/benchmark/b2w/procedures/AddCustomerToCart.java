package edu.mit.benchmark.b2w.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.ycsb.YCSBUtil;
import edu.mit.benchmark.b2w.B2WConstants;

@ProcInfo(
        partitionInfo = "CART.ID: 0",
        singlePartition = true
    )
public class AddCustomerToCart extends VoltProcedure {
//    private static final Logger LOG = Logger.getLogger(VoltProcedure.class);
//    private static final LoggerBoolean debug = new LoggerBoolean();
//    private static final LoggerBoolean trace = new LoggerBoolean();
//    static {
//        LoggerUtil.setupLogging();
//        LoggerUtil.attachObserver(LOG, debug, trace);
//    }
    
    public final SQLStmt createCartStmt = new SQLStmt(
            "INSERT INTO CART (" +
                "id, " + 
                "total, " +
                "salesChannel, " +
                "opn, " +
                "epar, " +
                "lastModified, " +
                "status, " +
                "autoMerge" +
            ") VALUES (" +
                "?, " +   // id
                "?, " +   // total
                "?, " +   // salesChannel
                "?, " +   // opn
                "?, " +   // epar
                "?, " +   // lastModified
                "?, " +   // status
                "?"   +   // autoMerge
            ");");
    
    public final SQLStmt createCartCustomerStmt = new SQLStmt(
            "INSERT INTO CART_CUSTOMER (" +
                "cartId, " +
                "id, " +
                "token, " +
                "guest, " +
                "isGuest" +
            ") VALUES (" +
                "?, " +   // cartId
                "?, " +   // id
                "?, " +   // token
                "?, " +   // guest
                "?"   +   // isGuest
            ");");
    
    
    public final SQLStmt getCartStmt = new SQLStmt("SELECT * FROM CART WHERE id = ? ");
    
    public final SQLStmt updateCartStmt = new SQLStmt(
            "UPDATE CART " +
            "   SET total = ?, " +
            "       lastModified = ?, " +
            "       status = ? " +
            " WHERE id = ?;"
        ); //total, lastModified, status, id


    public VoltTable[] run(String cart_id, String salesChannel, TimestampType timestamp, String customer_id){
        voltQueueSQL(getCartStmt, cart_id);
        final VoltTable[] cart_results = voltExecuteSQL();
        assert cart_results.length == 1;
        
        double total = 0;
        String opn = null;
        String epar = null;
        TimestampType lastModified = timestamp;
        String status = B2WConstants.STATUS_NEW;
        int autoMerge = 0;
        String token = YCSBUtil.astring(B2WConstants.TOKEN_LENGTH, B2WConstants.TOKEN_LENGTH);
        int guest = 0;
        int isGuest = 0;
        
        if (cart_results[0].getRowCount() > 0) {
            final VoltTableRow cart = cart_results[0].fetchRow(0);
            final int CART_ID = 0, TOTAL = 1, SALES_CHANNEL = 2, OPN = 3, EPAR = 4, LAST_MODIFIED = 5, STATUS = 6, AUTO_MERGE = 7;
            assert cart_id.equals(cart.getString(CART_ID));
            total = cart.getDouble(TOTAL);
            assert salesChannel.equals(cart.getString(SALES_CHANNEL));
            opn = cart.getString(OPN);
            epar = cart.getString(EPAR);
            lastModified = cart.getTimestampAsTimestamp(LAST_MODIFIED);
            status = cart.getString(STATUS);
            autoMerge = (int) cart.getLong(AUTO_MERGE);           
        } else {
            voltQueueSQL(createCartStmt, cart_id, total, salesChannel, opn, epar, lastModified, status, autoMerge);
        }
        
        voltQueueSQL(createCartCustomerStmt, cart_id, customer_id, token, guest, isGuest);
        voltQueueSQL(updateCartStmt, total, timestamp, status, cart_id);
        
        return voltExecuteSQL(true);
    }

}
