package edu.mit.benchmark.b2w.procedures;

import org.apache.log4j.Logger;
import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.mit.benchmark.b2w.B2WConstants;

@ProcInfo(
        partitionInfo = "CART.ID: 0",
        singlePartition = true
    )
public class ReserveCart extends VoltProcedure {
    private static final Logger LOG = Logger.getLogger(VoltProcedure.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.setupLogging();
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
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
            "   SET lastModified = ?, " +
            "       status = ? " +
            " WHERE id = ?;"
        ); // lastModified, status, id


    public VoltTable[] run(String cart_id, TimestampType timestamp, String customer_id,
            String token, int guest, int isGuest){
        voltQueueSQL(getCartStmt, cart_id);
        final VoltTable[] cart_results = voltExecuteSQL();
        assert cart_results.length == 1;
                
        if (cart_results[0].getRowCount() <= 0) {
            if(debug.val) 
                LOG.debug("No cart found with cart_id " + cart_id + " during attempt to reserve cart");
            return null;
        }
        
        String status = B2WConstants.STATUS_RESERVED;
        
        voltQueueSQL(createCartCustomerStmt, cart_id, customer_id, token, guest, isGuest);
        voltQueueSQL(updateCartStmt, timestamp, status, cart_id);
        
        return voltExecuteSQL(true);
    }

}
