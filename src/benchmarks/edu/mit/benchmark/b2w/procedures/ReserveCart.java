package edu.mit.benchmark.b2w.procedures;

import java.util.HashMap;

import org.apache.log4j.Logger;
import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.types.TimestampType;

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.mit.benchmark.b2w.B2WConstants;

import static edu.mit.benchmark.b2w.B2WLoader.hashPartition;

@ProcInfo(
        partitionInfo = "CART.partition_key: 0",
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
                "partition_key, " +
                "cartId, " +
                "id, " +
                "token, " +
                "guest, " +
                "isGuest" +
            ") VALUES (" +
                "?, " +   // partition_key
                "?, " +   // cartId
                "?, " +   // id
                "?, " +   // token
                "?, " +   // guest
                "?"   +   // isGuest
            ");");
    
    
    public final SQLStmt getCartLinesStmt = new SQLStmt("SELECT id, quantity FROM CART_LINES WHERE cartId = ? ");
    
    public final SQLStmt updateCartLineStmt = new SQLStmt(
            "UPDATE CART_LINES " +
            "   SET quantity = ?, " +
            "       stockTransactionId = ?, " +
            "       requestedQuantity = ?, " +
            "       status = ?, " +
            "       stockType = ? " +
            " WHERE cartId = ? AND id = ?;"
        ); // quantity, stockTransactionId, requestedQuantity, status, stockType, cartId, id

    public final SQLStmt updateCartLineQtyStmt = new SQLStmt(
            "UPDATE CART_LINES " +
            "   SET quantity = ?, " +
            "       requestedQuantity = ? " +
            " WHERE cartId = ? AND id = ?;"
        ); // quantity, requestedQuantity, cartId, id
    
    public final SQLStmt updateCartStmt = new SQLStmt(
            "UPDATE CART " +
            "   SET lastModified = ?, " +
            "       status = ? " +
            " WHERE id = ?;"
        ); // lastModified, status, id


    public VoltTable[] run(int partition_key, String cart_id, TimestampType timestamp, String customer_id,
            String token, byte guest, byte isGuest, String line_ids[], int requested_quantity[],
            int reserved_quantity[], String status[], String stock_type[], String transaction_id[]){
        assert(line_ids.length == transaction_id.length);
        assert(line_ids.length == requested_quantity.length);
        assert(line_ids.length == reserved_quantity.length);
        assert(line_ids.length == status.length);
        assert(line_ids.length == stock_type.length);
        
        HashMap<String, Integer> line_id_index = new HashMap<>();
        for(int i = 0; i < line_ids.length; ++i) {
            line_id_index.put(line_ids[i], i);
        }
        
        voltQueueSQL(getCartLinesStmt, cart_id);
        final VoltTable[] cart_results = voltExecuteSQL();
        assert cart_results.length == 1;
                
        if (cart_results[0].getRowCount() <= 0) {
            if(debug.val) 
                LOG.debug("No cart lines found with cart_id " + cart_id + " during attempt to reserve cart");
            return null;
        }
        
        for (int i = 0; i < cart_results[0].getRowCount(); ++i) {
            final VoltTableRow cartLine = cart_results[0].fetchRow(i);
            final int LINE_ID = 0, QUANTITY = 1;
            String line_id = cartLine.getString(LINE_ID);
            int original_quantity = (int) cartLine.getLong(QUANTITY);
            
            if (line_id_index.containsKey(line_id)) {
                int index = line_id_index.get(line_id);
                voltQueueSQL(updateCartLineStmt, reserved_quantity[index], transaction_id[index], requested_quantity[index], status[index], stock_type[index], cart_id, line_id);
            }
            else { // the cart line was not successfully reserved, so update the quantity to 0
                voltQueueSQL(updateCartLineQtyStmt, 0, original_quantity, cart_id, line_id);
            }
            
        }
        
        String cart_status = B2WConstants.STATUS_RESERVED;
        
        voltQueueSQL(createCartCustomerStmt, hashPartition(cart_id), cart_id, customer_id, token, guest, isGuest);
        voltQueueSQL(updateCartStmt, timestamp, cart_status, cart_id);
        
        return voltExecuteSQL(true);
    }

}
