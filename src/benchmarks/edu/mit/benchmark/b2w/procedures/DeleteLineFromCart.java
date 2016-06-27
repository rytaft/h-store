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
public class DeleteLineFromCart extends VoltProcedure {
//    private static final Logger LOG = Logger.getLogger(VoltProcedure.class);
//    private static final LoggerBoolean debug = new LoggerBoolean();
//    private static final LoggerBoolean trace = new LoggerBoolean();
//    static {
//        LoggerUtil.setupLogging();
//        LoggerUtil.attachObserver(LOG, debug, trace);
//    }
    
    public final SQLStmt deleteCartLineStmt = new SQLStmt(
            "DELETE FROM CART_LINES WHERE cartId = ? AND id = ?;");

    public final SQLStmt deleteCartLineProductStmt = new SQLStmt(
            "DELETE FROM CART_LINE_PRODUCTS WHERE cartId = ? AND lineId = ?;");

    public final SQLStmt deleteCartLinePromotionsStmt = new SQLStmt(
            "DELETE FROM CART_LINE_PROMOTIONS WHERE cartId = ? AND lineId = ?;");

    public final SQLStmt deleteCartLineProductWarrantiesStmt = new SQLStmt(
            "DELETE FROM CART_LINE_PRODUCT_WARRANTIES WHERE cartId = ? AND lineId = ?;");

    public final SQLStmt deleteCartLineProductStoresStmt = new SQLStmt(
            "DELETE FROM CART_LINE_PRODUCT_STORES WHERE cartId = ? AND lineId = ?;");

    public final SQLStmt getCartStmt = new SQLStmt("SELECT total FROM CART WHERE id = ?;");
    public final SQLStmt getCartLineStmt = new SQLStmt(
            "SELECT salesPrice FROM CART_LINES WHERE cartId = ? and id = ?;");
    
    public final SQLStmt updateCartStmt = new SQLStmt(
            "UPDATE CART " +
            "   SET total = ?, " +
            "       lastModified = ?, " +
            " WHERE id = ?;"
        ); //total, lastModified, id


    public VoltTable[] run(String cart_id, TimestampType timestamp, String line_id){
        voltQueueSQL(getCartStmt, cart_id);
        voltQueueSQL(getCartLineStmt, cart_id, line_id);
        final VoltTable[] cart_results = voltExecuteSQL();
        assert cart_results.length == 2;
        
        double total = 0;        
        double salesPrice = 0;

        if (cart_results[0].getRowCount() > 0) {
            final VoltTableRow cart = cart_results[0].fetchRow(0);
            final int TOTAL = 0;
            total = cart.getDouble(TOTAL);
        } else {
            return null;
        }
        
        if (cart_results[1].getRowCount() > 0) {
            final VoltTableRow cart_line = cart_results[1].fetchRow(0);
            final int SALES_PRICE = 0;
            salesPrice = cart_line.getDouble(SALES_PRICE);
        } else {
            return null;
        }       
        
        voltQueueSQL(deleteCartLineStmt, cart_id, line_id);        
        voltQueueSQL(deleteCartLineProductStmt, cart_id, line_id);
        voltQueueSQL(deleteCartLinePromotionsStmt, cart_id, line_id);
        voltQueueSQL(deleteCartLineProductWarrantiesStmt, cart_id, line_id);
        voltQueueSQL(deleteCartLineProductStoresStmt, cart_id, line_id);
        
        total -= salesPrice;
        
        voltQueueSQL(updateCartStmt, total, timestamp, cart_id);
        
        return voltExecuteSQL(true);
    }

}
