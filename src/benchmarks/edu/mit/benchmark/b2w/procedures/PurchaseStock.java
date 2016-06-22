package edu.mit.benchmark.b2w.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.types.TimestampType;

import edu.mit.benchmark.b2w.B2WConstants;

@ProcInfo(
        partitionInfo = "STK_INVENTORY_STOCK.ID: 0",
        singlePartition = true
    )
public class PurchaseStock extends VoltProcedure {
//    private static final Logger LOG = Logger.getLogger(VoltProcedure.class);
//    private static final LoggerBoolean debug = new LoggerBoolean();
//    private static final LoggerBoolean trace = new LoggerBoolean();
//    static {
//        LoggerUtil.setupLogging();
//        LoggerUtil.attachObserver(LOG, debug, trace);
//    }
        
    public final SQLStmt getStockQtyStmt = new SQLStmt("SELECT * FROM STK_INVENTORY_STOCK_QUANTITY WHERE id = ? ");
    
    public final SQLStmt updateStockQtyStmt = new SQLStmt(
            "UPDATE STK_INVENTORY_STOCK_QUANTITY " +
            "   SET available = ?, " +
            "       purchase = ?, " +
            "       session = ? " +
            " WHERE id = ?;"
        ); // available, purchase, session, id

    public final SQLStmt updateStockTxnStmt = new SQLStmt(
            "UPDATE STK_STOCK_TRANSACTION " +
            "   SET current_status = ?, " +
            " WHERE transaction_id = ?;"
        ); // current_status, transaction_id

    
    public VoltTable[] run(String stock_id, int requested_quantity, TimestampType timestamp, String transaction_id){
        voltQueueSQL(getStockQtyStmt, stock_id);
        final VoltTable[] stock_results = voltExecuteSQL();
        assert stock_results.length == 1;
        
        int available = 0;
        int purchase = 0;
        int session = 0;
        
        if (stock_results[0].getRowCount() > 0) {
            final VoltTableRow stock = stock_results[0].fetchRow(0);
            final int STOCK_ID = 0, AVAILABLE = 1, PURCHASE = 2, SESSION = 3;
            assert stock_id.equals(stock.getString(STOCK_ID));
            available = (int) stock.getLong(AVAILABLE);
            purchase = (int) stock.getLong(PURCHASE);
            session = (int) stock.getLong(SESSION);
        } else {
            return null;
        }
        
        // check if the item is available
        if(session < requested_quantity) {
            return null;
        }
        
        session -= requested_quantity;
        purchase += requested_quantity;
        
        voltQueueSQL(updateStockQtyStmt, available, purchase, session, stock_id);
        
        String current_status = B2WConstants.STATUS_PURCHASED;
        voltQueueSQL(updateStockTxnStmt, current_status, transaction_id);
        
        return voltExecuteSQL(true);
    }

}
