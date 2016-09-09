package edu.mit.benchmark.b2w.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;

@ProcInfo(
        partitionInfo = "STK_INVENTORY_STOCK_QUANTITY.partition_key: 0",
        singlePartition = true
    )
public class ReserveStock extends VoltProcedure {
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


    public VoltTable[] run(int partition_key, String stock_id, int requested_quantity){
        voltQueueSQL(getStockQtyStmt, stock_id);
        final VoltTable[] stock_results = voltExecuteSQL();
        assert stock_results.length == 1;
        
        int available = 0;
        int purchase = 0;
        int session = 0;
        
        if (stock_results[0].getRowCount() > 0) {
            final VoltTableRow stock = stock_results[0].fetchRow(0);
            final int STOCK_ID = 0 + 1, AVAILABLE = 1 + 1, PURCHASE = 2 + 1, SESSION = 3 + 1;
            assert stock_id.equals(stock.getString(STOCK_ID));
            available = (int) stock.getLong(AVAILABLE);
            purchase = (int) stock.getLong(PURCHASE);
            session = (int) stock.getLong(SESSION);
        } else {
            return null;
        }
        
        // check if the item is available
        if(available < requested_quantity) {
            requested_quantity = available;
        }
        
        available -= requested_quantity;
        session += requested_quantity;
        
        voltQueueSQL(updateStockQtyStmt, available, purchase, session, stock_id);
        voltExecuteSQL(true);
        
        VoltTable[] result = new VoltTable[1];
        result[0] = new VoltTable(
                new VoltTable.ColumnInfo("ReservedQty", VoltType.INTEGER));
        result[0].addRow(requested_quantity);
        return result;
    }

}
