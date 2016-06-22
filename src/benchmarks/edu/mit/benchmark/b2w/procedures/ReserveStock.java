package edu.mit.benchmark.b2w.procedures;

import java.util.Date;

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
public class ReserveStock extends VoltProcedure {
//    private static final Logger LOG = Logger.getLogger(VoltProcedure.class);
//    private static final LoggerBoolean debug = new LoggerBoolean();
//    private static final LoggerBoolean trace = new LoggerBoolean();
//    static {
//        LoggerUtil.setupLogging();
//        LoggerUtil.attachObserver(LOG, debug, trace);
//    }
    
    public final SQLStmt createStockTxnStmt = new SQLStmt(
            "INSERT INTO STK_STOCK_TRANSACTION (" +
                "transaction_id, " +
                "reserve_id, " +
                "brand, " +
                "creation_date, " +
                "current_status, " +
                "expiration_date, " +
                "is_kit, " +
                "requested_quantity, " +
                "reserve_lines, " +
                "reserved_quantity, " +
                "sku, " +
                "solr_query, " +
                "status, " +
                "store_id, " +
                "subinventory, " +
                "warehouse" +
            ") VALUES (" +
                "?, " +   // transaction_id
                "?, " +   // reserve_id
                "?, " +   // brand
                "?, " +   // creation_date
                "?, " +   // current_status
                "?, " +   // expiration_date
                "?, " +   // is_kit
                "?, " +   // requested_quantity
                "?, " +   // reserve_lines
                "?, " +   // reserved_quantity
                "?, " +   // sku
                "?, " +   // solr_query
                "?, " +   // status
                "?, " +   // store_id
                "?, " +   // subinventory
                "?"   +   // warehouse
            ");");
    
    public final SQLStmt getStockQtyStmt = new SQLStmt("SELECT * FROM STK_INVENTORY_STOCK_QUANTITY WHERE id = ? ");
    
    public final SQLStmt updateStockQtyStmt = new SQLStmt(
            "UPDATE STK_INVENTORY_STOCK_QUANTITY " +
            "   SET available = ?, " +
            "       purchase = ?, " +
            "       session = ? " +
            " WHERE id = ?;"
        ); // available, purchase, session, id


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
        if(available < requested_quantity) {
            return null;
        }
        
        available -= requested_quantity;
        session += requested_quantity;
        
        voltQueueSQL(updateStockQtyStmt, available, purchase, session, stock_id);
        
        String reserve_id = transaction_id; // todo: fixme
        String brand = null;
        String current_status = B2WConstants.STATUS_NEW;
        TimestampType expiration_date = new TimestampType(new Date(timestamp.getMSTime() + 30 * 60 * 1000)); // add 30 mins
        int is_kit = 0;
        String reserve_lines = null;
        int reserved_quantity = 0;
        long sku = 0;
        String solr_query = null;
        String status = null;
        long store_id = 0;
        int subinventory = 0;
        int warehouse = 0;
        
        voltQueueSQL(createStockTxnStmt,
                transaction_id,
                reserve_id,
                brand,
                timestamp,
                current_status,
                expiration_date,
                is_kit,
                requested_quantity,
                reserve_lines,
                reserved_quantity,
                sku,
                solr_query,
                status,
                store_id,
                subinventory,
                warehouse);
        
        return voltExecuteSQL(true);
    }

}
