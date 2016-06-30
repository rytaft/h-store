package edu.mit.benchmark.b2w.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

@ProcInfo(
        partitionInfo = "STK_INVENTORY_STOCK.ID: 0",
        singlePartition = true
    )
public class CreateStockTransaction extends VoltProcedure {
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

    public VoltTable[] run(String transaction_id, String reserve_id, String brand, TimestampType timestamp, String current_status,
        TimestampType expiration_date, int is_kit, int requested_quantity, String reserve_lines, int reserved_quantity, long sku, 
        String solr_query, String status, long store_id, int subinventory, int warehouse) {
        
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
