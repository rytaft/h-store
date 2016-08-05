package edu.mit.benchmark.b2w.procedures;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.mit.benchmark.b2w.B2WConstants;

@ProcInfo(
        partitionInfo = "STK_INVENTORY_STOCK.ID: 0",
        singlePartition = true
    )
public class CreateStockTransaction extends VoltProcedure {
    private static final Logger LOG = Logger.getLogger(VoltProcedure.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.setupLogging();
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
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

    public VoltTable[] run(String transaction_id, String[] reserve_id, String[] brand, TimestampType[] timestamp,
        TimestampType[] expiration_date, byte[] is_kit, int requested_quantity, String[] reserve_lines, int[] reserved_quantity, long[] sku, 
        String[] solr_query, long[] store_id, int[] subinventory, int[] warehouse) {
        
        String current_status = B2WConstants.STATUS_NEW;
        
        for (int i = 0; i < reserve_id.length; ++i) { 
            if (reserve_id[i] == null || reserve_id[i].isEmpty()) continue;

            JSONObject status_obj = new JSONObject();
            try {
                status_obj.append(timestamp[i].toString(), current_status);
            } catch (JSONException e) {
                if (debug.val) {
                    LOG.debug("Failed to append current status " + current_status + " at timestamp " + timestamp[i].toString());
                }
            }       
            String status = status_obj.toString();
            if (trace.val) {
                LOG.trace("Creating transaction " + transaction_id + " with status " + status);
            }
            
            voltQueueSQL(createStockTxnStmt,
                    transaction_id,
                    reserve_id[i],
                    brand[i],
                    timestamp[i],
                    current_status,
                    expiration_date[i],
                    is_kit[i],
                    requested_quantity,
                    reserve_lines[i],
                    reserved_quantity[i],
                    sku[i],
                    solr_query[i],
                    status,
                    store_id[i],
                    subinventory[i],
                    warehouse[i]);
        }
        
        if (reserve_id.length == 0) return null;
        
        return voltExecuteSQL(true);
    }

}
