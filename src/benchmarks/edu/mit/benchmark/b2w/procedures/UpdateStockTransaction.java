package edu.mit.benchmark.b2w.procedures;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.types.TimestampType;

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

@ProcInfo(
        partitionInfo = "STK_STOCK_TRANSACTION.partition_key: 0",
        singlePartition = true
    )
public class UpdateStockTransaction extends VoltProcedure {
    private static final Logger LOG = Logger.getLogger(VoltProcedure.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.setupLogging();
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
        
    public final SQLStmt getStockTxnStmt = new SQLStmt("SELECT current_status, status FROM STK_STOCK_TRANSACTION WHERE partition_key = ? AND transaction_id = ? ");

    public final SQLStmt updateStockTxnStmt = new SQLStmt(
            "UPDATE STK_STOCK_TRANSACTION " +
            "   SET current_status = ?, " +
            "       status = ? " +
            " WHERE partition_key = ? AND transaction_id = ?;"
        ); // current_status, status, partition_key, transaction_id

    
    public VoltTable[] run(int partition_key, String transaction_id, TimestampType timestamp, String current_status){
        voltQueueSQL(getStockTxnStmt, partition_key, transaction_id);
        final VoltTable[] stock_txn_results = voltExecuteSQL();
        assert stock_txn_results.length == 1;
        
        String previous_status = null;
        String status = null;
        
        if (stock_txn_results[0].getRowCount() > 0) {
            final VoltTableRow stock_txn = stock_txn_results[0].fetchRow(0);
            final int CURRENT_STATUS = 0, STATUS = 1;
            previous_status = stock_txn.getString(CURRENT_STATUS);
            status = stock_txn.getString(STATUS);
        } else {
            return null;
        }
        
        VoltTable[] result = new VoltTable[1];
        result[0] = new VoltTable(new VoltTable.ColumnInfo("StatusChanged", VoltType.BOOLEAN));
        if (previous_status.equals(current_status)) {
            result[0].addRow(false);
        } else {
            try {
                JSONObject status_obj = new JSONObject(status);
                status_obj.put(timestamp.toString(), current_status);
                status = status_obj.toString();
            } catch (JSONException e) {
                LOG.info("Failed to parse status: " + status);
            }

            voltQueueSQL(updateStockTxnStmt, current_status, status, partition_key, transaction_id);
            voltExecuteSQL(true);

            result[0].addRow(true);
        }

        return result;
    }

}
