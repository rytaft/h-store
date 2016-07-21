package edu.mit.benchmark.b2w.procedures;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.types.TimestampType;

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.mit.benchmark.b2w.B2WConstants;

@ProcInfo(
        partitionInfo = "STK_STOCK_TRANSACTION.TRANSACTION_ID: 0",
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
        
    public final SQLStmt getStockTxnStmt = new SQLStmt("SELECT status FROM STK_STOCK_TRANSACTION WHERE transaction_id = ? ");

    public final SQLStmt updateStockTxnStmt = new SQLStmt(
            "UPDATE STK_STOCK_TRANSACTION " +
            "   SET current_status = ?, " +
            "       status = ? " +
            " WHERE transaction_id = ?;"
        ); // current_status, status, transaction_id

    
    public VoltTable[] run(String transaction_id, TimestampType timestamp, String current_status){
        voltQueueSQL(getStockTxnStmt, transaction_id);
        final VoltTable[] stock_txn_results = voltExecuteSQL();
        assert stock_txn_results.length == 1;
        
        String status = null;
        
        if (stock_txn_results[0].getRowCount() > 0) {
            final VoltTableRow stock_txn = stock_txn_results[0].fetchRow(0);
            final int STATUS = 0;
            status = stock_txn.getString(STATUS);
        } else {
            return null;
        }
        
        try {
            JSONObject status_obj = new JSONObject(status);
            status_obj.append(timestamp.toString(), current_status);
            status = status_obj.toString();
        } catch (JSONException e) {
            LOG.info("Failed to parse status: " + status);
        }
        
        voltQueueSQL(updateStockTxnStmt, current_status, status, transaction_id);
        
        return voltExecuteSQL(true);
    }

}
