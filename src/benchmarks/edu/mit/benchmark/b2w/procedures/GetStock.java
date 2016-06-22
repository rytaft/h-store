package edu.mit.benchmark.b2w.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

@ProcInfo(
        partitionInfo = "STK_INVENTORY_STOCK.SKU: 0",
        singlePartition = true
    )
public class GetStock extends VoltProcedure {
//    private static final Logger LOG = Logger.getLogger(VoltProcedure.class);
//    private static final LoggerBoolean debug = new LoggerBoolean();
//    private static final LoggerBoolean trace = new LoggerBoolean();
//    static {
//        LoggerUtil.setupLogging();
//        LoggerUtil.attachObserver(LOG, debug, trace);
//    }
        
    public final SQLStmt getStockStmt = new SQLStmt("SELECT * FROM STK_INVENTORY_STOCK WHERE sku = ? ");
        
    public VoltTable[] run(long sku){
        voltQueueSQL(getStockStmt, sku);
        
        return voltExecuteSQL(true);
    }

}
