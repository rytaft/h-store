package edu.mit.benchmark.affinity.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

@ProcInfo(
        partitionInfo = "SUPPLIERS.SUPPLIER_KEY: 0",
        singlePartition = true
    )
public class GetSupplier extends VoltProcedure {
//    private static final Logger LOG = Logger.getLogger(VoltProcedure.class);
//    private static final LoggerBoolean debug = new LoggerBoolean();
//    private static final LoggerBoolean trace = new LoggerBoolean();
//    static {
//        LoggerUtil.setupLogging();
//        LoggerUtil.attachObserver(LOG, debug, trace);
//    }
    
    
    public final SQLStmt getSupplierStmt = new SQLStmt("SELECT * FROM SUPPLIERS WHERE SUPPLIER_KEY = ? ");
    
    public VoltTable[] run(long supplier_key){
        voltQueueSQL(getSupplierStmt, supplier_key);
        return voltExecuteSQL(true);
    }

}
