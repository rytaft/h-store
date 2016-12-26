package edu.mit.benchmark.b2w.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import edu.mit.benchmark.b2w.B2WUtil;

@ProcInfo(
        partitionInfo = "CART.partition_key: 0",
        singlePartition = true
    )
public class DeleteCart extends VoltProcedure {
//    private static final Logger LOG = Logger.getLogger(VoltProcedure.class);
//    private static final LoggerBoolean debug = new LoggerBoolean();
//    private static final LoggerBoolean trace = new LoggerBoolean();
//    static {
//        LoggerUtil.setupLogging();
//        LoggerUtil.attachObserver(LOG, debug, trace);
//    }
    
    public final SQLStmt deleteCartStmt = new SQLStmt(
            "DELETE FROM CART WHERE partition_key = ? AND id = ? ");
    
    public final SQLStmt deleteCartCustomerStmt = new SQLStmt(
            "DELETE FROM CART_CUSTOMER WHERE partition_key = ? AND cartId = ? ");
    
    public final SQLStmt deleteCartLinesStmt = new SQLStmt(
            "DELETE FROM CART_LINES WHERE partition_key = ? AND cartId = ? ");
    
    public final SQLStmt deleteCartLineProductsStmt = new SQLStmt(
            "DELETE FROM CART_LINE_PRODUCTS WHERE partition_key = ? AND cartId = ? ");
    
    public final SQLStmt deleteCartLinePromotionsStmt = new SQLStmt(
            "DELETE FROM CART_LINE_PROMOTIONS WHERE partition_key = ? AND cartId = ? ");
    
    public final SQLStmt deleteCartLineProductWarrantiesStmt = new SQLStmt(
            "DELETE FROM CART_LINE_PRODUCT_WARRANTIES WHERE partition_key = ? AND cartId = ? ");
    
    public final SQLStmt deleteCartLineProductStoresStmt = new SQLStmt(
            "DELETE FROM CART_LINE_PRODUCT_STORES WHERE partition_key = ? AND cartId = ? ");


    public VoltTable[] run(int partition_key, String cart_id, long sleep_time){
        B2WUtil.sleep(sleep_time);
        
        voltQueueSQL(deleteCartCustomerStmt, partition_key, cart_id);
        voltQueueSQL(deleteCartLinesStmt, partition_key, cart_id);
        voltQueueSQL(deleteCartLineProductsStmt, partition_key, cart_id);
        voltQueueSQL(deleteCartLinePromotionsStmt, partition_key, cart_id);
        voltQueueSQL(deleteCartLineProductWarrantiesStmt, partition_key, cart_id);
        voltQueueSQL(deleteCartLineProductStoresStmt, partition_key, cart_id);
        voltQueueSQL(deleteCartStmt, partition_key, cart_id);
        
        return voltExecuteSQL(true);
    }

}
