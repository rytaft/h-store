package edu.mit.benchmark.b2w.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

@ProcInfo(
        partitionInfo = "CART.partition_key: 0",
        singlePartition = true
    )
public class GetCart extends VoltProcedure {
//    private static final Logger LOG = Logger.getLogger(VoltProcedure.class);
//    private static final LoggerBoolean debug = new LoggerBoolean();
//    private static final LoggerBoolean trace = new LoggerBoolean();
//    static {
//        LoggerUtil.setupLogging();
//        LoggerUtil.attachObserver(LOG, debug, trace);
//    }
    
    public final SQLStmt getCartStmt = new SQLStmt(
            "SELECT * FROM CART WHERE partition_key = ? AND id = ? ");
    
    public final SQLStmt getCartCustomerStmt = new SQLStmt(
            "SELECT * FROM CART_CUSTOMER WHERE partition_key = ? AND cartId = ? ");
    
    public final SQLStmt getCartLinesStmt = new SQLStmt(
            "SELECT * FROM CART_LINES WHERE partition_key = ? AND cartId = ? ");
    
    public final SQLStmt getCartLineProductsStmt = new SQLStmt(
            "SELECT * FROM CART_LINE_PRODUCTS WHERE partition_key = ? AND cartId = ? ");
    
    public final SQLStmt getCartLinePromotionsStmt = new SQLStmt(
            "SELECT * FROM CART_LINE_PROMOTIONS WHERE partition_key = ? AND cartId = ? ");
    
    public final SQLStmt getCartLineProductWarrantiesStmt = new SQLStmt(
            "SELECT * FROM CART_LINE_PRODUCT_WARRANTIES WHERE partition_key = ? AND cartId = ? ");
    
    public final SQLStmt getCartLineProductStoresStmt = new SQLStmt(
            "SELECT * FROM CART_LINE_PRODUCT_STORES WHERE partition_key = ? AND cartId = ? ");


    public VoltTable[] run(int partition_key, String cart_id){
        voltQueueSQL(getCartStmt, partition_key, cart_id);
        voltQueueSQL(getCartCustomerStmt, partition_key, cart_id);
        voltQueueSQL(getCartLinesStmt, partition_key, cart_id);
        voltQueueSQL(getCartLineProductsStmt, partition_key, cart_id);
        voltQueueSQL(getCartLinePromotionsStmt, partition_key, cart_id);
        voltQueueSQL(getCartLineProductWarrantiesStmt, partition_key, cart_id);
        voltQueueSQL(getCartLineProductStoresStmt, partition_key, cart_id);
        
        return voltExecuteSQL(true);
    }

}
