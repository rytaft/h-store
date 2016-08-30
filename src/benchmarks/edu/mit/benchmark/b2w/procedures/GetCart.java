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
            "SELECT * FROM CART WHERE id = ? ");
    
    public final SQLStmt getCartCustomerStmt = new SQLStmt(
            "SELECT * FROM CART_CUSTOMER WHERE cartId = ? ");
    
    public final SQLStmt getCartLinesStmt = new SQLStmt(
            "SELECT * FROM CART_LINES WHERE cartId = ? ");
    
    public final SQLStmt getCartLineProductsStmt = new SQLStmt(
            "SELECT * FROM CART_LINE_PRODUCTS WHERE cartId = ? ");
    
    public final SQLStmt getCartLinePromotionsStmt = new SQLStmt(
            "SELECT * FROM CART_LINE_PROMOTIONS WHERE cartId = ? ");
    
    public final SQLStmt getCartLineProductWarrantiesStmt = new SQLStmt(
            "SELECT * FROM CART_LINE_PRODUCT_WARRANTIES WHERE cartId = ? ");
    
    public final SQLStmt getCartLineProductStoresStmt = new SQLStmt(
            "SELECT * FROM CART_LINE_PRODUCT_STORES WHERE cartId = ? ");


    public VoltTable[] run(Integer partition_key, String cart_id){
        voltQueueSQL(getCartStmt, cart_id);
        voltQueueSQL(getCartCustomerStmt, cart_id);
        voltQueueSQL(getCartLinesStmt, cart_id);
        voltQueueSQL(getCartLineProductsStmt, cart_id);
        voltQueueSQL(getCartLinePromotionsStmt, cart_id);
        voltQueueSQL(getCartLineProductWarrantiesStmt, cart_id);
        voltQueueSQL(getCartLineProductStoresStmt, cart_id);
        
        return voltExecuteSQL(true);
    }

}
