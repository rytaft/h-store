package edu.mit.benchmark.b2w.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

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
            "DELETE FROM CART WHERE id = ? ");
    
    public final SQLStmt deleteCartCustomerStmt = new SQLStmt(
            "DELETE FROM CART_CUSTOMER WHERE cartId = ? ");
    
    public final SQLStmt deleteCartLinesStmt = new SQLStmt(
            "DELETE FROM CART_LINES WHERE cartId = ? ");
    
    public final SQLStmt deleteCartLineProductsStmt = new SQLStmt(
            "DELETE FROM CART_LINE_PRODUCTS WHERE cartId = ? ");
    
    public final SQLStmt deleteCartLinePromotionsStmt = new SQLStmt(
            "DELETE FROM CART_LINE_PROMOTIONS WHERE cartId = ? ");
    
    public final SQLStmt deleteCartLineProductWarrantiesStmt = new SQLStmt(
            "DELETE FROM CART_LINE_PRODUCT_WARRANTIES WHERE cartId = ? ");
    
    public final SQLStmt deleteCartLineProductStoresStmt = new SQLStmt(
            "DELETE FROM CART_LINE_PRODUCT_STORES WHERE cartId = ? ");


    public VoltTable[] run(Integer partition_key, String cart_id){
        voltQueueSQL(deleteCartCustomerStmt, cart_id);
        voltQueueSQL(deleteCartLinesStmt, cart_id);
        voltQueueSQL(deleteCartLineProductsStmt, cart_id);
        voltQueueSQL(deleteCartLinePromotionsStmt, cart_id);
        voltQueueSQL(deleteCartLineProductWarrantiesStmt, cart_id);
        voltQueueSQL(deleteCartLineProductStoresStmt, cart_id);
        voltQueueSQL(deleteCartStmt, cart_id);
        
        return voltExecuteSQL(true);
    }

}
