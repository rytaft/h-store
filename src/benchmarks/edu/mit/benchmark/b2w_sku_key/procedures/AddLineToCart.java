package edu.mit.benchmark.b2w_sku_key.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.types.TimestampType;

import edu.mit.benchmark.b2w.B2WConstants;
import edu.mit.benchmark.b2w.B2WUtil;

@ProcInfo(
        partitionInfo = "CART.partition_key: 0",
        singlePartition = true
    )
public class AddLineToCart extends VoltProcedure {
//    private static final Logger LOG = Logger.getLogger(VoltProcedure.class);
//    private static final LoggerBoolean debug = new LoggerBoolean();
//    private static final LoggerBoolean trace = new LoggerBoolean();
//    static {
//        LoggerUtil.setupLogging();
//        LoggerUtil.attachObserver(LOG, debug, trace);
//    }
    
    public final SQLStmt createCartStmt = new SQLStmt(
            "INSERT INTO CART (" +
                "partition_key, " +
                "id, " +
                "total, " +
                "salesChannel, " +
                "opn, " +
                "epar, " +
                "lastModified, " +
                "status, " +
                "autoMerge" +
            ") VALUES (" +
                "?, " +   // partition_key
                "?, " +   // id
                "?, " +   // total
                "?, " +   // salesChannel
                "?, " +   // opn
                "?, " +   // epar
                "?, " +   // lastModified
                "?, " +   // status
                "?"   +   // autoMerge
            ");");
    
    public final SQLStmt createCartCustomerStmt = new SQLStmt(
            "INSERT INTO CART_CUSTOMER (" +
                "partition_key, " +
                "cartId, " +
                "id, " +
                "token, " +
                "guest, " +
                "isGuest" +
            ") VALUES (" +
                "?, " +   // partition_key
                "?, " +   // cartId
                "?, " +   // id
                "?, " +   // token
                "?, " +   // guest
                "?"   +   // isGuest
            ");");
    
    public final SQLStmt createCartLineStmt = new SQLStmt(
            "INSERT INTO CART_LINES (" +
                "partition_key, " +
                "cartId, " +
                "id, " +
                "productSku, " +
                "productId, " +
                "storeId, " +
                "unitSalesPrice, " +
                "salesPrice, " +
                "quantity, " +
                "maxQuantity, " +
                "maximumQuantityReason, " +
                "type, " +
                "stockTransactionId, " +
                "requestedQuantity, " +
                "status, " +
                "stockType, " +
                "insertDate" +
            ") VALUES (" +
                "?, " +   // partition_key
                "?, " +   // cartId
                "?, " +   // id
                "?, " +   // productSku
                "?, " +   // productId
                "?, " +   // storeId
                "?, " +   // unitSalesPrice
                "?, " +   // salesPrice
                "?, " +   // quantity
                "?, " +   // maxQuantity
                "?, " +   // maximumQuantityReason
                "?, " +   // type
                "?, " +   // stockTransactionId
                "?, " +   // requestedQuantity
                "?, " +   // status
                "?, " +   // stockType
                "?"   +   // insertDate
            ");");

    public final SQLStmt createCartLineProductStmt = new SQLStmt(
            "INSERT INTO CART_LINE_PRODUCTS (" +
                "partition_key, " +
                "cartId, " +
                "lineId, " +
                "id, " +
                "sku, " +
                "image, " +
                "name, " +
                "isKit, " +
                "price, " +
                "originalPrice, " +
                "isLarge, " +
                "department, " +
                "line, " +
                "subClass, " +
                "weight, " +
                "class" +
            ") VALUES (" +
                "?, " +   // partition_key
                "?, " +   // cartId
                "?, " +   // lineId
                "?, " +   // id
                "?, " +   // sku
                "?, " +   // image
                "?, " +   // name
                "?, " +   // isKit
                "?, " +   // price
                "?, " +   // originalPrice
                "?, " +   // isLarge
                "?, " +   // department
                "?, " +   // line
                "?, " +   // subClass
                "?, " +   // weight
                "?"   +   // class
            ");");

    
    public final SQLStmt getCartStmt = new SQLStmt("SELECT id, total, status FROM CART WHERE partition_key = ? AND id = ? ");
    
    public final SQLStmt updateCartStmt = new SQLStmt(
            "UPDATE CART " +
            "   SET total = ?, " +
            "       lastModified = ?, " +
            "       status = ? " +
            " WHERE partition_key = ? AND id = ?;"
        ); //total, lastModified, status, partition_key, id


    public VoltTable[] run(int partition_key, String cart_id, TimestampType timestamp, String line_id, 
            String product_sku, long product_id, String store_id, int quantity, String salesChannel, String opn, String epar, byte autoMerge,
            double unitSalesPrice, double salesPrice, int maxQuantity, String maximumQuantityReason, String type, String stockTransactionId,
            int requestedQuantity, String line_status, String stockType, String image, String name, byte isKit, double price, double originalPrice,
            byte isLarge, long department, long line, long subClass, double weight, long product_class, long sleep_time){
        B2WUtil.sleep(sleep_time);
        
        voltQueueSQL(getCartStmt, partition_key, cart_id);
        final VoltTable[] cart_results = voltExecuteSQL();
        assert cart_results.length == 1;
        
        double total = 0;
        String status = B2WConstants.STATUS_NEW;       
        
        if (cart_results[0].getRowCount() > 0) {
            final VoltTableRow cart = cart_results[0].fetchRow(0);
            final int CART_ID = 0, TOTAL = 1, STATUS = 2;
            assert cart_id.equals(cart.getString(CART_ID));
            total = cart.getDouble(TOTAL);
            status = cart.getString(STATUS);        
        } else {
            voltQueueSQL(createCartStmt, partition_key, cart_id, total, salesChannel, opn, epar, timestamp, status, autoMerge);
        }
        
        voltQueueSQL(createCartLineStmt, 
                partition_key,
                cart_id,
                line_id,
                product_sku,
                product_id,
                store_id,
                unitSalesPrice,
                salesPrice,
                quantity,
                maxQuantity,
                maximumQuantityReason,
                type,
                stockTransactionId,
                requestedQuantity,
                line_status,
                stockType,
                timestamp);
        
        voltQueueSQL(createCartLineProductStmt,
                partition_key,
                cart_id,
                line_id,
                product_id,
                product_sku,
                image,
                name,
                isKit,
                price,
                originalPrice,
                isLarge,
                department,
                line,
                subClass,
                weight,
                product_class);
        
        total += salesPrice;
        
        voltQueueSQL(updateCartStmt, total, timestamp, status, partition_key, cart_id);
        
        return voltExecuteSQL(true);
    }



}
