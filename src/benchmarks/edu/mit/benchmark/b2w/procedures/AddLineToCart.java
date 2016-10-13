package edu.mit.benchmark.b2w.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.types.TimestampType;

import edu.mit.benchmark.b2w.B2WConstants;

@ProcInfo(
        partitionInfo = "CART.ID: 0",
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
                "id, " + 
                "total, " +
                "salesChannel, " +
                "opn, " +
                "epar, " +
                "lastModified, " +
                "status, " +
                "autoMerge" +
            ") VALUES (" +
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
                "cartId, " +
                "id, " +
                "token, " +
                "guest, " +
                "isGuest" +
            ") VALUES (" +
                "?, " +   // cartId
                "?, " +   // id
                "?, " +   // token
                "?, " +   // guest
                "?"   +   // isGuest
            ");");
    
    public final SQLStmt createCartLineStmt = new SQLStmt(
            "INSERT INTO CART_LINES (" +
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

    
    public final SQLStmt getCartStmt = new SQLStmt("SELECT * FROM CART WHERE id = ? ");
    
    public final SQLStmt updateCartStmt = new SQLStmt(
            "UPDATE CART " +
            "   SET total = ?, " +
            "       lastModified = ?, " +
            "       status = ? " +
            " WHERE id = ?;"
        ); //total, lastModified, status, id


    public VoltTable[] run(String cart_id, TimestampType timestamp, String line_id, 
            String product_sku, long product_id, String store_id, int quantity, String salesChannel, String opn, String epar, byte autoMerge,
            double unitSalesPrice, double salesPrice, int maxQuantity, String maximumQuantityReason, String type, String stockTransactionId,
            int requestedQuantity, String line_status, String stockType, String image, String name, byte isKit, double price, double originalPrice,
            byte isLarge, long department, long line, long subClass, double weight, long product_class){
        voltQueueSQL(getCartStmt, cart_id);
        final VoltTable[] cart_results = voltExecuteSQL();
        assert cart_results.length == 1;
        
        double total = 0;
        String status = B2WConstants.STATUS_NEW;       
        
        if (cart_results[0].getRowCount() > 0) {
            final VoltTableRow cart = cart_results[0].fetchRow(0);
            final int CART_ID = 0, TOTAL = 1, STATUS = 6;
            assert cart_id.equals(cart.getString(CART_ID));
            total = cart.getDouble(TOTAL);
            status = cart.getString(STATUS);        
        } else {
            voltQueueSQL(createCartStmt, cart_id, total, salesChannel, opn, epar, timestamp, status, autoMerge);
        }
        
        voltQueueSQL(createCartLineStmt, 
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
        
        voltQueueSQL(updateCartStmt, total, timestamp, status, cart_id);
        
        return voltExecuteSQL(true);
    }

}
