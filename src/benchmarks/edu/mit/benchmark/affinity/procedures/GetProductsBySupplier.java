package edu.mit.benchmark.affinity.procedures;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import edu.brown.benchmark.ycsb.distributions.ZipfianGenerator;

@ProcInfo(partitionInfo = "SUPPLIES.SUPPLIER_KEY: 0", singlePartition = true)
public class GetProductsBySupplier extends VoltProcedure {
    // private static final Logger LOG = Logger.getLogger(VoltProcedure.class);
    // private static final LoggerBoolean debug = new LoggerBoolean();
    // private static final LoggerBoolean trace = new LoggerBoolean();
    // static {
    // LoggerUtil.setupLogging();
    // LoggerUtil.attachObserver(LOG, debug, trace);
    // }

    public final SQLStmt getSupplierInfoStmt = new SQLStmt("SELECT FIELD1, FIELD2, FIELD3 FROM SUPPLIERS WHERE SUPPLIER_KEY = ? ");
    public final SQLStmt getPartsBySupplierStmt = new SQLStmt("SELECT PART_KEY FROM SUPPLIES WHERE SUPPLIER_KEY = ? ");
    public final SQLStmt getProductsByPartStmt = new SQLStmt("SELECT PRODUCT_KEY FROM USES WHERE PART_KEY = ? ");
    public final SQLStmt getProductInfoStmt = new SQLStmt("SELECT FIELD1, FIELD2, FIELD3 FROM PRODUCTS WHERE PRODUCT_KEY = ? ");
    
    public VoltTable[] run(long supplier_key, boolean limitScan, int limitScanTo, boolean limitZipfianScan) {
        voltQueueSQL(getSupplierInfoStmt, supplier_key);
        voltQueueSQL(getPartsBySupplierStmt, supplier_key);
        final VoltTable[] results = voltExecuteSQL();
        assert results.length == 2;
        if (limitScan && results[1].getRowCount() > limitScanTo) {
            // Limit the data we are scanning. select parts at random
            long[] parts = new long[results[1].getRowCount()];

            // get the list of parts
            for (int i = 0; i < results[1].getRowCount(); ++i) {
                parts[i] = results[1].fetchRow(i).getLong(0);
            }

            if (limitZipfianScan) {
                // The chosen set of parts will follow a zipfian distribution
                // without replacement
                Arrays.sort(parts);
                ZipfianGenerator r = new ZipfianGenerator(parts.length);
                Set<Integer> indices = new HashSet<>();
                for (int i = 0; i < limitScanTo; ++i) {
                    Integer index = r.nextInt();
                    while (indices.contains(index)) {
                        index = (index + 1) % parts.length;
                    }
                    indices.add(index);
                }
                for (Integer index : indices) {
                    voltQueueSQL(getProductsByPartStmt, parts[index]);
                }
            } else {
                // Shuffle list of parts
                Random r = new Random();
                for (int i = parts.length - 1; i > 0; i--) {
                    int index = r.nextInt(i + 1);
                    // Simple swap
                    long a = parts[index];
                    parts[index] = parts[i];
                    parts[i] = a;
                }

                for (int i = 0; i < limitScanTo; ++i) {
                    voltQueueSQL(getProductsByPartStmt, parts[i]);
                }
            }

        } else {
            for (int i = 0; i < results[1].getRowCount(); ++i) {
                voltQueueSQL(getProductsByPartStmt, results[1].fetchRow(i).getLong(0));
            }
        }
        
        final VoltTable[] results1 = voltExecuteSQL();
        assert (results1.length == results[1].getRowCount() || results1.length == limitScanTo);
        Set<Long> products = new HashSet<>();
        
        for (VoltTable result : results1) {
            for (int i = 0; i < result.getRowCount(); ++i) {
                products.add(result.fetchRow(i).getLong(0));
            }
        }
        
        for (Long product : products) {
            voltQueueSQL(getProductInfoStmt, product.longValue());
        }
        
        return voltExecuteSQL(true);
    }

}
