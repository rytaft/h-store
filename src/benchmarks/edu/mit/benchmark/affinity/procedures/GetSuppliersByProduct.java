package edu.mit.benchmark.affinity.procedures;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import edu.brown.benchmark.ycsb.distributions.ZipfianGenerator;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

@ProcInfo(partitionInfo = "USES.PRODUCT_KEY: 0", singlePartition = true)
public class GetSuppliersByProduct extends VoltProcedure {
    // private static final Logger LOG = Logger.getLogger(VoltProcedure.class);
    // private static final LoggerBoolean debug = new LoggerBoolean();
    // private static final LoggerBoolean trace = new LoggerBoolean();
    // static {
    // LoggerUtil.setupLogging();
    // LoggerUtil.attachObserver(LOG, debug, trace);
    // }

    public final SQLStmt getProductInfoStmt = new SQLStmt("SELECT FIELD1, FIELD2, FIELD3 FROM PRODUCTS WHERE PRODUCT_KEY = ? ");
    public final SQLStmt getPartsByProductStmt = new SQLStmt("SELECT PART_KEY FROM USES WHERE PRODUCT_KEY = ? ");
    public final SQLStmt getSuppliersByPartStmt = new SQLStmt("SELECT SUPPLIER_KEY FROM SUPPLIES WHERE PART_KEY = ? ");
    public final SQLStmt getSupplierInfoStmt = new SQLStmt("SELECT FIELD1, FIELD2, FIELD3 FROM SUPPLIERS WHERE SUPPLIER_KEY = ? ");
    
    public VoltTable[] run(long product_key, boolean limitScan, int limitScanTo, boolean limitZipfianScan) {
        voltQueueSQL(getProductInfoStmt, product_key);
        voltQueueSQL(getPartsByProductStmt, product_key);
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
                    voltQueueSQL(getSuppliersByPartStmt, parts[index]);
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
                    voltQueueSQL(getSuppliersByPartStmt, parts[i]);
                }
            }

        } else {
            for (int i = 0; i < results[1].getRowCount(); ++i) {
                voltQueueSQL(getSuppliersByPartStmt, results[1].fetchRow(i).getLong(0));
            }
        }
        
        final VoltTable[] results1 = voltExecuteSQL();
        assert (results1.length == results[1].getRowCount() || results1.length == limitScanTo);
        Set<Long> suppliers = new HashSet<>();
        
        for (VoltTable result : results1) {
            for (int i = 0; i < result.getRowCount(); ++i) {
                suppliers.add(result.fetchRow(i).getLong(0));
            }
        }
        
        for (Long supplier : suppliers) {
            voltQueueSQL(getSupplierInfoStmt, supplier.longValue());
        }
        
        return voltExecuteSQL(true);
    }

}
