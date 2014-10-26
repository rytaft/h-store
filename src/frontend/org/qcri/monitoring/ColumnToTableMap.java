package org.qcri.monitoring;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Table;

import edu.brown.hashing.ExplicitPartitions;

public class ColumnToTableMap {
    private static final Logger LOG = Logger.getLogger(ExplicitPartitions.class);

    protected Map<Column, Table> column_to_table_map = new HashMap<Column, Table>();

    public ColumnToTableMap(CatalogContext catalog_context){
        for (Table table : catalog_context.getDataTables()) {
            if (table.getIsreplicated()) continue;
            
            String tableName = table.getName().toLowerCase();
          
            Column[] cols = new Column[table.getPartitioncolumns().size()];
            for(ColumnRef colRef : table.getPartitioncolumns().values()) {
                cols[colRef.getIndex()] = colRef.getColumn();
            }
            
            // partition columns may not have been set
            Column partitionCol;
            if (cols.length == 0) {
                partitionCol = table.getPartitioncolumn();
//                if (partitionCol != null) {
//                    table_partition_cols_map.put(tableName, new Column[]{partitionCol});
//                }
            }
            else {
                partitionCol = cols[0];
//                table_partition_cols_map.put(tableName, cols);
            }
            if (partitionCol == null) {
                LOG.info(String.format("Partition col for table %s is null. Skipping", tableName));
            } else {
                LOG.info(String.format("Adding table:%s partitionCol:%s %s", tableName, partitionCol, VoltType.get(partitionCol.getType())));
                this.column_to_table_map.put(partitionCol, table);
            }
        }
    }
    
    public Table getTable(Column col){
        return column_to_table_map.get(col);
    }
}
