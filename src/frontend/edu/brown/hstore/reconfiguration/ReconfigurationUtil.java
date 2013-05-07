package edu.brown.hstore.reconfiguration;

import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltTable.ColumnInfo;

import edu.brown.hashing.ReconfigurationPlan.ReconfigurationRange;

public class ReconfigurationUtil {
    



    
    public static VoltTable getExtractVoltTable(ReconfigurationRange range){
        ColumnInfo extractTableColumns[] = new ColumnInfo[4];

        extractTableColumns[0] = new ColumnInfo("TABLE", VoltType.STRING);
        extractTableColumns[1] = new ColumnInfo("KEY_TYPE", VoltType.STRING);
        extractTableColumns[2] = new ColumnInfo("MIN_INCLUSIVE", range.getVt());
        extractTableColumns[3] = new ColumnInfo("MAX_EXCLUSIVE", range.getVt());
        
        VoltTable vt = new VoltTable(extractTableColumns);
        vt.addRow(range.table_name,range.getVt().toString(),range.getMin_inclusive(),range.getMax_exclusive());
        return vt;
    }
}
