package edu.brown.hstore.reconfiguration;

import java.util.List;

import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltTable.ColumnInfo;

import edu.brown.hashing.ReconfigurationPlan.ReconfigurationRange;

public class ReconfigurationUtil {

    public static VoltTable getExtractVoltTable(ReconfigurationRange range) {
        ColumnInfo extractTableColumns[] = new ColumnInfo[4];

        extractTableColumns[0] = new ColumnInfo("TABLE_NAME", VoltType.INTEGER);
        extractTableColumns[1] = new ColumnInfo("KEY_TYPE", VoltType.INTEGER);
        extractTableColumns[2] = new ColumnInfo("MIN_INCLUSIVE", VoltType.BIGINT); // range.getVt());
        extractTableColumns[3] = new ColumnInfo("MAX_EXCLUSIVE", VoltType.BIGINT);// range.getVt());

        VoltTable vt = new VoltTable(extractTableColumns);
        // vt.addRow(range.table_name,range.getVt().toString(),range.getMin_inclusive(),range.getMax_exclusive());
        vt.addRow(1, 1, range.getMin_inclusive(), range.getMax_exclusive());

        return vt;
    }

    public static VoltTable getExtractVoltTable(List<ReconfigurationRange> ranges) {
        ColumnInfo extractTableColumns[] = new ColumnInfo[4];

        extractTableColumns[0] = new ColumnInfo("TABLE_NAME", VoltType.INTEGER);
        extractTableColumns[1] = new ColumnInfo("KEY_TYPE", VoltType.INTEGER);
        extractTableColumns[2] = new ColumnInfo("MIN_INCLUSIVE", VoltType.BIGINT); // range.getVt());
        extractTableColumns[3] = new ColumnInfo("MAX_EXCLUSIVE", VoltType.BIGINT);// range.getVt());

        VoltTable vt = new VoltTable(extractTableColumns);
        // vt.addRow(range.table_name,range.getVt().toString(),range.getMin_inclusive(),range.getMax_exclusive());
        for (ReconfigurationRange range : ranges) {
            vt.addRow(1, 1, range.getMin_inclusive(), range.getMax_exclusive());
        }

        return vt;
    }
}
