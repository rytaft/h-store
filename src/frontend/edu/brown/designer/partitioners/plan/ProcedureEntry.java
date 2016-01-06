package edu.brown.designer.partitioners.plan;

import org.voltdb.catalog.Column;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Table;
import org.voltdb.types.PartitionMethodType;

public class ProcedureEntry extends PartitionEntry<ProcParameter> {

    // Procedure Information
    public Boolean single_partition;
    public Table table;
    public Column column;

    public ProcedureEntry() {
        // For serialziation
    }

    public ProcedureEntry(PartitionMethodType method) {
        this(method, null, null, null, null);
    }

    public ProcedureEntry(PartitionMethodType method, ProcParameter catalog_param, Boolean single_partition) {
        this(method, catalog_param, single_partition, null, null);
    }

    public ProcedureEntry(PartitionMethodType method, ProcParameter catalog_param, Boolean single_partition, Table table, Column column) {
        super(method, catalog_param);
        this.single_partition = single_partition;
        this.table = table;
        this.column = column;
    }

    /**
     * Is the procedure this entry guaranteed to be single-partition?
     * 
     * @return
     */
    public Boolean isSinglePartition() {
        return this.single_partition;
    }

    public void setSinglePartition(boolean singlePartition) {
        this.single_partition = singlePartition;
    }

    public Table getTable() {
        return this.table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public Column getColumn() {
        return this.column;
    }

    public void setColumn(Column column) {
        this.column = column;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ProcedureEntry)) {
            return (false);
        }
        ProcedureEntry other = (ProcedureEntry) obj;

        // SinglePartition
        if (this.single_partition == null) {
            if (other.single_partition != null) {
                return (false);
            }
        } else if (!this.single_partition.equals(other.single_partition)) {
            return (false);
        }

        // Table
        if (this.table == null) {
            if (other.table != null) {
                return (false);
            }
        } else if (!this.table.equals(other.table)) {
            return (false);
        }

        // Column
        if (this.column == null) {
            if (other.column != null) {
                return (false);
            }
        } else if (!this.column.equals(other.column)) {
            return (false);
        }

        return (super.equals(other));
    }

}
