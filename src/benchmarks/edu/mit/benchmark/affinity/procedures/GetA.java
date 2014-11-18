package edu.mit.benchmark.affinity.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

@ProcInfo(
        partitionInfo = "TABLEA.A_KEY: 0",
        singlePartition = true
    )
public class GetA extends VoltProcedure {
    public final SQLStmt getAStmt = new SQLStmt("SELECT * FROM TABLEA WHERE A_KEY = ? ");
    
    public VoltTable[] run(long a_key){
        voltQueueSQL(getAStmt, a_key);
        return (voltExecuteSQL());
    }

}
