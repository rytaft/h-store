package edu.mit.benchmark.affinity.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class GetA extends VoltProcedure {
    public final SQLStmt GetA = new SQLStmt("SELECT * FROM TABLEA WHERE A_KEY = ? ");
    
    public VoltTable[] run(long a_key){
        voltQueueSQL(GetA, a_key);
        return (voltExecuteSQL());
    }

}
