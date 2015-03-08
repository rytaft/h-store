package com.oltpbenchmark.benchmarks.twitter.procedures;


import com.oltpbenchmark.benchmarks.twitter.TwitterConstants;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

@ProcInfo(
		singlePartition = false
)
public class GetFollowingCount extends VoltProcedure {
    
    public final SQLStmt getFollowingCount = new SQLStmt(
            "SELECT f1, count(*) FROM " + TwitterConstants.TABLENAME_FOLLOWS +
            " GROUP BY f1"
        );
    
    public VoltTable[] run() {
    	voltQueueSQL(getFollowingCount);
    	return voltExecuteSQL(true);
    }

}
