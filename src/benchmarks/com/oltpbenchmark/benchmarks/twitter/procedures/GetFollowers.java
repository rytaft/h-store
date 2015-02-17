package com.oltpbenchmark.benchmarks.twitter.procedures;


import com.oltpbenchmark.benchmarks.twitter.TwitterConstants;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

@ProcInfo(
		partitionInfo = "FOLLOWERS.F1: 0",
        singlePartition = true
)
public class GetFollowers extends VoltProcedure {
	
    public final SQLStmt getFollowers = new SQLStmt(
        "SELECT f2 FROM " + TwitterConstants.TABLENAME_FOLLOWERS +
		" WHERE f1 = ? LIMIT " + TwitterConstants.LIMIT_FOLLOWERS
    );
    
    /** NOTE: The ?? is substituted into a string of repeated ?'s */
    public final SQLStmt getFollowerName = new SQLStmt(
        "SELECT uid, name FROM " + TwitterConstants.TABLENAME_USER + 
        " WHERE uid = ?"
    );
    
    public VoltTable[] run(long uid) {
    	voltQueueSQL(getFollowers, uid);
    	VoltTable result[] = voltExecuteSQL();
        
        int num_params = Math.min(result[1].getRowCount(), TwitterConstants.LIMIT_FOLLOWERS);
        for(int i = 0; i < num_params; ++i) {
        	voltQueueSQL(getFollowerName, result[1].fetchRow(i).getLong(0));
        }
        
        if (num_params > 0) {
        	return voltExecuteSQL(true);
        }
        
        return null;
    }

}
