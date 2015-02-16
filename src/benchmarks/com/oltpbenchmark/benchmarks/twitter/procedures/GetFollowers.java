package com.oltpbenchmark.benchmarks.twitter.procedures;


import com.oltpbenchmark.benchmarks.twitter.TwitterConstants;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;


public class GetFollowers extends VoltProcedure {
	
    public final SQLStmt getFollowers = new SQLStmt(
        "SELECT f2 FROM " + TwitterConstants.TABLENAME_FOLLOWERS +
		" WHERE f1 = ? LIMIT " + TwitterConstants.LIMIT_FOLLOWERS
    );
    
    /** NOTE: The ?? is substituted into a string of repeated ?'s */
    public final SQLStmt getFollowerNames = new SQLStmt(
        "SELECT uid, name FROM " + TwitterConstants.TABLENAME_USER + 
        " WHERE uid IN (??)", TwitterConstants.LIMIT_FOLLOWERS
    );
    
    public VoltTable[] run(long uid) {
    	voltQueueSQL(getFollowers, uid);
    	VoltTable result[] = voltExecuteSQL();
        
        int num_params = Math.min(result[1].getRowCount(), TwitterConstants.LIMIT_FOLLOWERS);
        Object params[] = new Object[num_params];
        for(int i = 0; i < num_params; ++i) {
        	params[i] = result[1].fetchRow(i).getLong(0);
        }
        
        if (num_params > 0) {
            for(int i = num_params; i < TwitterConstants.LIMIT_FOLLOWERS; ++i) {
                params[i] = params[num_params-1];
            } // WHILE
            voltQueueSQL(getFollowerNames, params);
            return voltExecuteSQL(true);
        }
        
        return null;
    }

}
