package com.oltpbenchmark.benchmarks.twitter.procedures;


import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.oltpbenchmark.benchmarks.twitter.TwitterConstants;

import edu.brown.benchmark.ycsb.distributions.ZipfianGenerator;

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
		" WHERE f1 = ?"
    );
    
    /** NOTE: The ?? is substituted into a string of repeated ?'s */
    public final SQLStmt getFollowerName = new SQLStmt(
        "SELECT uid, name FROM " + TwitterConstants.TABLENAME_USER + 
        " WHERE uid = ?"
    );
    
    public VoltTable[] run(long uid) {
    	voltQueueSQL(getFollowers, uid);
    	final VoltTable result[] = voltExecuteSQL();
    	assert result.length == 1;
        
        if (Math.min(result[0].getRowCount(), TwitterConstants.LIMIT_FOLLOWERS) > 0) {
            long[] followers = new long[result[0].getRowCount()];

            // get the list of followers
            for (int i = 0; i < result[0].getRowCount(); ++i) {
                followers[i] = result[0].fetchRow(i).getLong(0);
            }

            // The chosen set of followers will follow a zipfian distribution
            // without replacement
            Arrays.sort(followers);
            ZipfianGenerator r = new ZipfianGenerator(followers.length);
            Set<Integer> indices = new HashSet<>();
            for (int i = 0; i < TwitterConstants.LIMIT_FOLLOWERS && i < followers.length; ++i) {
                Integer index = r.nextInt();
                while (indices.contains(index)) {
                    index = (index + 1) % followers.length;
                }
                indices.add(index);
            }
            for (Integer index : indices) {
                voltQueueSQL(getFollowerName, followers[index]);
            }
        
        	return voltExecuteSQL(true);
        }
        
        return null;
    }

}
