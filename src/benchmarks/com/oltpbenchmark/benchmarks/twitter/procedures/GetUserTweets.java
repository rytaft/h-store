package com.oltpbenchmark.benchmarks.twitter.procedures;

import com.oltpbenchmark.benchmarks.twitter.TwitterConstants;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

@ProcInfo(
		partitionInfo = "TWEETS.UID: 0",
        singlePartition = true
)
public class GetUserTweets extends VoltProcedure {

    public final SQLStmt getTweets = new SQLStmt(
        "SELECT * FROM " + TwitterConstants.TABLENAME_TWEETS +
        " WHERE uid = ? LIMIT " + TwitterConstants.LIMIT_TWEETS_FOR_UID
    );
    
    public VoltTable[] run(long uid) {
    	voltQueueSQL(getTweets, uid);
    	return voltExecuteSQL(true);
    }
}
