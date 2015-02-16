package com.oltpbenchmark.benchmarks.twitter.procedures;

import com.oltpbenchmark.benchmarks.twitter.TwitterConstants;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class GetTweet extends VoltProcedure {

    public SQLStmt getTweet = new SQLStmt(
        "SELECT * FROM " + TwitterConstants.TABLENAME_TWEETS + " WHERE id = ?"
    );

    public VoltTable[] run(long tweet_id) {
    	voltQueueSQL(getTweet, tweet_id);
    	return voltExecuteSQL(true);
    }
}
