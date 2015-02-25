package com.oltpbenchmark.benchmarks.twitter;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;

import com.oltpbenchmark.benchmarks.twitter.util.NameHistogram;
import com.oltpbenchmark.benchmarks.twitter.util.TweetHistogram;
import com.oltpbenchmark.benchmarks.twitter.util.TwitterGraphEdge;
import com.oltpbenchmark.benchmarks.twitter.util.TwitterGraphLoader;
import com.oltpbenchmark.distributions.ScrambledZipfianGenerator;
import com.oltpbenchmark.distributions.ZipfianGenerator;
import com.oltpbenchmark.util.TextGenerator;

import edu.brown.api.Loader;
import edu.brown.catalog.CatalogUtil;
import edu.brown.rand.RandomDistribution.FlatHistogram;

import org.voltdb.CatalogContext;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;

public class TwitterLoader extends Loader {
    private static final Logger LOG = Logger.getLogger(TwitterLoader.class);

    public final static int configCommitCount = 1000;

    private final int num_users;
    private final long num_tweets;
    private final int num_follows;
    private HashSet<Integer> users;
    
    private final Random rng = new Random();

    private TwitterGraphLoader graph_loader;
    private boolean use_network_file;
    private int max_user_id = -1;
    
    public TwitterLoader(String args[]) {
        super(args);
        
        for (String key : m_extraParams.keySet()) {
            String value = m_extraParams.get(key);

            if  (key.equalsIgnoreCase("network_file")) {
            	String filename = String.valueOf(value);
            	use_network_file = true;
            	try {
            		graph_loader = new TwitterGraphLoader(filename);
            	}
            	catch(FileNotFoundException e) {
            		throw new RuntimeException(e);
            	}
            }
            else if  (key.equalsIgnoreCase("max_user_id")) {
            	max_user_id = Integer.valueOf(value);
            }
        }
        
        if(use_network_file && max_user_id != -1) {
        	graph_loader.setMaxUserId(max_user_id);
        }
        
        this.users = new HashSet<>();
        this.num_users = (int)Math.round(TwitterConstants.NUM_USERS * this.getScaleFactor());
        this.num_tweets = (int)Math.round(TwitterConstants.NUM_TWEETS * this.getScaleFactor());
        this.num_follows = (int)Math.round(TwitterConstants.MAX_FOLLOW_PER_USER * this.getScaleFactor());
        if (LOG.isDebugEnabled()) {
            LOG.debug("# of USERS:  " + this.num_users);
            LOG.debug("# of TWEETS: " + this.num_tweets);
            LOG.debug("# of FOLLOWS: " + this.num_follows);
        }
    }
    
    /**
     * @author Djellel
     * Load num_users users.
     * @throws IOException
     */
    protected void loadUsers(Database catalog_db) throws IOException {
    	if(this.use_network_file) {
    		loadUsersGraph(catalog_db);
    	}
    	else {
    		loadUsersDefault(catalog_db);
    	}
    }
    
    protected void loadUsersDefault(Database catalog_db) throws IOException {
        Table catalog_tbl = catalog_db.getTables().getIgnoreCase(TwitterConstants.TABLENAME_USER);
        assert(catalog_tbl != null);
        VoltTable vt = CatalogUtil.getVoltTable(catalog_tbl);
        int num_cols = catalog_tbl.getColumns().size();
            
        NameHistogram name_h = new NameHistogram();
        FlatHistogram<Integer> name_len_rng = new FlatHistogram<Integer>(this.rng, name_h);
        
        int total = 0;
        int batchSize = 0;
        
        for (int i = 0; i < this.num_users; i++) {
        	// Generate a random username for this user
        	int name_length = name_len_rng.nextValue().intValue();
            String name = TextGenerator.randomStr(rng, name_length);
            
            Object row[] = new Object[num_cols];
            int param = 0;
            row[param++] = i; // ID
            row[param++] = name; // NAME
            row[param++] = name + "@tweeter.com"; // EMAIL
            row[param++] = VoltType.NULL_INTEGER;
            row[param++] = VoltType.NULL_TINYINT;
            row[param++] = VoltType.NULL_INTEGER;
            vt.addRow(row);
            
            batchSize++;
            total++;
            if ((batchSize % configCommitCount) == 0) {
                this.loadVoltTable(catalog_tbl.getName(), vt);
                vt.clearRowData();
                batchSize = 0;
                if (LOG.isDebugEnabled())
                    LOG.debug(String.format("Users %d / %d", total, num_users));
            }
        } // FOR
        if (batchSize > 0) {
        	this.loadVoltTable(catalog_tbl.getName(), vt);
            vt.clearRowData();
        }
        
        if (LOG.isDebugEnabled()) LOG.debug(String.format("Users Loaded [%d]", total));
    }

    protected void loadUsersGraph(Database catalog_db) throws IOException {
    	if(users.size() == 0) {
    		throw new RuntimeException("No users provided to loadUsersGraph()");
    	}
    	
    	Table catalog_tbl = catalog_db.getTables().getIgnoreCase(TwitterConstants.TABLENAME_USER);
        assert(catalog_tbl != null);
        VoltTable vt = CatalogUtil.getVoltTable(catalog_tbl);
        int num_cols = catalog_tbl.getColumns().size();
            
        NameHistogram name_h = new NameHistogram();
        FlatHistogram<Integer> name_len_rng = new FlatHistogram<Integer>(this.rng, name_h);
        
        int total = 0;
        int batchSize = 0;
        
        for (Integer user : this.users) {
        	// Generate a random username for this user
        	int name_length = name_len_rng.nextValue().intValue();
            String name = TextGenerator.randomStr(rng, name_length);
            
            Object row[] = new Object[num_cols];
            int param = 0;
            row[param++] = user.intValue(); // ID
            row[param++] = name; // NAME
            row[param++] = name + "@tweeter.com"; // EMAIL
            row[param++] = VoltType.NULL_INTEGER;
            row[param++] = VoltType.NULL_TINYINT;
            row[param++] = VoltType.NULL_INTEGER;
            vt.addRow(row);
            
            batchSize++;
            total++;
            if ((batchSize % configCommitCount) == 0) {
                this.loadVoltTable(catalog_tbl.getName(), vt);
                vt.clearRowData();
                batchSize = 0;
                if (LOG.isDebugEnabled())
                    LOG.debug(String.format("Users %d / %d", total, num_users));
            }
        } // FOR
        if (batchSize > 0) {
        	this.loadVoltTable(catalog_tbl.getName(), vt);
            vt.clearRowData();
        }
        
        if (LOG.isDebugEnabled()) LOG.debug(String.format("Users Loaded [%d]", total));
    }

    
    /**
     * @author Djellel
     * What's going on here?: 
     * The number of tweets is fixed to num_tweets
     * We simply select using the distribution who issued the tweet
     * @throws IOException
     */
    protected void loadTweets(Database catalog_db) throws IOException {
        Table catalog_tbl = catalog_db.getTables().getIgnoreCase(TwitterConstants.TABLENAME_TWEETS);
        assert(catalog_tbl != null);
        VoltTable vt = CatalogUtil.getVoltTable(catalog_tbl);
        int num_cols = catalog_tbl.getColumns().size();
        
        int total = 0;
        int batchSize = 0;
        ScrambledZipfianGenerator zy = new ScrambledZipfianGenerator(this.num_users);
        
        TweetHistogram tweet_h = new TweetHistogram();
        FlatHistogram<Integer> tweet_len_rng = new FlatHistogram<Integer>(this.rng, tweet_h);
        
        for (long i = 0; i < this.num_tweets; i++) {
            int uid = zy.nextInt();
            
            Object row[] = new Object[num_cols];
            int param = 0;
            row[param++] = i;
            row[param++] = uid;
            row[param++] = TextGenerator.randomStr(rng, tweet_len_rng.nextValue());
            row[param++] = VoltType.NULL_TIMESTAMP;
            vt.addRow(row);
            
            batchSize++;
            total++;

            if ((batchSize % configCommitCount) == 0) {
            	this.loadVoltTable(catalog_tbl.getName(), vt);
                vt.clearRowData();
                batchSize = 0;
                if (LOG.isDebugEnabled()) 
                    LOG.debug("tweet % " + total + "/"+this.num_tweets);
            }
        }
        if (batchSize > 0) {
        	this.loadVoltTable(catalog_tbl.getName(), vt);
            vt.clearRowData();
        }

        if (LOG.isDebugEnabled()) 
            LOG.debug("[Tweets Loaded] "+ this.num_tweets);
    }
    
    /**
     * @author Djellel
     * What's going on here?: 
     * For each user (follower) we select how many users he is following (followees List)
     * then select users to fill up that list.
     * Selecting is based on the distribution.
     * NOTE: We are using two different distribution to avoid correlation:
     * ZipfianGenerator (describes the followed most) 
     * ScrambledZipfianGenerator (describes the heavy tweeters)
     * @throws IOException
     */
    protected void loadFollowData(Database catalog_db) throws IOException {
    	if(this.use_network_file) {
    		loadFollowDataGraph(catalog_db);
    	}
    	else {
    		loadFollowDataDefault(catalog_db);
    	}
    }


    protected void loadFollowDataDefault(Database catalog_db) throws IOException {
        Table catalog_tbl_follows = catalog_db.getTables().getIgnoreCase(TwitterConstants.TABLENAME_FOLLOWS);
        assert(catalog_tbl_follows != null);
        VoltTable vt_follows = CatalogUtil.getVoltTable(catalog_tbl_follows);
        int num_cols_follows = catalog_tbl_follows.getColumns().size();
        
        Table catalog_tbl_followers = catalog_db.getTables().getIgnoreCase(TwitterConstants.TABLENAME_FOLLOWERS);
        assert(catalog_tbl_followers != null);
        VoltTable vt_followers = CatalogUtil.getVoltTable(catalog_tbl_followers);
        int num_cols_followers = catalog_tbl_followers.getColumns().size();
        
        int total = 1;
        int batchSize = 0;
        
        ZipfianGenerator zipfFollowee = new ZipfianGenerator(this.num_users,1.75);
        ZipfianGenerator zipfFollows = new ZipfianGenerator(this.num_follows,1.75);
        List<Integer> followees = new ArrayList<Integer>();
        for (int follower = 0; follower < this.num_users; follower++) {
            followees.clear();
            int time = zipfFollows.nextInt();
            if(time==0) time=1; // At least this follower will follow 1 user 
            for (int f = 0; f < time; ) {
                int followee = zipfFollowee.nextInt();
                if (follower != followee && !followees.contains(followee)) {
                	Object row_follows[] = new Object[num_cols_follows];
                    int param = 0;
                    row_follows[param++] = follower;
                    row_follows[param++] = followee;
                    vt_follows.addRow(row_follows);

                    Object row_followers[] = new Object[num_cols_followers];
                    param = 0;
                    row_followers[param++] = followee;
                    row_followers[param++] = follower;
                    vt_followers.addRow(row_followers);

                    followees.add(followee);
                    
                    total++;
                    batchSize++;
                    f++;

                    if ((batchSize % configCommitCount) == 0) {
                    	this.loadVoltTable(catalog_tbl_follows.getName(), vt_follows);
                    	this.loadVoltTable(catalog_tbl_followers.getName(), vt_followers);
                        vt_follows.clearRowData();
                        vt_followers.clearRowData();
                        batchSize = 0;
                        if (LOG.isDebugEnabled()) 
                            LOG.debug("Follows  % " + (int)(((double)follower/(double)this.num_users)*100));
                    }
                }
            } // FOR
        } // FOR
        if (batchSize > 0) {
        	this.loadVoltTable(catalog_tbl_follows.getName(), vt_follows);
        	this.loadVoltTable(catalog_tbl_followers.getName(), vt_followers);
            vt_follows.clearRowData();
            vt_followers.clearRowData();    
        }
        
        if (LOG.isDebugEnabled()) LOG.debug("[Follows Loaded] "+total);
    }

    protected void loadFollowDataGraph(Database catalog_db) throws IOException {
        Table catalog_tbl_follows = catalog_db.getTables().getIgnoreCase(TwitterConstants.TABLENAME_FOLLOWS);
        assert(catalog_tbl_follows != null);
        VoltTable vt_follows = CatalogUtil.getVoltTable(catalog_tbl_follows);
        int num_cols_follows = catalog_tbl_follows.getColumns().size();
        
        Table catalog_tbl_followers = catalog_db.getTables().getIgnoreCase(TwitterConstants.TABLENAME_FOLLOWERS);
        assert(catalog_tbl_followers != null);
        VoltTable vt_followers = CatalogUtil.getVoltTable(catalog_tbl_followers);
        int num_cols_followers = catalog_tbl_followers.getColumns().size();
        
        int total = 1;
        int batchSize = 0;
        
        while(this.graph_loader.hasNext()) {
        	TwitterGraphEdge e = this.graph_loader.readNextEdge();
        	if(e == null) break;
        	Object row_follows[] = new Object[num_cols_follows];
        	int param = 0;
        	row_follows[param++] = e.follower;
        	row_follows[param++] = e.followee;
        	vt_follows.addRow(row_follows);

        	Object row_followers[] = new Object[num_cols_followers];
        	param = 0;
        	row_followers[param++] = e.followee;
        	row_followers[param++] = e.follower;
        	vt_followers.addRow(row_followers);
        	
        	users.add(e.followee);
        	users.add(e.follower);

        	total++;
        	batchSize++;

        	if ((batchSize % configCommitCount) == 0) {
        		this.loadVoltTable(catalog_tbl_follows.getName(), vt_follows);
        		this.loadVoltTable(catalog_tbl_followers.getName(), vt_followers);
        		vt_follows.clearRowData();
        		vt_followers.clearRowData();
        		batchSize = 0;
        		if (LOG.isDebugEnabled()) 
        			LOG.debug("Follows  % " + (int)(((double)e.follower/(double)this.num_users)*100));
        	}
        }

        if (batchSize > 0) {
        	this.loadVoltTable(catalog_tbl_follows.getName(), vt_follows);
        	this.loadVoltTable(catalog_tbl_followers.getName(), vt_followers);
            vt_follows.clearRowData();
            vt_followers.clearRowData();    
        }
        
        if (LOG.isDebugEnabled()) LOG.debug("[Follows Loaded] "+total);
    }

    
    @Override
    public void load() throws IOException {
    	final CatalogContext catalogContext = this.getCatalogContext();
        this.loadFollowData(catalogContext.database);
        this.loadUsers(catalogContext.database);
        this.loadTweets(catalogContext.database);
    }
}
