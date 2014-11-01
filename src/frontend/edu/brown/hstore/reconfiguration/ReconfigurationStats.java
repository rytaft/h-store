/**
 * 
 */
package edu.brown.hstore.reconfiguration;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.statistics.FastIntHistogram;

/**
 * @author aelmore
 *
 */
public class ReconfigurationStats {
    private static final Logger LOG = Logger.getLogger(ReconfigurationStats.class);
    private static final LoggerBoolean debug = new LoggerBoolean(); 
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.setupLogging();
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private boolean on;
    private StringBuilder messages;
    private List<PullStat> pullsReceived;
    private List<PullStat> pullResponses;
    private List<EEStat> eeStats;
    private List<Stat> events;
    public FastIntHistogram asyncRequestTimeLockQSize;
    public long minAsyncRequestTime = 200;
    public long maxAsyncRequestTime = 10000;
    public long asyncRequestTime = minAsyncRequestTime;


    public FastIntHistogram asyncTimeToAnswerLockQSize;
    public long minTimeToAnswerAsync = 100;
    public long maxTimeToAnswerAsync = 1500;
    public long timeToAnswerAsync = minTimeToAnswerAsync;
    
    

    public int adaptive_min_bytes;
    public int adaptive_max_bytes;
    public int adaptive_cur_bytes;
    public FastIntHistogram queueSizeHist;
    
   
    /**
     * 
     */
    public ReconfigurationStats() {
        on = true;
        messages = new StringBuilder();
        pullsReceived = new ArrayList<>();
        pullResponses = new ArrayList<>();
        eeStats = new ArrayList<>();
        events = new ArrayList<>();
        asyncTimeToAnswerLockQSize = new FastIntHistogram();
        asyncRequestTimeLockQSize = new FastIntHistogram();
        this.adaptive_min_bytes = 1024;
        this.adaptive_max_bytes = 20*1024; 
        this.adaptive_cur_bytes = adaptive_min_bytes;
        this.queueSizeHist = new FastIntHistogram();
    }
    
    public void resetAdaptive(){
        timeToAnswerAsync = minTimeToAnswerAsync;
    }
    
    public long updateTimeToAnswerAsync(int queueSize){
        asyncTimeToAnswerLockQSize.put(queueSize);
        long old = timeToAnswerAsync;
        if (queueSize < 100) {
            timeToAnswerAsync*=.40;
        } else if (queueSize < 300) {
            timeToAnswerAsync*=.80;            
        } else if (queueSize < 300) {
            LOG.info("default time");
            return timeToAnswerAsync;            
        } else if (queueSize < 500) {
           timeToAnswerAsync*=1.05;
            
        } else if (queueSize > 700) {
            timeToAnswerAsync*=1.10;            
        }  
        timeToAnswerAsync = Math.min(timeToAnswerAsync, maxTimeToAnswerAsync);
        timeToAnswerAsync = Math.max(timeToAnswerAsync, minTimeToAnswerAsync);
        LOG.info(String.format("Q:%s Updating time to answer from %s to %s",queueSize, old,timeToAnswerAsync));
        return timeToAnswerAsync;        
    }
    
    
    public int getAdaptiveExctactSize(int queueSize) {
        this.queueSizeHist.put(queueSize);
        if (queueSize < 50) {
            adaptive_cur_bytes = (int)(adaptive_cur_bytes*1.40);
            LOG.info(String.format(" Adaptive Sizing * 1.40 Size:%s Q:%s", adaptive_cur_bytes, queueSize));
        }
        else if (queueSize < 100) {
            adaptive_cur_bytes = (int)(adaptive_cur_bytes*1.20);
            LOG.info(String.format(" Adaptive Sizing * 1.20 Size:%s Q:%s", adaptive_cur_bytes, queueSize));
        }
        else if (queueSize > 400) {
            adaptive_cur_bytes = (int)(adaptive_cur_bytes*.80);
            LOG.info(String.format(" Adaptive Sizing * .90 Size:%s Q:%s", adaptive_cur_bytes, queueSize));
        }
        else if (queueSize > 500) {
            adaptive_cur_bytes = (int)(adaptive_cur_bytes*.50);
            LOG.info(String.format(" Adaptive Sizing * .50 Size:%s Q:%s", adaptive_cur_bytes, queueSize));
        }

        adaptive_cur_bytes = Math.min(adaptive_max_bytes, adaptive_cur_bytes);
        adaptive_cur_bytes = Math.max(adaptive_min_bytes, adaptive_cur_bytes);
        return adaptive_cur_bytes; 
    }
    
    public void addMessage(String m){
        messages.append(m);
        messages.append(System.lineSeparator());
    }
    
    
    public void trackExtract(int partitionId, String tableName, int rowCount, int loadSizeKB, long timeTaken, int entryId, boolean isLive, int pullId, int chunkId, Number minKey, Number maxKey) {
        if (on){
            long ts = System.currentTimeMillis();
            EEStat s = new EEStat(partitionId, tableName, true, false, rowCount, loadSizeKB, timeTaken, ts, entryId, isLive, pullId, chunkId, minKey, maxKey);
            events.add(s);
            eeStats.add(s);
            
        }
    }
    

    
    public void trackLoad(int partitionId, String tableName, int rowCount, int loadSizeKB, long timeTaken, boolean isLive, int pullId, int chunkId, Number minKey, Number maxKey) {
        if (on){
            long ts = System.currentTimeMillis();
            EEStat s = new EEStat(partitionId, tableName, false, true, rowCount, loadSizeKB, timeTaken, ts, -1, isLive, pullId, 
                    chunkId, minKey, maxKey);
            events.add(s);
            eeStats.add(s);
            
        }
    }

    public void trackAsyncSrcResponse(int sourcePartitionId, int destPartitionId, String voltTableName, int responseSizeKB, boolean moreDataNeeded) {
        if (on){
            long ts = System.currentTimeMillis();
            PullStat s = new PullStat(destPartitionId, sourcePartitionId, voltTableName, responseSizeKB, -1, true, moreDataNeeded, ts);
            pullResponses.add(s);
            events.add(s);
            
        }
    }

    public void trackAsyncPullInit(int partitionId, int destPartitionId, String tableName) {
        if (on){
//            long ts = System.currentTimeMillis();
            
        }
    }

    public void trackAsyncReceived(int destId, int srcId, String table_name, int responseSizeKB, long timeTaken, boolean isAsyncRequest, boolean moreDataComing) {
        if (on){
            long ts = System.currentTimeMillis();
            PullStat s = new PullStat(destId, srcId, table_name, responseSizeKB, timeTaken, isAsyncRequest, moreDataComing, ts);
            pullsReceived.add(s);    
            events.add(s);        
        }
    }
    
    public void trackLivePull(int destId, int srcId, int responseSizeKB, long timeTaken, int queueGrowth) {
        if (on){
            long ts = System.currentTimeMillis();
            PullStat s = new PullStat(destId, srcId, "", responseSizeKB, timeTaken, false, false, queueGrowth, ts);
            pullsReceived.add(s);    
            events.add(s);        
        }
    }
    
    public interface Stat {
        public String toString();
    }
    
    public class EEStat implements Stat {
        int partId;
        String tableName;
        boolean isExtract;
        boolean isLoad;
        int rowCount;
        int sizeKb;
        long timeTaken;
        int queueGrowth;
        boolean hasQueueStats;
        long ts;
        int entryId;
        boolean isLive;
        private int chunkId;
        private int pullId;
        Number minKey;
        Number maxKey;
        
        public EEStat(int partId, String tableName, boolean isExtract, boolean isLoad, int rowCount, int sizeKb, long timeTaken, long ts, int entryId, boolean isLive, int pullId, int chunkId, Number minKey, Number maxKey) {
            super();
            this.partId = partId;
            this.tableName = tableName;
            this.isExtract = isExtract;
            this.isLoad = isLoad;
            this.rowCount = rowCount;
            this.sizeKb = sizeKb;
            this.timeTaken = timeTaken;
            this.hasQueueStats = false;
            this.ts = ts;
            this.entryId=entryId;
            this.isLive = isLive;
            this.pullId = pullId;
            this.chunkId = chunkId;
            this.minKey = minKey;
            this.maxKey = maxKey;
        }
        


        @Override
        public String toString() {
            return "EEStat [partId=" + partId + ", tableName=" + tableName + ", isExtract=" + isExtract + ", isLoad=" + isLoad + ", rowCount=" + rowCount + ", sizeKb=" + sizeKb + ", timeTaken="
                    + timeTaken + ", queueGrowth=" + queueGrowth + ", hasQueueStats=" + hasQueueStats + ", ts=" + ts + ", entryId=" + entryId + ", isLive=" + isLive + ", chunkId=" + chunkId
                    + ", pullId=" + pullId + "]";
        }
        
        public String toCSVString(){
            
            return  partId + "," + tableName + "," + isExtract + "," + isLoad + "," + rowCount + "," + sizeKb + ","
                    + timeTaken + "," + queueGrowth + "," + hasQueueStats + "," + ts + "," + entryId + "," + isLive + "," + pullId + ","+ chunkId + ","+ minKey + ","+ maxKey ;
        }
    }

    public static final Object FORMAT_VERSION = "1";
    public static String getEEHeader() {
        return "partId,tableName,isExtract,isLoad,rowCount,sizeKb," +
                "timeTaken,queueGrowth,hasQueueStats,ts,entryId,isLive,pullId,chunkId,minKey,maxKey";
    }
    

    public class PullStat implements Stat {
        int destId;
        int srcId;
        String tableName;
        int responseSizeKB;
        long timeTaken;
        boolean isAsync;
        boolean moreData;
        int queueGrowth;
        boolean hasQueueStats;
        long ts;
        
        public PullStat(int destId, int srcId, String tableName, int responseSizeKB, long timeTaken, boolean isAsync, boolean moreData, long ts) {
            super();
            this.destId = destId;
            this.srcId = srcId;
            this.tableName = tableName;
            this.responseSizeKB = responseSizeKB;
            this.timeTaken = timeTaken;
            this.isAsync = isAsync;
            this.moreData = moreData;
            this.hasQueueStats = false;
            this.ts = ts;
        }
        
        public PullStat(int destId, int srcId, String tableName, int responseSizeKB, long timeTaken, boolean isAsync, boolean moreData, int queueGrowth, long ts) {
            super();
            this.destId = destId;
            this.srcId = srcId;
            this.tableName = tableName;
            this.responseSizeKB = responseSizeKB;
            this.timeTaken = timeTaken;
            this.isAsync = isAsync;
            this.moreData = moreData;
            this.queueGrowth = queueGrowth;
            this.hasQueueStats = true;
            this.ts = ts;
        }

        @Override
        public String toString() {
            return "PullStat [destId=" + destId + ", srcId=" + srcId + ", tableName=" + tableName + ", responseSizeKB=" + responseSizeKB + ", timeTaken=" + timeTaken + ", isAsync=" + isAsync
                    + ", moreData=" + moreData + ", queueGrowth=" + queueGrowth + ", hasQueueStats=" + hasQueueStats + ", ts=" + ts + "]";
        }
        
        
    }


    public List<Stat> getEvents() {
        return events;
    }

    public List<EEStat> getEeStats() {
        return eeStats;
    }



    

    
    
}
