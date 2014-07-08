/**
 * 
 */
package edu.brown.hstore.reconfiguration;

import java.util.ArrayList;
import java.util.List;

/**
 * @author aelmore
 *
 */
public class ReconfigurationStats {

    
    private boolean on;
    private StringBuilder messages;
    private List<PullStat> pullsReceived;
    private List<PullStat> pullResponses;
    private List<EEStat> eeStats;
    private List<Stat> events;
    
   
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
    }
    
    public void addMessage(String m){
        messages.append(m);
        messages.append(System.lineSeparator());
    }
    
    
    public void trackExtract(int partitionId, String tableName, int rowCount, int loadSizeKB, long timeTaken, int entryId, boolean isLive) {
        if (on){
            long ts = System.currentTimeMillis();
            EEStat s = new EEStat(partitionId, tableName, true, false, rowCount, loadSizeKB, timeTaken, ts, entryId, isLive);
            events.add(s);
            eeStats.add(s);
            
        }
    }
    
    public void trackLoad(int partitionId, String tableName, int rowCount, int loadSizeKB, long timeTaken,int queueGrowth) {
        if (on){
            long ts = System.currentTimeMillis();
            EEStat s = new EEStat(partitionId, tableName, false, true, rowCount, loadSizeKB, timeTaken, queueGrowth, ts, -1);
            events.add(s);
            eeStats.add(s);
        }
    }
    
    public void trackLoad(int partitionId, String tableName, int rowCount, int loadSizeKB, long timeTaken, boolean isLive) {
        if (on){
            long ts = System.currentTimeMillis();
            EEStat s = new EEStat(partitionId, tableName, false, true, rowCount, loadSizeKB, timeTaken, ts, -1, isLive);
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
        
        public EEStat(int partId, String tableName, boolean isExtract, boolean isLoad, int rowCount, int sizeKb, long timeTaken, long ts, int entryId, boolean isLive) {
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
            
        }
        
        public EEStat(int partId, String tableName, boolean isExtract, boolean isLoad, int rowCount, int sizeKb,long timeTaken, int queueGrowth, long ts, int entryId) {
            super();
            this.partId = partId;
            this.tableName = tableName;
            this.isExtract = isExtract;
            this.isLoad = isLoad;
            this.rowCount = rowCount;
            this.sizeKb = sizeKb;
            this.timeTaken = timeTaken;
            this.queueGrowth = queueGrowth;
            this.hasQueueStats = true;
            this.ts = ts;
            this.entryId=entryId;
        }

        @Override
        public String toString() {
            return "EEStat [partId=" + partId + ", tableName=" + tableName + ", isExtract=" + isExtract + ", isLoad=" + isLoad + ", rowCount=" + rowCount + ", sizeKb=" + sizeKb + ", timeTaken="
                    + timeTaken + ", queueGrowth=" + queueGrowth + ", hasQueueStats=" + hasQueueStats + ", ts=" + ts + ", entryId=" + entryId + ", isLive=" + isLive + "]";
        }
        
        public String toCSVString(){
            
            return  partId + "," + tableName + "," + isExtract + "," + isLoad + "," + rowCount + "," + sizeKb + ","
                    + timeTaken + "," + queueGrowth + "," + hasQueueStats + "," + ts + "," + entryId + "," + isLive + "";
        }
    }

    public static final Object FORMAT_VERSION = "1";
    public static String getEEHeader() {
        return "partId,tableName,isExtract,isLoad,rowCount,sizeKb," +
                "timeTaken,queueGrowth,hasQueueStats,ts,entryId,isLive";
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
