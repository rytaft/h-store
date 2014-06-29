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
   
    /**
     * 
     */
    public ReconfigurationStats() {
        on = true;
        messages = new StringBuilder();
        pullsReceived = new ArrayList<>();
        pullResponses = new ArrayList<>();
        eeStats = new ArrayList<>();
    }
    
    public void trackExtract(int partitionId, String tableName, int rowCount, int loadSizeKB, long timeTaken) {
        if (on){
            long ts = System.currentTimeMillis();
            eeStats.add(new EEStat(partitionId, tableName, true, false, rowCount, loadSizeKB, timeTaken, ts));
        }
    }
    
    public void trackLoad(int partitionId, String tableName, int rowCount, int loadSizeKB, long timeTaken,int queueGrowth) {
        if (on){
            long ts = System.currentTimeMillis();
            eeStats.add(new EEStat(partitionId, tableName, false, true, rowCount, loadSizeKB, timeTaken, queueGrowth, ts));
        }
    }
    
    public void trackLoad(int partitionId, String tableName, int rowCount, int loadSizeKB, long timeTaken) {
        if (on){
            long ts = System.currentTimeMillis();
            eeStats.add(new EEStat(partitionId, tableName, false, true, rowCount, loadSizeKB, timeTaken, ts));
            
        }
    }

    public void trackAsyncSrcResponse(int sourcePartitionId, int destPartitionId, String voltTableName, int responseSizeKB, boolean moreDataNeeded) {
        if (on){
            long ts = System.currentTimeMillis();
            pullResponses.add(new PullStat(destPartitionId, sourcePartitionId, voltTableName, responseSizeKB, -1, true, moreDataNeeded, ts));
            
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
            pullsReceived.add(new PullStat(destId, srcId, table_name, responseSizeKB, timeTaken, isAsyncRequest, moreDataComing, ts));            
        }
    }
    
    public class EEStat {
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
        
        public EEStat(int partId, String tableName, boolean isExtract, boolean isLoad, int rowCount, int sizeKb, long timeTaken, long ts) {
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
        }
        
        public EEStat(int partId, String tableName, boolean isExtract, boolean isLoad, int rowCount, int sizeKb,long timeTaken, int queueGrowth, long ts) {
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
        }
    }
    

    public class PullStat {
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
        
    }
}
