/**
 * 
 */
package edu.brown.hstore.reconfiguration;

/**
 * @author aelmore
 *
 */
public class ReconfigurationStats {

    private boolean on;
    /**
     * 
     */
    public ReconfigurationStats() {
        on = true;
    }
    
    public void trackLoad(int partitionId, String tableName, int rowCount, int loadSizeKB, long timeTaken,int queueGrowth) {
        
    }
    
    public void trackLoad(int partitionId, String tableName, int rowCount, int loadSizeKB, long timeTaken) {
        
    }

    public void trackAsyncResponse(int sourcePartitionId, int destPartitionId, String voltTableName, int responseSizeKB, boolean moreDataNeeded) {
        
    }

    public void trackAsyncPullInit(int partitionId, int destPartitionId, String tableName) {
        if (on){
            System.currentTimeMillis();
            
        }
    }


}
