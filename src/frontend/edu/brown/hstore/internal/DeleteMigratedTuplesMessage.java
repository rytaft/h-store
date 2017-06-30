package edu.brown.hstore.internal;

import edu.brown.profilers.ProfileMeasurement;

public class DeleteMigratedTuplesMessage extends InternalMessage {
    public long createTime;
    public String tableName;
    
    public DeleteMigratedTuplesMessage(String tableName) {
        super();
        this.createTime = ProfileMeasurement.getTime();
        this.tableName = tableName;
    }

    public long getQueueTime() {
        return ProfileMeasurement.getTime() - this.createTime;
    }
    
    public String getTableName() {
        return this.tableName;
    }
}
