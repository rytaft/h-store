package edu.brown.hstore.stats;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.voltdb.StatsSource;
import org.voltdb.SysProcSelector;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.catalog.Procedure;

import edu.brown.hstore.HStoreSite;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.statistics.FastIntHistogram;

/**
 * Counts response times for each transaction id, grouped into buckets.
 * 
 * @author Marco
 * 
 */

public class PartitionRates extends StatsSource {
    public static final Logger LOG = Logger.getLogger(PartitionRates.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.setupLogging();
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
	
	private FastIntHistogram accessRates = null;
	private FastIntHistogram[] affinityMatrix = null;
	private int column_offset;
	private int numberOfPartitions;
	private long lastCall;
	private long lastInterval;

	public PartitionRates(int noOfPartitions){
		super(SysProcSelector.PARTITIONRATES.name(), false);
		LOG.info("Been in PartitionRates constructor");
		numberOfPartitions = noOfPartitions;
		affinityMatrix = new FastIntHistogram[numberOfPartitions];
		lastCall = System.currentTimeMillis();
	}
	
	/**
	 * Add measured accesses.
	 * 
	 * @param counts Histogram recording how many times we accessed the partitions
	 */

	public void addAccesses(FastIntHistogram counts){
		if(accessRates == null){
			accessRates = counts;
		}
		else{
			accessRates.put(counts);
		}
		for (int row = 0; row < numberOfPartitions; row++){
			if(counts.contains(row)){
				if(affinityMatrix[row] == null){
					affinityMatrix[row] = new FastIntHistogram(true, numberOfPartitions);
				}
				affinityMatrix[row].put(counts);
			}
		}
	}
	
    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        super.populateColumnSchema(columns);
        this.column_offset = columns.size();
		columns.add(new VoltTable.ColumnInfo("PARTITION_ID", VoltType.BIGINT));
		columns.add(new VoltTable.ColumnInfo("TOT_ACCESSES", VoltType.BIGINT));
		columns.add(new VoltTable.ColumnInfo("AFFINITY", VoltType.STRING));
    }
	
    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
    	long thisCall = System.currentTimeMillis();
    	lastInterval = thisCall - lastCall;
    	lastCall = thisCall;
        return new Iterator<Object>() {
        	int next = 0;
            @Override
            public boolean hasNext() {
                return next < numberOfPartitions;
            }
            @Override
            public Object next() {
            	next ++;
                return next-1;
            }
            @Override
            public void remove() {}
        };
    }

    @Override
    protected synchronized void updateStatsRow(Object rowKey, Object[] rowValues) {
    	super.updateStatsRow(rowKey, rowValues);
        LOG.info("Processing row: " + rowKey);    	
        LOG.info("Row has : " + rowValues.length + " elements");    	
        rowValues[this.column_offset] = rowKey;
        if(accessRates != null){
        	rowValues[this.column_offset+1] = accessRates.get((Integer) rowKey);
        }
        else{
        	rowValues[this.column_offset+1] = -1;
        }        	
        LOG.info("Number of partitions: " + numberOfPartitions);
        StringBuffer str = new StringBuffer();
		for (int i = 0; i < numberOfPartitions; i++){
			if(affinityMatrix[(Integer) rowKey] != null){
				str.append("\t"+ (affinityMatrix[(Integer) rowKey].get(i)/(lastInterval/1000)));
			}
			else{
				str.append("\t-1");
			}
		}
		rowValues[this.column_offset+2] = str.toString();
    }
}