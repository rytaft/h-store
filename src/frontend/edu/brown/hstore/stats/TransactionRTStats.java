package edu.brown.hstore.stats;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;
import org.voltdb.StatsSource;
import org.voltdb.SysProcSelector;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.catalog.Procedure;

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

/**
 * Counts response times for each transaction id, grouped into buckets.
 * 
 * @author Marco
 * 
 */

public class TransactionRTStats extends StatsSource {
    public static final Logger LOG = Logger.getLogger(TransactionRTStats.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.setupLogging();
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
	
	private final Map <Procedure, Buckets> responseTimes = new HashMap <Procedure, Buckets> ();
	private final long[] boundaries;
	private int column_offset;

	/**
	 * Creates profiler with four default buckets:
	 * - less than 100 ms
	 * - between 100 ms and 500 ms
	 * - between 500 ms and 1 sec
	 * - more than 1 sec
	 * 
	 * @param nanosecond_latencies latencies are in nanoseconds if true, in milliseconds if false
	 */
	public TransactionRTStats(boolean nanosecond_latencies) {
		super(SysProcSelector.TXNRESPONSETIME.name(), false);
//		LOG.info("Hi I am in RT");
		long multiplier = 1;
		if (nanosecond_latencies) multiplier = 1000000;
		this.boundaries = new long [3];
		this.boundaries[0] = 100 * multiplier;		
		this.boundaries[1] = 500 * multiplier;
		this.boundaries[2] = 1000 * multiplier;
	}	
		
	private class Buckets{
		long[] buckets = new long[boundaries.length + 1];
		
		void addToBucket(long time){
			for(int i = 0; i < boundaries.length; i++){
				if (time < boundaries[i]){
					buckets[i]++;
					break;
				}
			}
			if (time > boundaries[boundaries.length - 1]){
				buckets[boundaries.length] ++;
			}
		}
		
		long[] getAndResetBuckets(){
			long[] oldBuckets = buckets;
			buckets = new long[boundaries.length + 1];
			return oldBuckets;
		}
	}
	
	public void addResponseTime(Procedure catalog_proc, long time){
	    if(catalog_proc == null){
	        LOG.debug("TransactionRTStats: Tried to measure response time for null procedure");
	        return;
	    }
		Buckets b = this.responseTimes.get(catalog_proc);
		if (b == null){
			b = new Buckets();
			this.responseTimes.put(catalog_proc, b);
		}
		b.addToBucket(time);
	}
	
    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        super.populateColumnSchema(columns);
        this.column_offset = columns.size();

		columns.add(new VoltTable.ColumnInfo("PROCEDURE", VoltType.STRING));
		columns.add(new VoltTable.ColumnInfo("COUNT-100", VoltType.BIGINT));
		columns.add(new VoltTable.ColumnInfo("COUNT-500", VoltType.BIGINT));
		columns.add(new VoltTable.ColumnInfo("COUNT-1000", VoltType.BIGINT));
		columns.add(new VoltTable.ColumnInfo("COUNT>1000", VoltType.BIGINT));
    }
	
    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        final Iterator<Procedure> it = this.responseTimes.keySet().iterator();
        return new Iterator<Object>() {
            @Override
            public boolean hasNext() {
                return it.hasNext();
            }
            @Override
            public Object next() {
                return it.next();
            }
            @Override
            public void remove() {
                it.remove();
            }
        };
    }

    @Override
    protected synchronized void updateStatsRow(Object rowKey, Object[] rowValues) {
    	super.updateStatsRow(rowKey, rowValues);
    	Procedure catalog_proc = (Procedure) rowKey;
        rowValues[this.column_offset] = catalog_proc.getName();
		long[] buckets = this.responseTimes.get(catalog_proc).getAndResetBuckets();
		int currPos = this.column_offset + 1;
		for (int i = 0; i < buckets.length; i++){
			rowValues[currPos] = buckets[i];
			currPos ++;
		}
    }
}