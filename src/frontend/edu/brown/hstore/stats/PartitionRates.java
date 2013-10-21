package edu.brown.hstore.stats;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
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
 * Counts accesses to local partitions.
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
//	private FastIntHistogram[] affinityMatrix = null;
	private int column_offset;
	private int numberOfPartitions;
//	private int[] localPartitions;
//	private int numberOfLocalPartitions;
	private boolean firstCall = true;
	private final int TABLE_SIZE = 300;
//	private final long TABLE_ROW_PERIOD = 150000;
	private final long TABLE_ROW_PERIOD = 10000;
	
	private Timer timer;
	private FastIntHistogram[] accessesTable = new FastIntHistogram[TABLE_SIZE];
	private int currPosTable = 0;
	
	public PartitionRates(CatalogContext catalog, int siteId){
		super(SysProcSelector.PARTITIONRATES.name(), false);
//		LOG.info("Been in PartitionRates constructor");
		numberOfPartitions = catalog.numberOfPartitions;
		accessRates = new FastIntHistogram(false,numberOfPartitions);
//		affinityMatrix = new FastIntHistogram[numberOfPartitions];
//		for (int i = 0; i < numberOfPartitions; i++){
//			affinityMatrix[i] = new FastIntHistogram(false,numberOfPartitions);
//		}

//		localPartitions = new int [numberOfPartitions];
//		int curr = 0;
//		numberOfLocalPartitions = 0;
//		for (int part = 0; part < numberOfPartitions; part++){
//			if(catalog.getSiteIdForPartitionId(part) == siteId){
//				localPartitions[curr++] = part;
//				numberOfLocalPartitions++;
//			}
//		}

		//		lastCall = System.currentTimeMillis();
	}
	
	/**
	 * Add measured accesses.
	 * 
	 * @param counts Histogram recording how many times we accessed the partitions
	 */

	public void addAccesses(FastIntHistogram counts){
		if(firstCall){
			timer = new Timer();
			timer.scheduleAtFixedRate(new NewRow(), TABLE_ROW_PERIOD, TABLE_ROW_PERIOD);
			firstCall = false;
		}
		for (int part = 0; part < numberOfPartitions; part++){
			if(counts.contains(part)){
				accessRates.put(part);
//				accessRates.put(part, accessRates.get(part)+1);
			}
			
//			accessRates.put(part,counts.get(part));
						
//			if(counts.contains(part)){
//				for (int j = 0; j < numberOfLocalPartitions; j++){
//					int innerPart = localPartitions[j];
//					affinityMatrix[part].put(innerPart,counts.get(innerPart));
//				}
//			}
		}
	}
	
	class NewRow extends TimerTask{
		@Override
		public void run(){
			accessesTable[currPosTable++] = accessRates; //TODO must use circular buffer
			accessRates = new FastIntHistogram(false,numberOfPartitions);
		}
	}


	@Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        super.populateColumnSchema(columns);
        this.column_offset = columns.size();
		columns.add(new VoltTable.ColumnInfo("PARTITION_ID", VoltType.BIGINT));
		columns.add(new VoltTable.ColumnInfo("TOT_ACCESSES (PER TIME INTERVAL OF " + TABLE_ROW_PERIOD + " MS)", VoltType.STRING));
    }
	
    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        return new Iterator<Object>() {
        	int nextPart = 0;
            @Override
            public boolean hasNext() {
                return nextPart < numberOfPartitions;
            }
            @Override
            public Object next() {
                return nextPart++;
            }
            @Override
            public void remove() {}
        };
    }

    @Override
    protected synchronized void updateStatsRow(Object rowKey, Object[] rowValues) {
    	super.updateStatsRow(rowKey, rowValues);
//        LOG.info("Processing row: " + rowKey);    	
//        LOG.info("Row has : " + rowValues.length + " elements");    	
        rowValues[this.column_offset] = rowKey;
//        LOG.info("Number of partitions: " + numberOfPartitions);
        StringBuffer str = new StringBuffer();
		for (int i = 0; i < currPosTable; i++){
			str.append(accessesTable[i].get((Integer) rowKey) + "\t");
		}
		rowValues[this.column_offset+1] = str.toString();
    }
}