package edu.brown.hstore.stats;

/**
 * monitoring CPU utilization.
 * 
 * @author Essam Mansour
 * 
 */

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;


import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.processtools.ShellTools;
import org.voltdb.utils.PlatformProperties;
import org.voltdb.utils.SystemStatsCollector;
import org.voltdb.utils.SystemStatsCollector.PSScraper;

//import org.apache.log4j.Logger;
import org.voltdb.StatsSource;
//import org.voltdb.SysProcSelector;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
//import org.voltdb.catalog.Procedure;
//
//import edu.brown.hstore.HStoreSite;
//import edu.brown.logging.LoggerUtil;
//import edu.brown.logging.LoggerUtil.LoggerBoolean;

public class CPUStats extends StatsSource {
	
	
	static long time_old =0;
	static long etime_old=0;
    
     
    static class PartitionCPURow {
        int user = 0;
        int nice = 0;
        int system = 0;
        int idle = 0;
        double usage = (double) 30.5;
        
    }
    Map<Long, PartitionCPURow> c_cpuStats = new TreeMap<Long, PartitionCPURow>();

    public CPUStats() {
        super("CPU", false);
    }
    
    

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        return new Iterator<Object>() {
            boolean returnRow = true;

            @Override
            public boolean hasNext() {
                return returnRow;
            }

            @Override
            public Object next() {
                if (returnRow) {
                    returnRow = false;
                    return new Object();
                } else {
                    return null;
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        super.populateColumnSchema(columns);
//        this.column_offset = columns.size();

        columns.add(new VoltTable.ColumnInfo("PID", VoltType.INTEGER));
        columns.add(new VoltTable.ColumnInfo("Usage", VoltType.FLOAT));
		
    }

    /**
     * Givent the format "ps" uses for a time duration, parse it into
     * a numbe of milliseconds.
     */
    static long getDurationFromPSString(String duration) {
        String[] parts;

        // split into days and sub-days
        duration = duration.trim();
        parts = duration.split("-");
        assert(parts.length > 0);
        assert(parts.length <= 2);
        String dayString = "0"; if (parts.length == 2) dayString = parts[0];
        String subDayString = parts[parts.length - 1];
        long days = Long.parseLong(dayString);

        // split into > seconds in 00:00:00 time and second fractions
        subDayString = subDayString.trim();
        parts = subDayString.split("\\.");
        assert(parts.length > 0);
        assert(parts.length <= 2);
        String fractionString = "0"; if (parts.length == 2) fractionString = parts[parts.length - 1];
        subDayString = parts[0];
        while (fractionString.length() < 3) fractionString += "0";
        long miliseconds = Long.parseLong(fractionString);

        // split into hours,minutes,seconds
        parts = subDayString.split(":");
        assert(parts.length > 0);
        assert(parts.length <= 3);
        String hoursString = "0"; if (parts.length == 3) hoursString = parts[parts.length - 3];
        String minutesString = "0"; if (parts.length >= 2) minutesString = parts[parts.length - 2];
        String secondsString = parts[parts.length - 1];
        long hours = Long.parseLong(hoursString);
        long minutes = Long.parseLong(minutesString);
        long seconds = Long.parseLong(secondsString);

        // compound down to ms
        hours = hours + (days * 24);
        minutes = minutes + (hours * 60);
        seconds = seconds + (minutes * 60);
        miliseconds = miliseconds + (seconds * 1000);
        return miliseconds;
    }
    
    @Override
    protected synchronized void updateStatsRow(Object rowKey, Object[] rowValues) {
        // sum up all of the site statistics
    	PartitionCPURow totals = new PartitionCPURow();
        for (PartitionCPURow pmr : c_cpuStats.values()) {
        	pmr.usage = 0;
        	totals.usage = pmr.usage ;
            
        }

        // get system CPU statistics
        double CPUUsage = (double) 30;
        
        String processName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
        String pidString = processName.substring(0, processName.indexOf('@'));
        int pid = Integer.valueOf(pidString);
        
//        String command = String.format("ps -p %d -o rss,pmem,pcpu,time,etime", pid);
        
        String command = String.format("ps -p %d -o pcpu,time,etime", pid);
        String results = ShellTools.cmd(command);

        // parse ps into value array
        String[] lines = results.split("\n");
        if (lines.length == 2)
        {    
        results = lines[1];
        results = results.trim();

        // For systems where LANG != en_US.UTF-8.
        // see: http://community.voltdb.com/node/422
        results = results.replace(",", ".");

        String[] values = results.split("\\s+");

        // tease out all the stats
               
        //double pcpu = Double.valueOf(values[0]);
        
        long time = getDurationFromPSString(values[1]);
        long etime = getDurationFromPSString(values[2]);
        
        CPUUsage = (double) ((float)(time - time_old)/(float)(etime-etime_old)) * 100;
        
        time_old = time;
        etime_old =etime;
        
        
        }
        else 
        	{
        	CPUUsage = (double) -1; 
        	}
        
        rowValues[columnNameToIndex.get("PID")] = pid;
        rowValues[columnNameToIndex.get("Usage")] = CPUUsage;
        
        
        super.updateStatsRow(rowKey, rowValues);
    }

    public synchronized void eeUpdateCPUStats(long siteId,
                                              float usage
                                              ) {
        PartitionCPURow pmr = new PartitionCPURow();
        pmr.usage = usage;
              
        c_cpuStats.put(siteId, pmr);
    }
}
