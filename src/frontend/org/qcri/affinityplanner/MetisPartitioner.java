package org.qcri.affinityplanner;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;

import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntSet;

public class MetisPartitioner extends GraphGreedyExtended {

    private static final Logger LOG = Logger.getLogger(MetisPartitioner.class);
    
    private int m_partitionsNo;

    public MetisPartitioner (CatalogContext catalogContext, File planFile, Path[] logFiles, Path[] intervalFiles){
        super(catalogContext, planFile, logFiles, intervalFiles);
        
        m_partitionsNo = catalogContext.numberOfPartitions;
    }

    @Override
    public boolean repartition() {
        LOG.info(String.format("Writing metis graph out to %s ",FileSystems.getDefault().getPath(".", Controller.METIS_OUT)));

        Path metisFile = FileSystems.getDefault().getPath(".", Controller.METIS_OUT);
        Path metisMapFile = FileSystems.getDefault().getPath(".", Controller.METIS_MAP_OUT);

        long start = System.currentTimeMillis();
        m_graph.toMetisFile(metisFile, metisMapFile);

        long time = System.currentTimeMillis() - start;
        LOG.info("generating metis out file took : " + time);
        Path metisOut= FileSystems.getDefault().getPath(".", Controller.METIS_OUT + ".part." + m_partitionsNo); 
        String metisExe = String.format("gpmetis %s %s", Controller.METIS_OUT, m_partitionsNo);
        
        //RESULTS map of hashID -> new partition ID
        Int2IntOpenHashMap metisGeneratedPartitioning = null;
        try {
            LOG.info("Calling metis " + metisExe);
            start = System.currentTimeMillis();

            Process metisProc = new ProcessBuilder("gpmetis", Controller.METIS_OUT, ""+ m_partitionsNo).start();

            // LOG.info("metis proc: " + metisProc.toString());
            int result = metisProc.waitFor();
            time = System.currentTimeMillis() - start;

            if (result == 0){
                LOG.info(String.format("Metis ran successfully. took : " + time));

                metisGeneratedPartitioning = getMetisMapping(metisOut, metisMapFile);
                LOG.info("Results in metis map files: " + metisGeneratedPartitioning.keySet().size());

                m_graph.setPartitionMaps(metisGeneratedPartitioning);
            
            } else {
                LOG.info(String.format("Metis ran unsuccessfully (%s) : %s", result, metisExe));
                return false;
            }
            
        } catch (Exception e) {
            LOG.error("Exception running metis", e);
            return false;
        }
        
        return true;
    }

    private Int2IntOpenHashMap getMetisMapping(Path metisOut, Path metisMapFile) {
        
        LOG.info(String.format("Getting metis out and mapping to hashes for %s and %s", metisOut, metisMapFile));
        Int2IntOpenHashMap res = new Int2IntOpenHashMap();
        BufferedReader outReader, metisMapReader;
        String outPart, hashId;

        try {
        
            outReader = Files.newBufferedReader(metisOut, Charset.forName("US-ASCII"));
            metisMapReader = Files.newBufferedReader(metisMapFile, Charset.forName("US-ASCII"));
            
            while(true){
                
                outPart = outReader.readLine();
                hashId = metisMapReader.readLine();
                
                if (outPart == null && hashId == null){
                    break;
                } else if (outPart == null && hashId != null){
                    LOG.error("Ran out of hashes before partition maps...");
                    break;
                } else if (outPart == null && hashId == null){
                    LOG.error("Ran out of partition maps before hashes...");
                    break;
                }
                
                res.put(Integer.parseInt(hashId), Integer.parseInt(outPart));
            }

            
        } catch (IOException e) {
            Controller.record("Error while reading out files \n Stack trace:\n" + Controller.stackTraceToString(e));
        }
        return res;
    }

}
