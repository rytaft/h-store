package org.qcri.affinityplanner;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.net.UnknownHostException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collection;

import org.voltdb.CatalogContext;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Site;
import org.voltdb.client.ClientFactory;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.CollectionUtil;

public class Controller {
//    private org.voltdb.client.Client client;
//    private Collection<Site> sites;
    private CatalogContext catalog_context;
//    private String connectedHost;
//
//    private Path planFile;
//    private Path outputPlanFile;
    
    public final static int PARTITIONS_PER_SITE = 1;
    public final static int DTXN_MULTIPLIER = 5;
    public final static int LOCAL_MPT_MULTIPLIER = 0;
    public final static int MAX_MOVED_VERTICES_PER_SOURCE_SITE = 8;
    public final static int MIN_DELTA_FOR_MOVEMENT = -1;
    public final static int MAX_PARTITIONS_ADDED_RECONF = 4;
    public final static int MAX_PARTITIONS = 8;
    
    public Controller (Catalog catalog, HStoreConf hstore_conf, CatalogContext catalog_context) {
//        client = ClientFactory.createClient();
//        client.configureBlocking(false);
//        sites = CatalogUtil.getAllSites(catalog);
        this.catalog_context = catalog_context;
//        connectToHost();
//
//        if(hstore_conf.global.hasher_plan == null){
//            System.out.println("Must set global.hasher_plan to specify plan file!");
//            System.out.println("Using default (plan.json)");
//            planFile = FileSystems.getDefault().getPath("plan.json");
//
//        }
//        else{
//            planFile = FileSystems.getDefault().getPath(hstore_conf.global.hasher_plan);
//        }
//
//        outputPlanFile = FileSystems.getDefault().getPath("plan_out.json");
//
//        try {
//            Files.copy(planFile, outputPlanFile, StandardCopyOption.REPLACE_EXISTING);              
//        }
//        catch(IOException e) {
//            System.out.println("Controller: IO Exception while copying plan file to output plan file");
//            e.printStackTrace();
//        }

        //TODO select planners here
    }
    
    public void run () throws Exception {
        // TODO hardcoded, for the moment
        File planFile = new File ("plan.json");
        Path[] logFiles = new Path[4];
        logFiles[0] = FileSystems.getDefault().getPath(".", "transactions-partition-0.log");
        logFiles[1] = FileSystems.getDefault().getPath(".", "transactions-partition-1.log");
        logFiles[2] = FileSystems.getDefault().getPath(".", "transactions-partition-2.log");
        logFiles[3] = FileSystems.getDefault().getPath(".", "transactions-partition-3.log");
        
        GraphPartitioner graph = new GraphPartitioner();
        int partitions = 4;
        graph.loadFromFiles(catalog_context, planFile, partitions, logFiles);
        Path graphFile = FileSystems.getDefault().getPath(".", "graph.log");
        graph.toFileDebug(graphFile);
//        AffinityGraph[] partitions = graph.fold();
//        int i = 0;
//        for (AffinityGraph partition : partitions){
//            graphFile = FileSystems.getDefault().getPath(".", "graph-" + i++ +".log");
//            partition.toFileDebug(graphFile);
//        }
        
        graph.repartition(2700);
        
        System.out.println("Loads per partition");
        for (int j = 0; j < graph.getPartitionsNo(); j++){
            System.out.println(j + " " + graph.getLoadPerPartition(j));
        }
        
//        partitions = graph.fold();
//        i = 0;
//        for (AffinityGraph partition : partitions){
//            graphFile = FileSystems.getDefault().getPath(".", "graph-" + i++ +"-after-reconf.log");
//            partition.toFileDebug(graphFile);
//        }        
    }
    
//    public void connectToHost(){
//        Site catalog_site = CollectionUtil.random(sites);
//        connectedHost= catalog_site.getHost().getIpaddr();
//
//        try {
//            client.createConnection(null, connectedHost, HStoreConstants.DEFAULT_PORT, "user", "password");
//        } catch (UnknownHostException e) {
//            System.out.println("Controller: tried to connect to unknown host");
//            e.printStackTrace();
//            System.exit(1);
//        } catch (IOException e) {
//            System.out.println("Controller: IO Exception while connecting to host");
//            e.printStackTrace();
//            System.exit(1);
//        }
//        System.out.println("Connected to host " + connectedHost);
//    }
    
    /**
     * @param args
     */
    public static void main(String[] vargs){
        
        try {
            System.setErr(new PrintStream("error.log"));
        } catch (FileNotFoundException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

        if(vargs.length == 0){
            System.out.println("Must specify server hostname");
            return;
        }       

        ArgumentsParser args = null;
        try {
            args = ArgumentsParser.load(vargs,ArgumentsParser.PARAM_CATALOG);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Not good");
            return;
        }

        HStoreConf hstore_conf = HStoreConf.initArgumentsParser(args);

        //System.out.println("Params: bench"+params[0] +" no. part " +vargs[1] + " twin "+vargs[2]+" plannerID "+vargs[3]);
        System.out.println("args:: "+args.toString());

        System.out.println("vargs.length: "+vargs.length);

        Controller c = new Controller(args.catalog, hstore_conf, args.catalogContext);
        try {
            c.run();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Not good");
            return;
        }
    }
}
