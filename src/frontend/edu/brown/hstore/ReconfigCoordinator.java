package edu.brown.hstore;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Host;
import org.voltdb.catalog.Site;
import org.voltdb.exceptions.ServerFaultException;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.utils.EstTime;
import org.voltdb.utils.NotImplementedException;
import org.voltdb.utils.Pair;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.Hstoreservice.AsyncPullRequest;
import edu.brown.hstore.Hstoreservice.AsyncPullResponse;
import edu.brown.hstore.Hstoreservice.DataTransferRequest;
import edu.brown.hstore.Hstoreservice.DataTransferResponse;
import edu.brown.hstore.Hstoreservice.HStoreService;
import edu.brown.hstore.Hstoreservice.HeartbeatRequest;
import edu.brown.hstore.Hstoreservice.HeartbeatResponse;
import edu.brown.hstore.Hstoreservice.InitializeRequest;
import edu.brown.hstore.Hstoreservice.InitializeResponse;
import edu.brown.hstore.Hstoreservice.LivePullRequest;
import edu.brown.hstore.Hstoreservice.LivePullResponse;
import edu.brown.hstore.Hstoreservice.MultiPullReplyRequest;
import edu.brown.hstore.Hstoreservice.MultiPullReplyResponse;
import edu.brown.hstore.Hstoreservice.ReconfigurationControlRequest;
import edu.brown.hstore.Hstoreservice.ReconfigurationControlResponse;
import edu.brown.hstore.Hstoreservice.ReconfigurationControlType;
import edu.brown.hstore.Hstoreservice.ReconfigurationRequest;
import edu.brown.hstore.Hstoreservice.ReconfigurationResponse;
import edu.brown.hstore.Hstoreservice.SendDataRequest;
import edu.brown.hstore.Hstoreservice.SendDataResponse;
import edu.brown.hstore.Hstoreservice.ShutdownPrepareRequest;
import edu.brown.hstore.Hstoreservice.ShutdownPrepareResponse;
import edu.brown.hstore.Hstoreservice.ShutdownRequest;
import edu.brown.hstore.Hstoreservice.ShutdownResponse;
import edu.brown.hstore.Hstoreservice.TimeSyncRequest;
import edu.brown.hstore.Hstoreservice.TimeSyncResponse;
import edu.brown.hstore.Hstoreservice.TransactionDebugRequest;
import edu.brown.hstore.Hstoreservice.TransactionDebugResponse;
import edu.brown.hstore.Hstoreservice.TransactionFinishRequest;
import edu.brown.hstore.Hstoreservice.TransactionFinishResponse;
import edu.brown.hstore.Hstoreservice.TransactionInitRequest;
import edu.brown.hstore.Hstoreservice.TransactionInitResponse;
import edu.brown.hstore.Hstoreservice.TransactionMapRequest;
import edu.brown.hstore.Hstoreservice.TransactionMapResponse;
import edu.brown.hstore.Hstoreservice.TransactionPrefetchAcknowledgement;
import edu.brown.hstore.Hstoreservice.TransactionPrefetchResult;
import edu.brown.hstore.Hstoreservice.TransactionPrepareRequest;
import edu.brown.hstore.Hstoreservice.TransactionPrepareResponse;
import edu.brown.hstore.Hstoreservice.TransactionRedirectRequest;
import edu.brown.hstore.Hstoreservice.TransactionRedirectResponse;
import edu.brown.hstore.Hstoreservice.TransactionReduceRequest;
import edu.brown.hstore.Hstoreservice.TransactionReduceResponse;
import edu.brown.hstore.Hstoreservice.TransactionWorkRequest;
import edu.brown.hstore.Hstoreservice.TransactionWorkResponse;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.interfaces.Shutdownable;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.protorpc.NIOEventLoop;
import edu.brown.protorpc.ProtoRpcChannel;
import edu.brown.protorpc.ProtoRpcController;
import edu.brown.protorpc.ProtoServer;
import edu.brown.utils.EventObservable;
import edu.brown.utils.ThreadUtil;

/**
 * 
 * @author pavlo
 */
public class ReconfigCoordinator implements Shutdownable {
    private static final Logger LOG = Logger.getLogger(ReconfigCoordinator.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    // ----------------------------------------------------------------------------
    // INTERNAL STATE
    // ----------------------------------------------------------------------------
    
    private final HStoreSite hstore_site;
    private final HStoreConf hstore_conf;
    private final Site catalog_site;
    private final int num_sites;
    private final int local_site_id;
    
    /** SiteId -> HStoreService */
    private final HStoreService channels[];
    
    private final Thread listener_thread;
    private final ProtoServer listener;
    private final HStoreService remoteService;
    private final NIOEventLoop eventLoop = new NIOEventLoop();
    
    private Shutdownable.ShutdownState state = ShutdownState.INITIALIZED;
        
    /**
     * Special observable that is invoked when this ReconfigCoordinator is on-line
     * and ready to communicating with other nodes in the cluster.
     */
    private final EventObservable<ReconfigCoordinator> ready_observable = new EventObservable<ReconfigCoordinator>();
        
    // ----------------------------------------------------------------------------
    // MESSENGER LISTENER THREAD
    // ----------------------------------------------------------------------------
    
    /**
     * 
     */
    private class MessengerListener implements Runnable {
        @Override
        public void run() {
            Thread self = Thread.currentThread();
            self.setName(HStoreThreadManager.getThreadName(hstore_site, HStoreConstants.THREAD_NAME_RECONFIG));
            hstore_site.getThreadManager().registerProcessingThread();
            
            Throwable error = null;
            try {
                ReconfigCoordinator.this.eventLoop.run();
            } catch (Throwable ex) {
                error = ex;
            }
            
            if (error != null) {
                if (hstore_site.isShuttingDown() == false) {
                    LOG.error(this.getClass().getSimpleName() + " has stopped!", error);
                }
                
                Throwable cause = null;
                if (error instanceof ServerFaultException && error.getCause() != null) {
                    if (error.getCause().getMessage() != null && error.getCause().getMessage().isEmpty() == false) {
                        cause = error.getCause();
                    }
                }
                if (cause == null) cause = error;
                
                // These errors are ok if we're actually stopping...
                if (ReconfigCoordinator.this.state == ShutdownState.SHUTDOWN ||
                    ReconfigCoordinator.this.state == ShutdownState.PREPARE_SHUTDOWN ||
                    ReconfigCoordinator.this.hstore_site.isShuttingDown()) {
                    // IGNORE
                } else {
                    LOG.fatal("Unexpected error in messenger listener thread", cause);
                }
            }
            if (trace.val)
                LOG.trace("Messenger Thread for Site #" + catalog_site.getId() + " has stopped!");
        }
    }
    
    // ----------------------------------------------------------------------------
    // INITIALIZATION
    // ----------------------------------------------------------------------------

    /**
     * Constructor
     * @param hstore_site
     */
    public ReconfigCoordinator(HStoreSite hstore_site) {
        this.hstore_site = hstore_site;
        this.hstore_conf = this.hstore_site.getHStoreConf();
        this.catalog_site = this.hstore_site.getSite();
        this.local_site_id = this.catalog_site.getId();
        this.num_sites = this.hstore_site.getCatalogContext().numberOfSites;
        this.channels = new HStoreService[this.num_sites];
        
        if (debug.val)
            LOG.debug(String.format("Local Partitions for Site #%d: %s",
                      hstore_site.getSiteId(), hstore_site.getLocalPartitionIds()));

        // Incoming RPC Handler
        this.remoteService = this.initHStoreService();
        
        // This listener thread will process incoming messages
        this.listener = new ProtoServer(this.eventLoop);
                
        // Wrap the listener in a daemon thread
        this.listener_thread = new Thread(new MessengerListener());
        this.listener_thread.setDaemon(true);
        this.eventLoop.setExitOnSigInt(true);
    }
    
    protected HStoreService initHStoreService() {
        return (new RemoteServiceHandler());
    }
    
    /**
     * Start the messenger. This is a blocking call that will initialize the connections
     * and start the listener thread!
     */
    public synchronized void start() {
        assert(this.state == ShutdownState.INITIALIZED) : "Invalid MessengerState " + this.state;
        
        this.state = ShutdownState.STARTED;
        
        if (debug.val) LOG.debug("Initializing connections");
        this.initConnections();
        
        if (debug.val) LOG.debug("Starting listener thread");
        this.listener_thread.start();
        
        this.ready_observable.notifyObservers(this);
    }

    /**
     * Returns true if the messenger has started
     * @return
     */
    public boolean isStarted() {
        return (this.state == ShutdownState.STARTED);
    }
    
    /**
     * Internal call for testing to hide errors
     */
    @Override
    public void prepareShutdown(boolean error) {
        if (this.state != ShutdownState.PREPARE_SHUTDOWN) {
            assert(this.state == ShutdownState.STARTED) : "Invalid ReconfigCoordinator State " + this.state;
            this.state = ShutdownState.PREPARE_SHUTDOWN;
        }
    }
    
    /**
     * Stop this ReconfigCoordinator. This kills the ProtoRPC messenger event loop
     */
    @Override
    public synchronized void shutdown() {
        assert(this.state == ShutdownState.STARTED || this.state == ShutdownState.PREPARE_SHUTDOWN) :
            "Invalid MessengerState " + this.state;
        
        this.state = ShutdownState.SHUTDOWN;
        
        try {
            if (trace.val) LOG.trace("Stopping eventLoop for Site #" + this.getLocalSiteId());
            this.eventLoop.exitLoop();

            if (trace.val) LOG.trace("Stopping listener thread for Site #" + this.getLocalSiteId());
            this.listener_thread.interrupt();
            
            if (trace.val) LOG.trace("Joining on listener thread for Site #" + this.getLocalSiteId());
            this.listener_thread.join();
        } catch (InterruptedException ex) {
            // IGNORE
        } catch (Throwable ex) {
            LOG.error("Unexpected error when trying to stop messenger for Site #" + this.getLocalSiteId(), ex);
        } finally {
            if (trace.val) LOG.trace("Closing listener socket for Site #" + this.getLocalSiteId());
            this.listener.close();
        }
    }
    
    /**
     * Returns true if the messenger has stopped
     * @return
     */
    @Override
    public boolean isShuttingDown() {
        return (this.state == ShutdownState.PREPARE_SHUTDOWN);
    }
    
    public boolean isShutdownOrPrepareShutDown() {
        return (this.state == ShutdownState.PREPARE_SHUTDOWN || this.state == ShutdownState.SHUTDOWN);
    }
    
    protected int getLocalSiteId() {
        return (this.local_site_id);
    }
    protected int getLocalMessengerPort() {
        return (this.hstore_site.getSite().getReconfigMessenger_port());
    }
    protected final Thread getListenerThread() {
        return (this.listener_thread);
    }
    
    public HStoreService getChannel(int site_id) {
        return (this.channels[site_id]);
    }
    public HStoreService getHandler() {
        return (this.remoteService);
    }
    public EventObservable<ReconfigCoordinator> getReadyObservable() {
        return (this.ready_observable);
    }
        
    /**
     * Initialize all the network connections to remote
     *  
     */
    private void initConnections() {
        if (debug.val) LOG.debug("Configuring outbound network connections for Site #" + this.catalog_site.getId());
        
        // Initialize inbound channel
        Integer local_port = this.catalog_site.getReconfigMessenger_port();
        assert(local_port != null);
        if (debug.val) LOG.debug("Binding listener to port " + local_port + " for Site #" + this.catalog_site.getId());
        this.listener.register(this.remoteService);
        this.listener.bind(local_port);

        // Find all the destinations we need to connect to
        // Make the outbound connections
        List<Pair<Integer, InetSocketAddress>> destinations = ReconfigCoordinator.getRemoteCoordinators(this.catalog_site);
        
        if (destinations.isEmpty()) {
            if (debug.val) LOG.debug("There are no remote sites so we are skipping creating connections");
        }
        else {
            if (debug.val) LOG.debug("Connecting to " + destinations.size() + " remote site messengers");
            ProtoRpcChannel[] channels = null;
            InetSocketAddress arr[] = new InetSocketAddress[destinations.size()];
            for (int i = 0; i < arr.length; i++) {
                arr[i] = destinations.get(i).getSecond();
                if (debug.val) LOG.debug("Attemping to connect to " + arr[i]);
            } // FOR
                    
            int tries = hstore_conf.site.network_startup_retries;
            boolean success = false;
            Throwable error = null;
            while (tries-- > 0 && success == false) {
                try {
                    channels = ProtoRpcChannel.connectParallel(this.eventLoop,
                                                               arr,
                                                               hstore_conf.site.network_startup_wait);
                    success = true;
                } catch (Throwable ex) {
                    if (tries > 0) {
                        LOG.warn("Failed to connect to remote sites. Going to try again...");
                        continue;
                    }
                }
            } // WHILE
            if (success == false) {
                LOG.fatal("Site #" + this.getLocalSiteId() + " failed to connect to remote sites");
                this.listener.close();
                throw new RuntimeException(error);
            }
            assert channels.length == destinations.size();
            for (int i = 0; i < channels.length; i++) {
                Pair<Integer, InetSocketAddress> p = destinations.get(i);
                this.channels[p.getFirst()] = HStoreService.newStub(channels[i]);
            } // FOR
            
            if (debug.val) LOG.debug("Site #" + this.getLocalSiteId() + " is fully connected to all sites");
        }
    }
    
    // ----------------------------------------------------------------------------
    // HSTORE RPC SERVICE METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * We want to make this a private inner class so that we do not expose
     * the RPC methods to other parts of the code.
     */
    private class RemoteServiceHandler extends HStoreService {
    
        @Override
        public void transactionInit(RpcController controller, TransactionInitRequest request, RpcCallback<TransactionInitResponse> callback) {
            throw new NotImplementedException("transactionInit");
        }
        
        @Override
        public void transactionWork(RpcController controller, TransactionWorkRequest request, RpcCallback<TransactionWorkResponse> callback) {
            throw new NotImplementedException("transactionWork");
        }

        @Override
        public void transactionPrefetch(RpcController controller, TransactionPrefetchResult request, RpcCallback<TransactionPrefetchAcknowledgement> callback) {
            throw new NotImplementedException("transactionPrefetch");
        }
        
        @Override
        public void transactionMap(RpcController controller, TransactionMapRequest request, RpcCallback<TransactionMapResponse> callback) {
            throw new NotImplementedException("transactionMap");
        }
        
        @Override
        public void transactionReduce(RpcController controller, TransactionReduceRequest request, RpcCallback<TransactionReduceResponse> callback) {
            throw new NotImplementedException("transactionReduce");
        }
        
        @Override
        public void transactionPrepare(RpcController controller, TransactionPrepareRequest request, RpcCallback<TransactionPrepareResponse> callback) {
            throw new NotImplementedException("transactionPrepare");
        }
        
        @Override
        public void transactionFinish(RpcController controller, TransactionFinishRequest request, RpcCallback<TransactionFinishResponse> callback) {
            throw new NotImplementedException("transactionFinish");
        }
        
        @Override
        public void transactionRedirect(RpcController controller, TransactionRedirectRequest request, RpcCallback<TransactionRedirectResponse> done) {
            throw new NotImplementedException("transactionRedirect");
        }
        
        @Override
        public void sendData(RpcController controller, SendDataRequest request, RpcCallback<SendDataResponse> done) {
            throw new NotImplementedException("sendData");
        }
        
        @Override
        public void initialize(RpcController controller, InitializeRequest request, RpcCallback<InitializeResponse> done) {
            throw new NotImplementedException("initialize");
        }
        
        @Override
        public void shutdownPrepare(RpcController controller, ShutdownPrepareRequest request, RpcCallback<ShutdownPrepareResponse> done) {
            throw new NotImplementedException("shutdownPrepare");
        }
        
        @Override
        public void shutdown(RpcController controller, ShutdownRequest request, RpcCallback<ShutdownResponse> done) {
            throw new NotImplementedException("shutdown");
        }
        
        @Override
        public void heartbeat(RpcController controller, HeartbeatRequest request, RpcCallback<HeartbeatResponse> done) {
            throw new NotImplementedException("heartbeat");          
        }

        @Override
        public void timeSync(RpcController controller, TimeSyncRequest request, RpcCallback<TimeSyncResponse> done) {
            throw new NotImplementedException("timeSync");
        }
        
        @Override
        public void reconfiguration(RpcController controller, ReconfigurationRequest request, RpcCallback<ReconfigurationResponse> done) {
            if (debug.val)
                LOG.debug(String.format("Received %s from HStoreSite %s",
                          request.getClass().getSimpleName(),
                          HStoreThreadManager.formatSiteName(request.getSenderSite())));
            ReconfigurationResponse.Builder builder = ReconfigurationResponse.newBuilder()
                                                    .setT0S(request.getT0S())
                                                    .setSenderSite(local_site_id);
            ThreadUtil.sleep(10);
            done.run(builder.build());
        }

        @Override
        public void transactionDebug(RpcController controller, TransactionDebugRequest request, RpcCallback<TransactionDebugResponse> done) {
            throw new NotImplementedException("transactionDebug");
        }

        @Override
        public void dataTransfer(RpcController controller,
            DataTransferRequest request, RpcCallback<DataTransferResponse> done) {
          
          if (debug.val)
            LOG.debug(String.format("Received %s from HStoreSite %s",
                      request.getClass().getSimpleName(),
                      HStoreThreadManager.formatSiteName(request.getSenderSite())));
          
          VoltTable vt = null;
          VoltTable minIncl = null;
          VoltTable maxExcl = null;
          try {
        	  ByteString minInclBytes = request.getMinInclusive();
              ByteString maxExclBytes = request.getMaxExclusive();
              minIncl = FastDeserializer.deserialize(minInclBytes.toByteArray(), VoltTable.class);
              maxExcl = FastDeserializer.deserialize(maxExclBytes.toByteArray(), VoltTable.class);
              
              vt = FastDeserializer.deserialize(request.getVoltTableData().toByteArray(), VoltTable.class);
          } catch (IOException e) {
            // TODO Auto-generated catch block
            LOG.error("Error in deserializing volt table");
          }
          DataTransferResponse response = null;
          try {
            response = hstore_site.getReconfigurationCoordinator().receiveTuples(
                request.getSenderSite(), request.getT0S(), request.getOldPartition(), request.getNewPartition(), 
                request.getVoltTableName(), vt, minIncl, maxExcl);
          } catch (Exception e) {
            // TODO Auto-generated catch block
            LOG.error("Exception incurred while receiving tuples", e);
          }
          
          done.run(response);
          
        }

        @Override
        public void livePull(RpcController controller, LivePullRequest request,
            RpcCallback<LivePullResponse> done) {
          if (debug.val)
            LOG.debug(String.format("Received %s from HStoreSite %s",
                      request.getClass().getSimpleName(),
                      HStoreThreadManager.formatSiteName(request.getSenderSite())));
          
          LivePullResponse response = null;
          try {
              hstore_site.getReconfigurationCoordinator().dataPullRequest(
                request, done);
          } catch (Exception e) {
            // TODO Auto-generated catch block
            LOG.error("Exception incurred while receiving tuples", e);
          }
          
          // Callback will be made when the work item is processed
          // from the queue
          //done.run(response);
          
        }

        @Override
        public void asyncPull(RpcController controller, AsyncPullRequest request,
            RpcCallback<AsyncPullResponse> done) {
            
            // This is an async data pull request which will be sent to RC and
            // processed asynchronously
             try {
                 hstore_site.getReconfigurationCoordinator().asyncPullRequestFromRC (
                   request, done);
             } catch (Exception e) {
               // TODO Auto-generated catch block
               LOG.error("Exception occured while processing async pull request", e);
             }
            
        }
        
        
        @Override
        public void reconfigurationControlMsg(RpcController controller, 
                ReconfigurationControlRequest request, RpcCallback<ReconfigurationControlResponse> done) {

            if (debug.val)
                LOG.debug(String.format("Received %s from HStoreSite %s",
                          request.getClass().getSimpleName(),
                          HStoreThreadManager.formatSiteName(request.getSenderSite())));
            
            // No callback to be sent
            try{
                if(request.getReconfigControlType() == ReconfigurationControlType.PULL_RECEIVED){
                    hstore_site.getReconfigurationCoordinator().deleteTuples(request);
                } else if(request.getReconfigControlType() == ReconfigurationControlType.CHUNK_RECEIVED){
                	//TODO : Have to delete tuples for the chunk received messages as well
                    //hstore_site.getReconfigurationCoordinator().deleteTuples(request);
                } else if(request.getReconfigControlType() == ReconfigurationControlType.RECONFIGURATION_DONE) {
                    hstore_site.getReconfigurationCoordinator().leaderReceiveRemoteReconfigComplete(request.getSenderSite());
                }  else if(request.getReconfigControlType() == ReconfigurationControlType.
                		RECONFIGURATION_DONE_RECEIVED) {
                    hstore_site.getReconfigurationCoordinator().receiveReconfigurationCompleteFromLeader();
                } else if(request.getReconfigControlType() == ReconfigurationControlType.NEXT_RECONFIGURATION_PLAN) {
                    hstore_site.getReconfigurationCoordinator().receiveNextReconfigurationPlanFromLeader();
                }
               
            } catch (Exception e) {
                // TODO Auto-generated catch block
                LOG.error("Exception incurred while deleting tuples (not just marking)", e);
              }
            
        }
        
        @Override
        public void multiPullReply(RpcController controller, 
            MultiPullReplyRequest request, RpcCallback<MultiPullReplyResponse> done) {
        	if (debug.val)
                LOG.debug(String.format("Received %s from HStoreSite %s",
                          request.getClass().getSimpleName(),
                          HStoreThreadManager.formatSiteName(request.getSenderSite())));
        	
        	try{
        		if(request.getIsAsync()){
        		    if (debug.val) LOG.debug("Processing an async pull reply message");
        			hstore_site.getReconfigurationCoordinator().processPullReplyFromRC(request, done);
        		} else {
        		    if (debug.val) LOG.debug("Processing a live pull reply message");
                    hstore_site.getReconfigurationCoordinator().processPullReplyFromRC(request, done);

        		}
        		
        	} catch (Exception e){
        		LOG.error("Exception incurred while processing chunked async pull reply", e);
        	}
          
        }
        


    } // END CLASS

    // ----------------------------------------------------------------------------
    // UTILITY METHODS
    // ----------------------------------------------------------------------------
    
    public static List<Pair<Integer, InetSocketAddress>> getRemoteCoordinators(Site catalog_site) {
        List<Pair<Integer, InetSocketAddress>> m = new ArrayList<Pair<Integer,InetSocketAddress>>();
        
        Database catalog_db = CatalogUtil.getDatabase(catalog_site);
        Map<Host, Set<Site>> host_partitions = CatalogUtil.getSitesPerHost(catalog_db);
        for (Entry<Host, Set<Site>> e : host_partitions.entrySet()) {
            String host = e.getKey().getIpaddr();
            for (Site remote_site : e.getValue()) {
                if (remote_site.getId() != catalog_site.getId()) {
                    InetSocketAddress address = new InetSocketAddress(host, remote_site.getReconfigMessenger_port()); 
                    m.add(Pair.of(remote_site.getId(), address));
                    if (debug.val)
                        LOG.debug(String.format("Creating RpcChannel to %s for site %s",
                                  address, HStoreThreadManager.formatSiteName(remote_site.getId())));
                } // FOR
            } // FOR 
        } // FOR
        return (m);
    }

    /**
     * Returns an HStoreService handle that is connected to the given site
     * This should not be called directly.
     * @param catalog_site
     * @return
     */
    protected static HStoreService getHStoreService(Site catalog_site) {
        NIOEventLoop eventLoop = new NIOEventLoop();
        InetSocketAddress addresses[] = new InetSocketAddress[] {
            new InetSocketAddress(catalog_site.getHost().getIpaddr(), catalog_site.getReconfigMessenger_port()) 
        };
        ProtoRpcChannel[] channels = null;
        try {
            channels = ProtoRpcChannel.connectParallel(eventLoop, addresses);
        } catch (Exception ex) {
            
        }
        HStoreService channel = HStoreService.newStub(channels[0]);
        return (channel);
    }

    public synchronized HStoreService[] getChannels() {
      return channels;
    }
}
