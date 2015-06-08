package org.qcri.affinityplanner;

import java.util.Timer;
import java.util.TimerTask;

import org.voltdb.CatalogContext;

import edu.brown.hstore.conf.HStoreConf;

public class ControllerThread {

    Timer timer;

    public ControllerThread (long delay, HStoreConf hstore_conf, CatalogContext catalog_context){
        timer = new Timer();
        timer.schedule(new ControllerTimerTask(hstore_conf, catalog_context), delay);
    }

    class ControllerTimerTask extends TimerTask{
        HStoreConf hstore_conf;
        CatalogContext catalog_context;
        
        public ControllerTimerTask(HStoreConf hstore_conf, CatalogContext catalog_context){
            this.hstore_conf = hstore_conf;
            this.catalog_context = catalog_context;
        }
        
        @Override
        public void run(){
            Controller c = new Controller(catalog_context.catalog, hstore_conf, catalog_context);
            c.run();
            timer.cancel();
        }
    }

}
