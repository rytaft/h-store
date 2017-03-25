package org.qcri.affinityplanner;

import edu.brown.hstore.conf.HStoreConf;
import org.voltdb.CatalogContext;

import java.util.Timer;
import java.util.TimerTask;

/**
 * Created by mserafini on 3/23/17.
 */
public class PredictiveControllerThread {
    Timer timer;
    PredictiveController controller;

    public PredictiveControllerThread (long delay, HStoreConf hstore_conf, CatalogContext catalog_context){
        timer = new Timer();
        timer.schedule(new PredictiveControllerThread.PredictiveControllerTimerTask(hstore_conf, catalog_context), delay);
    }

    class PredictiveControllerTimerTask extends TimerTask {
        HStoreConf hstore_conf;
        CatalogContext catalog_context;

        public PredictiveControllerTimerTask(HStoreConf hstore_conf, CatalogContext catalog_context){
            this.hstore_conf = hstore_conf;
            this.catalog_context = catalog_context;
        }

        @Override
        public void run(){
            PredictiveController c = new PredictiveController(catalog_context.catalog, hstore_conf);
            try {
                c.run();
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Controller got exception, exiting");
            }
            timer.cancel();
        }
    }

    public void stop(){
        controller.stop();
    }
}
