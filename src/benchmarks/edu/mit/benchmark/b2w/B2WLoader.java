package edu.mit.benchmark.b2w;

import java.io.*;
import java.util.Calendar;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicLong;

import edu.brown.utils.FileUtil;
import edu.brown.utils.JSONUtil;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.voltdb.CatalogContext;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;

import edu.brown.api.BenchmarkComponent;
import edu.brown.api.Loader;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

import static com.google.gdata.util.ContentType.JSON;

public class B2WLoader extends Loader {
    private static final Logger LOG = Logger.getLogger(B2WLoader.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug);
    }

    private B2WConfig config;

    public static void main(String args[]) throws Exception {
        if (debug.val) {
            LOG.debug("MAIN: " + B2WLoader.class.getName());
        }
        BenchmarkComponent.main(B2WLoader.class, args, true);
    }

    public B2WLoader(String[] args) {
        super(args);
        if (debug.val) {
            LOG.debug("CONSTRUCTOR: " + B2WLoader.class.getName());
        }
        this.config = new B2WConfig(this.m_extraParams);
        if (debug.val) {
            LOG.debug(this.config.toString());
        }
    }
    
    protected void loadCartData(Database catalog_db, String path) throws FileNotFoundException, JSONException {
        JSONArray obj = new JSONArray(FileUtil.readFile(new File(path)));
//        Table catalog_tbl_follows

    }

    @Override
    public void load() throws IOException {
        if (debug.val) {
            LOG.debug("Starting B2WLoader");
        }
        LOG.debug("Starting B2WLoader");
        final CatalogContext catalogContext = this.getCatalogContext();
        try {
            this.loadCartData(catalogContext.database,"");
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }


 
}
