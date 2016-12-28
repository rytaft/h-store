package edu.mit.benchmark.b2w_sku_key;

public class B2WUtil {

    public static int sleep(long sleep_time) {
        long start_time = System.currentTimeMillis();
        int i = 0;
        while(System.currentTimeMillis() - start_time < sleep_time) {
            ++i; // spin wait
        }
        return i;
//        try {
//            Thread.sleep(sleep_time);
//        } catch(InterruptedException e) {
//            // do nothing
//        }
    }

}
