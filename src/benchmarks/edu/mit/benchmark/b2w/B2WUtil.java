package edu.mit.benchmark.b2w;

public class B2WUtil {

    public static void sleep(long sleep_time) {
        long start_time = System.currentTimeMillis();
        while(System.currentTimeMillis() - start_time < sleep_time) {
            continue; // spin wait
        }
//        try {
//            Thread.sleep(sleep_time);
//        } catch(InterruptedException e) {
//            // do nothing
//        }
    }

}
