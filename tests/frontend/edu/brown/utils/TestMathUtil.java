package edu.brown.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import edu.brown.statistics.FastIntHistogram;
import edu.brown.statistics.Histogram;
import edu.brown.statistics.HistogramUtil;
import edu.brown.statistics.ObjectHistogram;

import junit.framework.TestCase;

public class TestMathUtil extends TestCase {

    private static final int NUM_VALUES = 10;
    
    /**
     * Just some know values that I already have the known geometric mean for
     */
    private static final double TEST_VALUES[] = {
        1.0,
        0.01130203601213331,
        0.01823143760522339,
        0.017141472718601114,
        0.002007288849070199,
        0.008572316547717063,
        0.008176750277889333,
        0.011508064996154976,
        0.00688530755354444,
        0.011432267059707457
    };

    private static final int TEST_INT_VALUES[] = {
        14, 24, 6, 1224, 4548787, 50, 77, 98, 10, 4
    };
    
    /**
     * testFudgeyEquals
     */
    public void testFudgeyEquals() {
        // So dirty...
        double val0 = 1.110;
        double val1 = 1.150;
        
        assert(MathUtil.equals(val0, val1, 2, 0.04));
        assertFalse(MathUtil.equals(val0, val1, 2, 0.01));
    }

    /**
     * testGeometricMean
     */
    @Test
    public void testGeometricMean() {
        double expected = 0.015d;
        double mean = MathUtil.geometricMean(TEST_VALUES);
        assertEquals(expected, MathUtil.roundToDecimals(mean, 3));
        mean = MathUtil.geometricMean(TEST_VALUES, MathUtil.GEOMETRIC_MEAN_ZERO);
        assertEquals(expected, MathUtil.roundToDecimals(mean, 3));
    }
    
    /**
     * testGeometricMeanOnes
     */
    @Test
    public void testGeometricMeanOnes() {
        double values[] = new double[NUM_VALUES];
        for (int i = 0; i < NUM_VALUES; i++) {
            values[i] = 1.0;
        } // FOR
        double mean = MathUtil.geometricMean(values);
        assertEquals(1.0, mean);
    }

    /**
     * testGeometricMeanZeroes
     */
    @Test
    public void testGeometricMeanZeroes() {
        double values[] = new double[NUM_VALUES];
        for (int i = 0; i < NUM_VALUES; i++) {
            values[i] = 0.0d;
        } // FOR
        double mean = MathUtil.geometricMean(values, MathUtil.GEOMETRIC_MEAN_ZERO);
        assertEquals(0.0, MathUtil.roundToDecimals(mean, 2));
    }
    
    /**
     * testStandardDeviation1
     */
    @Test
    public void testStandardDeviation1() {
        double expected = 0.3129164048d;
        double stddev = MathUtil.stdev(TEST_VALUES);
        assertEquals(expected, stddev, 0.001);
    }
    
    /**
     * testStandardDeviation2
     */
    @Test
    public void testStandardDeviation2() {
        List<Double> values = new ArrayList<Double>();
        for (int i = 0; i < 100; i++) {
            double v = Double.parseDouble(String.format("%d.%d", i, i));
            values.add(v);
        } // FOR
        
        double expected = 29.2410446405d;
        double stddev = MathUtil.stdev(CollectionUtil.toDoubleArray(values));
        assertEquals(expected, stddev, 0.001);
    }
    
    /**
     * testStandardDeviationHistogram
     */
    @Test
    public void testStandardDeviationHistogram() {
        Random rand = new Random(0);
        List<Integer> values = new ArrayList<Integer>();
        FastIntHistogram h0 = new FastIntHistogram(100);
        Histogram<Integer> h1 = new ObjectHistogram<Integer>(); 
        for (int i = 0; i < 1000; i++) {
            int val = rand.nextInt(100);
            values.add(val);
            h0.put(val);
            h1.put(val);
        } // FOR
        
        double expected = MathUtil.stdev(CollectionUtil.toDoubleArray(values));
        assertEquals(expected, HistogramUtil.stdev(h0), 0.001);
        assertEquals(expected, HistogramUtil.stdev(h1), 0.001);
    }
    /**
     * testStandardDeviationHistogram
     */
    @Test
    public void testPercentile() {
        FastIntHistogram h0 = new FastIntHistogram(110);
        for (int i = 0; i < TEST_INT_VALUES.length; i++) {
            h0.put(TEST_INT_VALUES[i]);
        } // FOR
        for (int i = 0; i < 90; i++) {
            h0.put(1);
        } // FOR
        int[] pers = { 90,85,50,95,99};
        double[] r= HistogramUtil.percentile(h0,pers);
        
        
        assertEquals(1.3, r[0],0.01);
        assertEquals(1.0, r[1]);
        assertEquals(1.0, r[2]);
        assertEquals(25.3, r[3],0.01);
        assertEquals(46699.63, r[4],0.01);
        
        
        h0 = new FastIntHistogram(110);
        h0.put(10);
        int[] pers2 = { 10, 50, 90,100};
        double[] r2 = HistogramUtil.percentile(h0,pers2);
        assertEquals(10.0, r2[0]);
        assertEquals(10.0, r2[1]);
        assertEquals(10.0, r2[2]);
        assertEquals(10.0, r2[3]);     
        
        h0 = new FastIntHistogram(110);
        h0.put(1);
        h0.put(2);
        h0.put(3);
        h0.put(4);
        int[] pers3 = {  50, 75 ,100};
        double[] r3 = HistogramUtil.percentile(h0,pers3);
        assertEquals(2.5, r3[0]);
        assertEquals(3.25, r3[1]);
        assertEquals(4.0, r3[2]); 
        
        h0.put(10000);
        double[] r4 = HistogramUtil.percentile(h0,pers3);
        assertEquals(3.0, r4[0]);
        assertEquals(4.0, r4[1]);
        assertEquals(10000.0, r4[2]); 
        
        h0 = new FastIntHistogram(110);
        double[] r5 = HistogramUtil.percentile(h0,pers3);
        assertEquals(r5[0],Double.NaN);
    }

}