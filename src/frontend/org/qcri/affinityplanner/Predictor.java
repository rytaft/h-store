package org.qcri.affinityplanner;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import edu.brown.utils.FileUtil;

/**
 * Created by mserafini on 2/15/17.
 */
public class Predictor {
    public Predictor(){}

    public static void record(String s){
        System.out.println(s);
        FileUtil.appendEventToFile(s);
    }


    public ArrayList<Long> predictLoad (ArrayList<Long> currentLoad, int numPredsAhead, String modelCoefficientsFileName){
    	// currentLoad: "n" observations to be used for predicting future load
       // System.out.print("<--- currentLoad --->");
       // System.out.println( currentLoad.toString() );

      String predModelCoeffs = FileUtil.readFile(modelCoefficientsFileName);
      String[] contents = predModelCoeffs.split("\n");

      //[1 = Model order (p); 2 = x.mean; 3 = x.intercept; 4..p = model coefficients]
      int modelOrder = Integer.valueOf(contents[0]);
      double xMean = Double.valueOf(contents[1]);
      double xIntercept = Double.valueOf(contents[2]);
      double[] coeffs = new double[modelOrder];

      ArrayList<Long> preds = new ArrayList<>();
      if( currentLoad.size() < modelOrder ){
      	System.out.println("Not enough historical data points to make a prediction!");
      	return null;
      }
      else{
        //System.out.print("predModelCoeffs: ");
        int idX = 0;
        for (int i = 3; i < contents.length; i++) {
          coeffs[idX] = Double.valueOf(contents[i]);
          //System.out.print( coeffs[idX] + ",");
          idX += 1;
        }
        //System.out.println();

        //Make the predictions:
      	// Subtract sample mean
        ArrayList<Long> currentLoadCopy = new ArrayList<Long>(currentLoad);
  		for (int f = 0; f < currentLoad.size(); f++){
  			currentLoadCopy.set(f, (currentLoad.get(f)- (long) xMean) );
  		}

      	for (int j = 0; j < numPredsAhead; j++) {
      		double result =  xIntercept;
      		for (int k = 0; k < coeffs.length; k++) {
      			double temp = coeffs[k] * currentLoadCopy.get( (currentLoadCopy.size()-1)-k );
      			result += temp;
      		}
      		currentLoadCopy.add( (long) result );
      		preds.add( (long) result );
		}

  		for (int f = 0; f < preds.size(); f++){
  			preds.set(f, (preds.get(f) + (long) xMean) );
  		}
    }

    	return preds;
    }
}
