package org.qcri.PartitioningPlanner.placement;


import org.qcri.PartitioningPlanner.placement.Plan;
import org.qcri.PartitioningPlanner.placement.GreedyPlacement;

import edu.brown.BaseTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;



public class TestGreedyPlacement extends BaseTestCase {
	static Integer partitionCount = 4;
	static Long tupleCount = 10000L; // 10k tuples in a table
	static int seed = 1024;
	static Long hotTupleCount = 30L;
	static Long accessRange = 1024L; 
	static Long hotTupleRange = 128L;

	
    
	public void testGreedyPlacement() throws Exception {

		// start with an evenly distributed plan
		Plan aPlan = new Plan(); // start with uniform case
		Random generator = new Random(seed);
		GreedyPlacement aPlacement = new GreedyPlacement();

		Map<Integer, Long> partitionTotals = new HashMap<Integer, Long>();  // partitionID --> summed access count
		ArrayList<Map<Long, Long>> hotTuplesList = new ArrayList<Map<Long, Long>>();
		
		Long tuplesPerInstance = tupleCount / partitionCount;
		Long modulusCount = tupleCount % partitionCount;
		if(modulusCount > 0) {
			++tuplesPerInstance;
		}
		
		Long startRange = 0L;
		Long endRange = tuplesPerInstance - 1; // inclusive
		
		for(Integer i = 0; i < partitionCount; ++i) {
			aPlan.addPartition(i);
			aPlan.addRange(i, startRange, endRange);
			Long iTmp = Long.parseLong(String.valueOf(i));

			if(iTmp == modulusCount && modulusCount > 0) {
				--tuplesPerInstance;
			}
			startRange = endRange + 1;
			endRange = startRange + tuplesPerInstance - 1;
			hotTuplesList.add(new HashMap<Long, Long>());  // tupleId --> summed access count

		}

		System.out.println("Started with plan:");
		aPlan.printPlan();
		

		for(Integer i = 0; i < partitionCount; ++i) {
			partitionTotals.put(i, Math.abs(generator.nextLong()) % accessRange);			
		}
		
		for(Integer i = 0; i < hotTupleCount; ++i) {
			Long tupleId = Math.abs(generator.nextLong()) % tupleCount;
			Integer tupleLocation = aPlan.getTuplePartition(tupleId);
			Long accessCount =  Math.abs(generator.nextLong()) % hotTupleRange;
			hotTuplesList.get(tupleLocation).put(tupleId, accessCount);

			//add capacity for partitionTotals
			partitionTotals.put(tupleLocation, accessCount + partitionTotals.get(tupleLocation)); 		
				
		}

		System.out.println("Starting with load:");
		for(Integer i : partitionTotals.keySet()) {
			System.out.println("Partition " + i + ": " + partitionTotals.get(i));
		}
		

		aPlan = aPlacement.computePlan(hotTuplesList, partitionTotals,  aPlan);

		System.out.println("Ending with plan:");
		aPlan.printPlan();

		System.out.println("Ending with load:");
		for(Integer i : partitionTotals.keySet()) {
			System.out.println("Partition " + i + ": " + partitionTotals.get(i));
		}
		aPlan.toJSON("test.txt");

	}
	
}