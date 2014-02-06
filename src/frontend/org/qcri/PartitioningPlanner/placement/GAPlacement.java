package org.qcri.PartitioningPlanner.placement;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jaga.definitions.GAParameterSet;
import org.jaga.definitions.GAResult;
import org.jaga.definitions.Individual;
import org.jaga.definitions.ReproductionAlgorithm;
import org.jaga.hooks.AnalysisHook;
import org.jaga.individualRepresentation.greycodedNumbers.NDecimalsIndividual;
import org.jaga.individualRepresentation.greycodedNumbers.NDecimalsIndividualSimpleFactory;
import org.jaga.individualRepresentation.greycodedNumbers.RangeConstraint;
import org.jaga.masterAlgorithm.ReusableSimpleGA;
import org.jaga.masterAlgorithm.InitialPopulationGA;
import org.jaga.reproduction.greycodedNumbers.SimpleBinaryXOverWithMutation;
import org.jaga.selection.RouletteWheelSelection;
import org.jaga.util.DefaultParameterSet;
import org.jaga.util.FittestIndividualResult;
import org.voltdb.utils.Pair;
import org.qcri.PartitioningPlanner.placement.Plan;

public class GAPlacement extends Placement {
	
	Long coldPartitionWidth = 1000L; // redistribute cold tuples in chunks of 1000
	ArrayList<Long> tupleIds = null;
	ArrayList<Long> accesses = null; 
	ArrayList<Integer> locations = null; 
	ArrayList<List<Plan.Range>> slices = null;
	ArrayList<Long> sliceSizes = null;
	int tupleCount = 0;
	int sliceCount = 0;
	Long totalAccesses = 0L;
	
	public GAPlacement(){
		
	}
	
	// initialize the private data members based on the input parameters
		private void init(ArrayList<Map<Long, Pair<Long,Integer> >> hotTuplesList, Map<Integer, Long> partitionTotals, Plan aPlan, int partitionCount) {
			tupleIds = new ArrayList<Long>();
			accesses = new ArrayList<Long>(); 
			locations = new ArrayList<Integer>(); 
			slices = new ArrayList<List<Plan.Range>>();
			sliceSizes = new ArrayList<Long>();

			// copy partitionTotals into oldLoad
			totalAccesses = 0L;
			Map<Integer, Long> oldLoad = new HashMap<Integer, Long> ();
			for(Integer i : partitionTotals.keySet()) {
				totalAccesses += partitionTotals.get(i);
				oldLoad.put(i,  partitionTotals.get(i));
			}

			// copy aPlan into oldPlan
			Plan oldPlan = new Plan();
			Map<Integer, List<Plan.Range>> ranges = aPlan.getAllRanges();
			for(Integer i : ranges.keySet()) {
				List<Plan.Range> partitionRanges = ranges.get(i);
				oldPlan.addPartition(i);
				for(Plan.Range range : partitionRanges) {
					oldPlan.addRange(i, range.from, range.to);
				}
			}

			// calculate the load and plan if the hot tuples were removed, store
			// them in oldLoad and oldPlan
			tupleCount = 0;
			Integer partitionId = 0;
			for(Map<Long, Pair<Long,Integer> >  hotTuples : hotTuplesList) {
				tupleCount += hotTuples.keySet().size();
				for(Long i : hotTuples.keySet()) {
					oldLoad.put(partitionId, oldLoad.get(partitionId) - hotTuples.get(i).getFirst());
					oldPlan.removeTupleId(partitionId, i);
				}
				++partitionId;
			}

			// store the ids, access counts, and locations of each of the hot tuples
			partitionId = 0;
			for(Map<Long, Pair<Long,Integer> >  hotTuples : hotTuplesList) {
				for(Long i : hotTuples.keySet()) {
					tupleIds.add(i);
					sliceSizes.add((long) hotTuples.get(i).getSecond().intValue());
					accesses.add(hotTuples.get(i).getFirst());
					locations.add(partitionId);
				}
				++partitionId;
			}

			int coldAccesses = 0;
                        for(Integer i : oldLoad.keySet()) {
                                coldAccesses += oldLoad.get(i);
                        }
                        int meanColdAccesses = coldAccesses / partitionCount;

			// store the ranges, sizes, access counts, and locations of each of the slices of cold tuples
			sliceCount = 0;
			for(Integer i : oldPlan.getAllRanges().keySet()) { // for each partition
				// VOTER HACK: we want each partition slice to contain ~1000 tuples, but we don't know how many tuples
				// are in a range
				long denom = Math.max(partitionTotals.get(i), coldPartitionWidth);
				List<List<Plan.Range>> partitionSlices = oldPlan.getRangeSlices(i,  coldPartitionWidth * maxPhoneNumber / denom);
				if(partitionSlices.size() > 0) {
					sliceCount += partitionSlices.size();
					Double tupleWeight = (double) oldLoad.get(i) / meanColdAccesses; // per tuple - VOTER HACK
					for(List<Plan.Range> slice : partitionSlices) {  // for each slice
						// VOTER HACK
						Long sliceSize = (long) (Plan.getRangeListWidth(slice) * (double) partitionTotals.get(i) / maxPhoneNumber);
						Long newWeight = (long) (tupleWeight *  ((double) sliceSize));
						slices.add(slice);
						sliceSizes.add(sliceSize);
						accesses.add(newWeight);
						locations.add(i);
					} // end for each slice
				}
			} // end for each partition
		}
		
	
	// hotTuples: tupleId --> access count
	// siteLoads: partitionId --> total access count
	public Plan computePlan(ArrayList<Map<Long, Pair<Long,Integer> >> hotTuplesList, Map<Integer, Long> partitionTotals, String planFilename, int partitionCount, int timeLimit){
		
		Plan aPlan = new Plan(planFilename);
		this.init(hotTuplesList, partitionTotals, aPlan, partitionCount);

		int placementCount = tupleCount + sliceCount; // number of placements we will make
		Long meanAccesses = totalAccesses / partitionCount;

		System.out.println("Mean access count: " + meanAccesses);
		
		// Set up the genetic algorithm
		GAParameterSet params = new DefaultParameterSet();
		params.setPopulationSize(50);
		GAPlacementFitness fitness = new GAPlacementFitness();
		fitness.initialize(tupleIds,accesses,locations,slices,sliceSizes,tupleCount,
				sliceCount,totalAccesses,partitionCount); 
		params.setFitnessEvaluationAlgorithm(fitness);
		params.setSelectionAlgorithm(new RouletteWheelSelection(-10E10));
		params.setMaxGenerationNumber(50);
		ReproductionAlgorithm reprAlg = new SimpleBinaryXOverWithMutation(0.65, 0.05);
		params.setReproductionAlgorithm(reprAlg);
		int precision = (int) Math.ceil(Math.log(partitionCount)/Math.log(2)) + 1;
		NDecimalsIndividualSimpleFactory fact = new NDecimalsIndividualSimpleFactory(placementCount, 0, precision);
		for(int i = 0; i < placementCount; ++i) {
			fact.setConstraint(i, new RangeConstraint(0, partitionCount-1));
		}
		params.setIndividualsFactory(fact);
		InitialPopulationGA ga = new InitialPopulationGA();
		
		// seed the initial population with individuals matching the current plan
		Individual [] initialIndivs = new Individual[5];
		for(int i = 0; i < 5; ++i) {
			NDecimalsIndividual indiv = new NDecimalsIndividual(placementCount, 1, precision);
			for(int j = 0; j < locations.size(); j++) {
			    indiv.setDoubleValue(j, (double) locations.get(j));
			}
			initialIndivs[i] = indiv;
		}
		ga.setInitialPopulation(initialIndivs);
		
		// Analysis for debugging
		//AnalysisHook hook = new AnalysisHook();
		//hook.setLogStream(System.out);
		//hook.setUpdateDelay(100);
		//hook.setAnalyseGenMinFit(true);
		//ga.addHook(hook);

		// Execute the genetic algorithm
		GAResult result = ga.exec(params);
		FittestIndividualResult fittestResult = (FittestIndividualResult) result;
		NDecimalsIndividual indiv = (NDecimalsIndividual) fittestResult.getFittestIndividual();

		// Update the plan based on the results
		for(int i = 0; i < indiv.getSize(); ++i) {
			Integer srcPartition = locations.get(i);
			Integer dstPartition = (int) indiv.getDoubleValue(i);
			if(srcPartition != dstPartition) {
				if(i < tupleCount) {
					Long id = tupleIds.get(i);
					aPlan.removeTupleId(srcPartition, id);
					if(!aPlan.hasPartition(dstPartition)) {
						aPlan.addPartition(dstPartition);
					}
					aPlan.addRange(dstPartition, id, id);
				}
				else {
					List<Plan.Range> slice = slices.get(i - tupleCount);
					for(Plan.Range r : slice) { 
						if(!aPlan.hasPartition(dstPartition)) {
							aPlan.addPartition(dstPartition);
						}

						List<Plan.Range> oldRanges = aPlan.getRangeValues(srcPartition, r.from, r.to);
						for(Plan.Range oldRange : oldRanges) {
							aPlan.removeRange(srcPartition, oldRange.from);
							aPlan.addRange(dstPartition, Math.max(oldRange.from, r.from), Math.min(oldRange.to, r.to));

							if(oldRange.from < r.from) {
								aPlan.addRange(srcPartition, oldRange.from, r.from - 1);
							}
							if(r.to < oldRange.to) {
								aPlan.addRange(srcPartition, r.to + 1, oldRange.to);
							}
						}
					}
				}
			}
		}

		

		aPlan = demoteTuples(hotTuplesList, aPlan);
		removeEmptyPartitions(aPlan);
		return aPlan;
		
	}
	

}
