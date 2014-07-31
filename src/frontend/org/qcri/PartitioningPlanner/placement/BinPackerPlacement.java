package org.qcri.PartitioningPlanner.placement;

import org.gnu.glpk.GLPK;
import org.gnu.glpk.glp_prob;
import org.gnu.glpk.glp_iocp;
import org.gnu.glpk.SWIGTYPE_p_int;
import org.gnu.glpk.SWIGTYPE_p_double;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.voltdb.utils.Pair;
import org.qcri.PartitioningPlanner.placement.Plan;
import org.voltdb.CatalogContext;


public class BinPackerPlacement extends Placement {

	Long coldPartitionWidth = 100000L; // redistribute cold tuples in chunks of 100000
	ArrayList<Long> tupleIds = null;
	ArrayList<Long> accesses = null; 
	ArrayList<Integer> locations = null; 
	ArrayList<List<Plan.Range>> slices = null;
	ArrayList<Long> sliceSizes = null;
	int tupleCount = 0;
	int sliceCount = 0;
	Long totalAccesses = 0L;

	public BinPackerPlacement(){
		tupleIds = new ArrayList<Long>();
		accesses = new ArrayList<Long>(); 
		locations = new ArrayList<Integer>(); 
		slices = new ArrayList<List<Plan.Range>>();
		sliceSizes = new ArrayList<Long>();
	}

	// initialize the private data members based on the input parameters
	private void init(ArrayList<Map<Long, Pair<Long,Integer> >> hotTuplesList, Map<Integer, Pair<Long,Integer>> partitionTotals, Plan aPlan, int partitionCount) {
		tupleIds = new ArrayList<Long>();
		accesses = new ArrayList<Long>(); 
		locations = new ArrayList<Integer>(); 
		slices = new ArrayList<List<Plan.Range>>();
		sliceSizes = new ArrayList<Long>();

		// copy partitionTotals into oldLoad
		totalAccesses = 0L;
		Map<Integer, Pair<Long,Integer>> oldLoad = new HashMap<Integer, Pair<Long,Integer>> ();
		for(Integer i : partitionTotals.keySet()) {
			totalAccesses += partitionTotals.get(i).getFirst();
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
				oldLoad.put(partitionId, new Pair<Long, Integer>(oldLoad.get(partitionId).getFirst() - hotTuples.get(i).getFirst(), 
						oldLoad.get(partitionId).getSecond() - hotTuples.get(i).getSecond()));
				oldPlan.removeTupleId(partitionId, i);
			}
			++partitionId;
		}

	    // store the ids, access counts, and locations of each of the hot tuples
		partitionId = 0;
		for(Map<Long, Pair<Long,Integer> >  hotTuples : hotTuplesList) {
			for(Long i : hotTuples.keySet()) {
				tupleIds.add(i);
				sliceSizes.add(hotTuples.get(i).getSecond().longValue());
				accesses.add(hotTuples.get(i).getFirst());
				locations.add(partitionId);
			}
			++partitionId;
		}

		// store the ranges, sizes, access counts, and locations of each of the slices of cold tuples
		sliceCount = 0;
		for(Integer i : oldPlan.getAllRanges().keySet()) { // for each partition
			// VOTER HACK: we want each partition slice to contain ~1000 tuples, but we don't know how many tuples
			// are in a range
			Double tuplesPerKey = (double) oldLoad.get(i).getSecond() / Plan.getRangeListWidth(oldPlan.getAllRanges(i));
			List<List<Plan.Range>> partitionSlices = oldPlan.getRangeSlices(i,  (long) (coldPartitionWidth / tuplesPerKey));
			if(partitionSlices.size() > 0) {
				sliceCount += partitionSlices.size();
				Double tupleWeight = (double) oldLoad.get(i).getFirst() / oldLoad.get(i).getSecond(); // per tuple - VOTER HACK
				for(List<Plan.Range> slice : partitionSlices) {  // for each slice
					// VOTER HACK
					Long sliceSize = (long) (Plan.getRangeListWidth(slice) * tuplesPerKey);
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
	public Plan computePlan(ArrayList<Map<Long, Pair<Long,Integer> >> hotTuplesList, Map<Integer, Pair<Long,Integer>> partitionTotals, String planFile, int partitionCount, int timeLimit, CatalogContext catalogContext){

		Plan aPlan = new Plan(planFile);
		this.init(hotTuplesList, partitionTotals, aPlan, partitionCount);

		glp_prob lp = GLPK.glp_create_prob();

		GLPK.glp_set_prob_name(lp, "min_bandwidth");
		GLPK.glp_set_obj_dir(lp, GLPK.GLP_MIN);

		int placementCount = tupleCount + sliceCount; // number of placements we will make
		SWIGTYPE_p_int idxX = GLPK.new_intArray(placementCount * partitionCount * 2 + 1);
		SWIGTYPE_p_int idxY = GLPK.new_intArray(placementCount * partitionCount * 2 + 1);
		SWIGTYPE_p_double idxR = GLPK.new_doubleArray(placementCount * partitionCount * 2 + 1);

		glp_iocp parm = new glp_iocp();
		GLPK.glp_init_iocp(parm);
		parm.setPresolve(GLPK.GLP_ON);
		parm.setTm_lim(timeLimit);

		// reserved for glpk use in C++
		GLPK.intArray_setitem(idxX, 0, 0);
		GLPK.intArray_setitem(idxY, 0, 0);
		GLPK.doubleArray_setitem(idxR, 0, 0);

		getHottestTuple(hotTuplesList);
		Long meanAccesses = totalAccesses / partitionCount;
		System.out.println("Mean access count: " + meanAccesses);

		double partitionUpperBound = Math.max(meanAccesses, _hotAccessCount) * 1.05; // slightly over target

		// one constraint for each partition for load balancing, one for each placement s.t. it appears exactly once
		GLPK.glp_add_rows(lp, partitionCount + placementCount);

		// load balancing constraints
		int i;
		for(i = 1; i < partitionCount+1; ++i) {
			GLPK.glp_set_row_bnds(lp, i, GLPK.GLP_UP, 0.0, partitionUpperBound);
		}

		// each placement must appear exactly once
		for(int j = 0; j < placementCount; ++j) {
			GLPK.glp_set_row_bnds(lp, i, GLPK.GLP_FX, 1.0, 1.0); // each placement must appear exactly once
			++i;
		}

		GLPK.glp_add_cols(lp, placementCount * partitionCount); // binary decision variable

		// d.v. x_{j, i} -  placement is inner dimension
		int k = 1;

		// set up bandwidth costs - objective function coeffs
		long cost;
		for(int j = 0; j < partitionCount; ++j) {
			for(i = 0; i < placementCount; ++i) {
				GLPK.glp_set_col_kind(lp, k, GLPK.GLP_BV); // _IV

				cost = ((locations.get(i) == j) ? 0 : sliceSizes.get(i)); 
				GLPK.glp_set_obj_coef(lp, k, cost);
				++k;
			}
		}

		// set up load balancing constraints
		int c = 1; // array position in idxR
		int d = 1; // array position in idxX and idxY
		for(int j = 1; j < partitionCount+1; ++j) {
			// list of tuple accesses are our coefficients
			for(Long access : accesses) {
				GLPK.doubleArray_setitem(idxR, c, access.doubleValue());
				++c;
			}

			for(i = 1; i < placementCount+1; ++i) {
				// a[j, i] = c_{i-1}
				GLPK.intArray_setitem(idxX, d, j);
				GLPK.intArray_setitem(idxY, d, i + (j - 1) * placementCount); // it is one long vector of decision variables
				++d;
			}
		}

		// load balancing constraints are correct
		k = partitionCount + 1; // first idx
		// each tuple must appear exactly once
		for(i = 1; i < placementCount + 1; ++i) {
			for(int j = 0; j < partitionCount; ++j) {
				//i.e. a[k+1, 1] = 1
				GLPK.intArray_setitem(idxX, d, k);
				GLPK.intArray_setitem(idxY, d, j * placementCount + i);
				GLPK.doubleArray_setitem(idxR, c, 1);
				++c;
				++d;
			}
			++k;
		}

		System.out.println("Initialized " + (d - 1) + " constraint variables, have " + GLPK.glp_get_num_bin(lp) + " binary cols.");
		GLPK.glp_load_matrix(lp, d-1, idxX, idxY, idxR);
		GLPK.glp_intopt(lp, parm);

		System.out.println("Solution cost " + GLPK.glp_mip_obj_val(lp));

		k = 1;
		for(int j = 0; j < partitionCount; ++j) {
			for(i = 0; i < placementCount; ++i) {
				if(GLPK.glp_mip_col_val(lp, k) != 0) {
					Integer srcPartition = locations.get(i);
					Integer dstPartition = j;
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
				++k;
			}
		}

		GLPK.glp_delete_prob(lp);
		
		if(!catalogContext.jarPath.getName().contains("tpcc")) {
			aPlan = demoteTuples(hotTuplesList, aPlan);
		}
		removeEmptyPartitions(aPlan);
		return aPlan;

	}

}
