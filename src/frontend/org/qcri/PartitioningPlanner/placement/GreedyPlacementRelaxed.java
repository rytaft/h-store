package org.qcri.PartitioningPlanner.placement;


import java.util.ArrayList;

import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.qcri.PartitioningPlanner.placement.Plan;
import org.voltdb.CatalogContext;


public class GreedyPlacementRelaxed extends Placement {
    
    Long coldPartitionWidth = 100000L; // redistribute cold tuples in chunks of 100000 if shrinking
    Double stdDeviationsThreshold = 3.0D;
    Boolean pointedSkew = false;
    
    public GreedyPlacementRelaxed(){
        
    }
    
    public GreedyPlacementRelaxed(int partPerSite){
        super(partPerSite);
    }
    
ArrayList<Map<Long, Long>>  clone(ArrayList<Map<Long, Long>> hotTuplesList) {
        ArrayList<Map<Long, Long>>  hotTuplesListCopy = new ArrayList<Map<Long, Long>>();       
        
        for(Integer i = 0; i < hotTuplesList.size(); ++i) {
            hotTuplesListCopy.add(new HashMap<Long, Long>());
            for(Long j : hotTuplesList.get(i).keySet()) {
                hotTuplesListCopy.get(i).put(j, hotTuplesList.get(i).get(j));
            }
        }
        
        return hotTuplesListCopy;
}

    
    Long calculateThreshold(ArrayList<Map<Long, Long>> hotTuplesList) {
        
        Long totalAccesses = 0L;
        Integer tupleCount = 0;
        Long meanAccesses;
        Long sse = 0L;
        Double stdDeviation;
    
        
        // get mean of set
        for(Integer i = 0; i < hotTuplesList.size(); ++i) {
            for(Long j : hotTuplesList.get(i).keySet()) {
                totalAccesses += hotTuplesList.get(i).get(j);
                ++tupleCount;
            }
        }
        
        meanAccesses = totalAccesses / tupleCount;
        
        // get sum squared error of the set
        for(Integer i = 0; i < hotTuplesList.size(); ++i) {
            for(Long j : hotTuplesList.get(i).keySet()) {
                sse += (hotTuplesList.get(i).get(j) - meanAccesses) * (hotTuplesList.get(i).get(j) - meanAccesses);
            }
        }
        
        stdDeviation =  Math.sqrt(sse/tupleCount);
        
        return meanAccesses + Math.round(stdDeviationsThreshold * stdDeviation);
        
    }
    
    ArrayList<Map<Long, Long>> getPointedSkew(ArrayList<Map<Long, Long>> hotTuplesList) {
        
        Long threshold = calculateThreshold(hotTuplesList);
        ArrayList<Map<Long, Long>> outTuples = clone(hotTuplesList);
        Long accessCount;



        // filter it for skew
        for(Integer i = 0; i < hotTuplesList.size(); ++i) {
            for(Long j : hotTuplesList.get(i).keySet()) {
                accessCount = hotTuplesList.get(i).get(j);
                if(accessCount < threshold) {
                    outTuples.get(i).remove(j);
                }
            }
        }
                
        return outTuples;
    }
    
    ArrayList<Map<Long, Long>> getSkew(ArrayList<Map<Long, Long>> hotTuplesList, Long totalDeviations) {
        ArrayList<Map<Long, Long>> outTuples = new ArrayList<Map<Long, Long>>();
        ArrayList<Map<Long, Long>> hotTuplesCopy = clone(hotTuplesList);
        Long deviationsAssigned = 0L;
                
        // init outTuples with partition ids and no tuples      
        for(Integer i = 0; i < hotTuplesList.size(); ++i) {
            outTuples.add(new HashMap<Long, Long>());
        }
        
        
        while(deviationsAssigned < totalDeviations) {
            getHottestTuple(hotTuplesCopy);
            deviationsAssigned += _hotAccessCount;
            outTuples.get(_srcPartition).put(_hotTupleId, _hotAccessCount);
            hotTuplesCopy.get(_srcPartition).remove(_hotTupleId);
        }
        
        return outTuples;
    }
    
    // hotTuples: tupleId --> access count
    // siteLoads: partitionId --> total access count
    public Plan computePlan(ArrayList<Map<Long, Long>> hotTuplesList, Map<Integer, Long> partitionTotals, String planFilename, int partitionCount, int timeLimit, CatalogContext catalogContext){
        
        Integer dstPartition = -1;
        Long totalAccesses = 0L;
        Long meanAccesses;
        Long totalDeviations = 0L;
        Long hotTupleCount = 0L;
        
        Plan aPlan = new Plan(planFilename);

        for(Integer i : partitionTotals.keySet()) {
            totalAccesses = totalAccesses + partitionTotals.get(i);         
        }

        meanAccesses = totalAccesses / partitionCount;
        //System.out.println("Mean access count: " + meanAccesses);

        for(Integer i : partitionTotals.keySet()) {
            if(partitionTotals.get(i) > meanAccesses) {
                Long t = partitionTotals.get(i) - meanAccesses;
                totalDeviations += partitionTotals.get(i) - meanAccesses;
            }
        }

        
        for(int i = 0; i < partitionCount; ++i) {
            if(partitionTotals.get(i) == null) {
                partitionTotals.put(i, 0L);
            }
        }
        
        

        if(pointedSkew) {
            hotTuplesList = getPointedSkew(hotTuplesList);          
        }
        else {
            hotTuplesList = getSkew(hotTuplesList, totalDeviations);
        }
        
        for(Map<Long, Long> hotTuples : hotTuplesList) {
            hotTupleCount = hotTupleCount + hotTuples.size();
        }

        ArrayList<Map<Long, Long>> hotTuplesListCopy = new ArrayList<Map<Long, Long>> ();
        hotTuplesListCopy.addAll(hotTuplesList);

        System.out.println("Received " + hotTupleCount + " hot tuples.");
        
        
        for(Long i = 0L; i < hotTupleCount; ++i) {
            getHottestTuple(hotTuplesList);
            //System.out.println("Processing hot tuple id " + _hotTupleId + " with access count " + _hotAccessCount);

            if(partitionTotals.get(_srcPartition) > meanAccesses || _srcPartition >= partitionCount) {
                    dstPartition = getMostUnderloadedPartitionId(partitionTotals, partitionCount, true);
                    if(dstPartition != _srcPartition) {
                        //System.out.println(" sending it to " + dstPartition);
                        partitionTotals.put(_srcPartition, partitionTotals.get(_srcPartition)  - _hotAccessCount);
                        partitionTotals.put(dstPartition,partitionTotals.get(dstPartition)  + _hotAccessCount);
                        aPlan.removeTupleId(_srcPartition, _hotTupleId);
                        if(!aPlan.hasPartition(dstPartition)) {
                            aPlan.addPartition(dstPartition);
                        }
                        aPlan.addRange(dstPartition, _hotTupleId, _hotTupleId);
                    }
                }
            hotTuplesList.get(_srcPartition).remove(_hotTupleId);

        }

        // If we are shrinking the number of partitions, use the first fit approach 
        // to place the cold tuples from the deleted partitions
        for(Integer i : aPlan.getAllRanges().keySet()) { // foreach partition
            if(i.intValue() >= partitionCount) { // in case of shrinking number of partitions
                List<List<Plan.Range>> partitionSlices = aPlan.getRangeSlices(i,  coldPartitionWidth);
                if(partitionSlices.size() > 0) {
                        Double tupleWeight = ((double) partitionTotals.get(i)) / aPlan.getTupleCount(i); // per tuple

                    for(List<Plan.Range> slice : partitionSlices) {  // for each slice

                        Boolean placed = false;
                        Integer newWeight = (int) (tupleWeight *  ((double) Plan.getRangeListWidth(slice)));

                        for(Integer k : partitionTotals.keySet()) {
                            if(partitionTotals.get(k) + newWeight <= meanAccesses) {
                                dstPartition = k;
                                placed = true;
                                break;
                            }
                        }
                        if(!placed) {
                            dstPartition = getMostUnderloadedPartitionId(partitionTotals, partitionCount);
                        }
                        for(Plan.Range r : slice) { 
                            if(!aPlan.hasPartition(dstPartition)) {
                                aPlan.addPartition(dstPartition);
                            }
                            aPlan.removeRange(i, r.from);
                            aPlan.addRange(dstPartition, r.from, r.to);
                        }
                        partitionTotals.put(dstPartition, partitionTotals.get(dstPartition) + newWeight);

                    } // end for each slice
                }
            } // end in case of shrinking number of partitions
        } // end for each partition

        if(!catalogContext.jarPath.getName().contains("tpcc")) {
            aPlan = demoteTuples(hotTuplesListCopy, aPlan);
        }
        removeEmptyPartitions(aPlan);
        return aPlan;
        
    }
    

}
