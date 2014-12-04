package org.qcri.affinityplanner;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.voltdb.catalog.Catalog;

public class GraphPartitioner extends AffinityGraph {
    
    public int repartition (double minLoadPerPartition, double maxLoadPerPartition, Catalog catalog) {
        int addedPartitions = 0;

        System.out.println("Calculating site loads");
        
        /*
         *  SCALE OUT
         */

        // detect overloaded and active partitions
        Set<Integer> activePartitions = new HashSet<Integer>();
        Set<Integer> overloadedPartitions = new HashSet<Integer>();
        for(int i = 0; i < Controller.MAX_PARTITIONS; i++){
            if(!m_partitionVertices.get(i).isEmpty()){
                activePartitions.add(i);
                System.out.println(getLoadPerPartition(i));
                if (getLoadPerPartition(i) > maxLoadPerPartition){
                    overloadedPartitions.add(i);
                }
            }
        }

        if (! overloadedPartitions.isEmpty()){
            // offload each overloaded partition
            for(Integer overloadedPartition : overloadedPartitions){
                
                // DEBUG
                System.out.println("SCALING OUT");
                System.out.println("#######################");
                System.out.println("offloading site " + overloadedPartition);
    
                // get hottest vertices. the actual length of the array is min(Controller.MAX_MOVED_VERTICES, #tuples held site);
                List<String> hotVerticesNotMoved = getHottestVertices(overloadedPartition, Controller.MAX_MOVED_VERTICES_PER_SOURCE_SITE);
                final int actualMaxMovedVertices = hotVerticesNotMoved.size();
    
                // DEBUG
    //            System.out.println("hot vertices:");
    //            for (String hotVertex : hotVerticesNotMoved){
    //                System.out.println(hotVertex);
    //            }
    
                int currHotVertex = 0;
                int numMovedVertices = 0;
                Set<String> movingVertices = new HashSet<String>();
                while(getLoadPerPartition(overloadedPartition) > maxLoadPerPartition){
                    
                    System.out.println("--------------------");
    
                    // Step 1) add partition if I have over-expanded movingVertices. 
                    // We expand movingVertices only if all partitions reject the previous movingVertices sets.
                    // We fill up low-order partitions first to minimize the number of servers
                    if (numMovedVertices + movingVertices.size() >= actualMaxMovedVertices){
                        System.out.println("Adding a new partition");
                        if(addedPartitions < Controller.MAX_PARTITIONS_ADDED_RECONF){
                            currHotVertex = 0;
                            movingVertices.clear();
                            addPartitions(1);
                            addedPartitions++;
                            for(int i = 0; i < Controller.MAX_PARTITIONS; i++){
                                if(!activePartitions.contains(i)){
                                    activePartitions.add(i);
                                }
                            }
                        }
                        else{
                            System.out.println("Partition " + overloadedPartitions + " cannot be offloaded.");
                            return -1;
                        }
                    }
                    System.out.println("Adding a new vertex");
                    // Step 2) add one vertex to movingVertices
                    if (movingVertices.isEmpty()){
                        // if empty, add a new hot vertex
                        assert (currHotVertex < hotVerticesNotMoved.size()); // If all hot vertices are elsewhere, I have already moved actualMaxMovedVertices so I should not be here 
                        String nextHotVertex = null;
                        do{
                            nextHotVertex = hotVerticesNotMoved.get(currHotVertex);
                            currHotVertex++;
                        } while (m_vertexPartition.get(nextHotVertex) != overloadedPartition);
                        assert (nextHotVertex != null); // If all hot vertices are elsewhere, I have already moved actualMaxMovedVertices so I should not be here
                        System.out.println("Adding hot vertex " + nextHotVertex);
                        movingVertices.add(nextHotVertex);
                    } // END if(movedVertices.isEmpty())
                    else{
                        // extend current moved set
                        // assess gain with extension
                        double deltaEdgeExtension = Double.MIN_VALUE;                        
                        String nextEdgeExtension = getMostAffineExtension(movingVertices);
                        if(nextEdgeExtension != null){
                            movingVertices.add(nextEdgeExtension);
                            deltaEdgeExtension = getDeltaGiveVertices(movingVertices, -1);
                            movingVertices.remove(nextEdgeExtension);
                        }
                        // assess gain with next hot tuple. may need to skip a few hot tuples that are already included in hottestVerticesToMove. 
                        double deltaHotTuple = Double.MIN_VALUE;
                        String nextHotVertex = null;
                        int skip = 0;
                        do{
                            skip ++;
                            nextHotVertex = hotVerticesNotMoved.get(currHotVertex + skip);
                        } while (movingVertices.contains(nextHotVertex)); // I could also check (currHotVertex + jump < hottestVerticesToMove.size()) but if all hot vertices are elsewhere, I have already moved actualMaxMovedVertices so I should not be here
                        assert(nextHotVertex != null);
                        movingVertices.add(nextHotVertex);
                        deltaHotTuple = getDeltaGiveVertices(movingVertices, -1);
                        movingVertices.remove(nextHotVertex);
                        // pick best available choice
                        if (deltaEdgeExtension < deltaHotTuple){
                            movingVertices.add(nextEdgeExtension);
                            System.out.println("Adding edge extension " + nextEdgeExtension);
                        }
                        else{
                            movingVertices.add(nextHotVertex);
                            System.out.println("Adding hot vertex " + nextHotVertex);
                            currHotVertex += skip;
                        }
                    } // END if(!movedVertices.isEmpty())
    
                    // DEBUG
                    System.out.println("Moving vertices: ");
                    for (String vertex : movingVertices){
                        System.out.println(vertex);
                    }
    
                    // Step 3) search site to offload (might not find it)
                    // first try to offload to local partitions
                    boolean moved = false;
                    Collection<Integer> localPartitions = PlanHandler.getPartitionsSite(PlanHandler.getSitePartition(overloadedPartition));
                    for(Integer toPartition : localPartitions){
                        if(!overloadedPartitions.contains(toPartition)){
                            System.out.println("Trying with partition " + toPartition);
                            int movedVertices = tryMoveVertices(movingVertices, overloadedPartition, toPartition, new Double(maxLoadPerPartition), hotVerticesNotMoved);
                            if(movedVertices > 0){
                                numMovedVertices += movedVertices;                            
                                moved = true;
                                break;
                            }
                        }
                    }
                    // then try to offload to remote partitions
                    if (!moved){
                        for(Integer toPartition : activePartitions){
                            if(!overloadedPartitions.contains(toPartition) && !localPartitions.contains(toPartition)){
                                System.out.println("Trying with partition " + toPartition);
                                int movedVertices = tryMoveVertices(movingVertices, overloadedPartition, toPartition, new Double(maxLoadPerPartition), hotVerticesNotMoved);
                                if(movedVertices > 0){
                                    numMovedVertices += movedVertices;    
                                    break;
                                }
                            }
                        }
                    }
                } // END while(getLoadPerSite(overloadedPartition) <= maxLoadPerSite)
            }// END for(Integer overloadedPartition : overloadedPartitions)
        } // END if (!overloadedPartitions.isEmpty())
        else{
            /*
             *  SCALE IN
             *  
             *  very simple policy: if a partition is underloaded, try to move its whole content to another partition
             */
            
            System.out.println("SCALING IN");
            
            // detect underloaded partitions
            TreeSet<Integer> underloadedPartitions = new TreeSet<Integer>();
            for(Integer part : activePartitions){
                if (getLoadPerPartition(part) < minLoadPerPartition){
                    underloadedPartitions.add(part);
                }
            }
            // offload from partitions with higher id to partitions with lower id. this helps emptying up the latest servers.
            Iterator<Integer> descending = underloadedPartitions.descendingIterator();
            HashSet<Integer> removedPartitions = new HashSet<Integer>();
            while(descending.hasNext()){
                Integer underloadedPartition = descending.next();
                System.out.println("Offloading partition " + underloadedPartition);
                Set<String> movingVertices = new HashSet<String>();
                movingVertices.addAll(m_partitionVertices.get(underloadedPartition));
                // try to offload to remote partitions
                Collection<Integer> localPartitions = PlanHandler.getPartitionsSite(PlanHandler.getSitePartition(underloadedPartition));
                for(Integer toPartition : activePartitions){
                    if(!overloadedPartitions.contains(toPartition) 
                            && !localPartitions.contains(toPartition) 
                            && !removedPartitions.contains(toPartition)){
                        System.out.println("Trying with partition " + toPartition);
                        int movedVertices = tryMoveVertices(movingVertices, underloadedPartition, toPartition, new Double(maxLoadPerPartition), null);
                        if(movedVertices > 0){
                            removedPartitions.add(underloadedPartition);
                            break;                            
                        }
                    }
                }
            }
            activePartitions.removeAll(removedPartitions);
        } // END if(overloadedPartitions.isEmpty())
        return 1;
    }

    /*
     * finds the LOCAL vertex with the highest affinity
     * 
     * ASSUMES that all vertices are on the same partition
     */
    private String getMostAffineExtension(Set<String> vertices){
        double maxAdjacency = -1;
        String res = null;
        int partition = m_vertexPartition.get(vertices.iterator().next());
        for(String vertex : vertices){
            Map<String,Double> adjacency = m_edges.get(vertex);
            for(Map.Entry<String, Double> edge : adjacency.entrySet()){
                if (edge.getValue() > maxAdjacency
                        && m_vertexPartition.get(edge.getKey()) == partition
                        && !vertices.contains(edge.getKey())){
                    maxAdjacency = edge.getValue();
                    res = edge.getKey();
                }
            }
        }
        return res;
    }
    
    /*
     * Tries to move movingVertices from overloadedPartition to toPartition. 
     * Fails if the move does not result in a minimal gain threshold for the fromPartition OR
     * if the toPartition becomes overloaded as an effect of the transfer.
     * Returns the number of partitions actually moved.
     */
    private int tryMoveVertices(Set<String> movingVertices, Integer fromPartition, Integer toPartition, Double maxLoadPerPartition, List<String> hotVerticesNotMoved) {
        int numMovedVertices = 0;
        double deltaFromPartition = getDeltaGiveVertices(movingVertices, toPartition);
        double deltaToPartition = getDeltaReceiveVertices(movingVertices, toPartition);
        // check that I get enough overall gain and the additional load of the receiving site does not make it overloaded
        System.out.println("Deltas from " + deltaFromPartition + " - to " + deltaToPartition);
        if(deltaFromPartition <= Controller.MIN_DELTA_FOR_MOVEMENT 
                && getLoadPerPartition(toPartition) + deltaToPartition < maxLoadPerPartition){   // if gainToSite is negative, the load of the receiving site grows
            System.out.println("Moving to partition " + toPartition);
            System.out.println("Weights before moving " + getLoadPerPartition(fromPartition) + " " + getLoadPerPartition(toPartition));
            moveVertices(movingVertices, fromPartition, toPartition);
            System.out.println("Weights after moving " + getLoadPerPartition(fromPartition) + " " + getLoadPerPartition(toPartition));
            if(hotVerticesNotMoved != null){
                hotVerticesNotMoved.removeAll(movingVertices);
            }
            numMovedVertices = movingVertices.size();
            movingVertices.clear();
        }
        return numMovedVertices;
    }
}
