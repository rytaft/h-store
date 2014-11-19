package org.qcri.affinityplanner;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class GraphPartitioner extends AffinityGraph {
    
    public int repartition (int minLoadPerPartition, int maxLoadPerPartition) {
        int currPartitions = getPartitionsNo();
        int addedPartitions = 0;

        System.out.println("Calculating site loads");
        
        // detect overloaded partitions
        Set<Integer> overloadedPartitions = new HashSet<Integer>();
        Set<Integer> underloadedPartitions = new HashSet<Integer>();
        for(int i = 0; i < currPartitions; i++){
            System.out.println(getLoadPerPartition(i));
            if (getLoadPerPartition(i) > maxLoadPerPartition){
                overloadedPartitions.add(i);
            }
            else if (getLoadPerPartition(i) < minLoadPerPartition){
                underloadedPartitions.add(i);
            }
        }

        if (! overloadedPartitions.isEmpty()){
            // offload each overloaded partition
            for(Integer overloadedPartition : overloadedPartitions){
                
                // DEBUG
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
    
                    // FIRST add one vertex to the movingVertices set
                    // add partition if I have over-expanded movingVertices. I expand movingVertices only if all partitions reject the previous movingVertices sets. 
                    if (numMovedVertices + movingVertices.size() >= actualMaxMovedVertices){
                        System.out.println("Adding a new partition");
                        if(addedPartitions < Controller.MAX_PARTITIONS_ADDED_RECONF){
                            addPartitions(1);
                            addedPartitions++;
                            currPartitions++;
                            currHotVertex = 0;
                            movingVertices.clear();
                        }
                        else{
                            System.out.println("Partition " + overloadedPartitions + " cannot be offloaded.");
                            return -1;
                        }
                    }
                    System.out.println("Adding a new vertex");
                    // add one vertex to movingVertices
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
                        int deltaEdgeExtension = Integer.MIN_VALUE;                        
                        String nextEdgeExtension = getMostAffineExtension(movingVertices);
                        if(nextEdgeExtension != null){
                            movingVertices.add(nextEdgeExtension);
                            deltaEdgeExtension = getDeltaGiveVertices(movingVertices, -1);
                            movingVertices.remove(nextEdgeExtension);
                        }
                        // assess gain with next hot tuple. may need to skip a few hot tuples that are already included in hottestVerticesToMove. 
                        int deltaHotTuple = Integer.MIN_VALUE;
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
    
                    // search site to offload (might not find it)
                    // starts with local partitions
                    boolean moved = false;
                    List<Integer> localPartitions = PlanHandler.getPartitionsSite(PlanHandler.getSitePartition(overloadedPartition));
                    for(Integer toPartition : localPartitions){
                        if(!overloadedPartitions.contains(toPartition)){
                            System.out.println("Trying with partition " + toPartition);
                            int movedVertices = tryMovePartition(movingVertices, overloadedPartition, toPartition, new Integer(maxLoadPerPartition), hotVerticesNotMoved);
                            if(movedVertices > 0){
                                numMovedVertices += movedVertices;                            
                                moved = true;
                            }
                        }
                    }
                    // then try with remote partitions
                    if (!moved){
                        for(int toPartition = 0; toPartition < currPartitions; toPartition++){
                            if(!overloadedPartitions.contains(toPartition) && !localPartitions.contains(toPartition)){
                                System.out.println("Trying with partition " + toPartition);
                                int movedVertices = tryMovePartition(movingVertices, overloadedPartition, toPartition, new Integer(maxLoadPerPartition), hotVerticesNotMoved);
                                if(movedVertices > 0){
                                    numMovedVertices += movedVertices;                            
                                }
                            }
                        }
                    }
                } // END while(getLoadPerSite(overloadedPartition) <= maxLoadPerSite)
            }// END for(Integer overloadedPartition : overloadedPartitions)
        } // END if (!overloadedPartitions.isEmpty())
        else{
            // TODO handle underloadded partitions
        }
        return 1;
    }

    /*
     * finds the LOCAL vertex with the highest affinity
     * 
     * ASSUMES that all vertices are on the same partition
     */
    private String getMostAffineExtension(Set<String> vertices){
        int maxAdjacency = -1;
        String res = null;
        int partition = m_vertexPartition.get(vertices.iterator().next());
        for(String vertex : vertices){
            Map<String,Integer> adjacency = m_edges.get(vertex);
            for(Map.Entry<String, Integer> edge : adjacency.entrySet()){
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
     * returns the number of partitions moved
     */
    private int tryMovePartition(Set<String> movingVertices, Integer overloadedPartition, Integer toPartition, Integer maxLoadPerPartition, List<String> hotVerticesNotMoved) {
        int numMovedVertices = 0;
        System.out.println("Trying with partition " + toPartition);
        int deltaFromPartition = getDeltaGiveVertices(movingVertices, toPartition);
        int deltaToPartition = getDeltaReceiveVertices(movingVertices, toPartition);
        // check that I get enough overall gain and the additional load of the receiving site does not make it overloaded
        System.out.println("Deltas from " + deltaFromPartition + " - to " + deltaToPartition);
        if(deltaFromPartition <= Controller.MIN_DELTA_FOR_MOVEMENT 
                && getLoadPerPartition(toPartition) + deltaToPartition < maxLoadPerPartition){   // if gainToSite is negative, the load of the receiving site grows
            System.out.println("Moving to partition " + toPartition);
            System.out.println("Weights before moving " + getLoadPerPartition(overloadedPartition) + " " + getLoadPerPartition(toPartition));
            moveVertices(movingVertices, overloadedPartition, toPartition);
            System.out.println("Weights after moving " + getLoadPerPartition(overloadedPartition) + " " + getLoadPerPartition(toPartition));
            hotVerticesNotMoved.removeAll(movingVertices);
            numMovedVertices = movingVertices.size();
            movingVertices.clear();
        }
        return numMovedVertices;
    }
}
