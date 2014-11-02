package org.qcri.affinityplanner;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class GraphPartitioner extends AffinityGraph {
    public void repartition (int maxLoadPerSite) throws Exception{
        int currSites = getSitesNo();
        int addedSites = 0;

        System.out.println("Calculating site loads");
        
        // detect overloaded sites
        Set<Integer> overloadedSites = new HashSet<Integer>();
        for(int i = 0; i < currSites; i++){
            System.out.println(getLoadPerSite(i));
            if (getLoadPerSite(i) > maxLoadPerSite){
                overloadedSites.add(i);
            }
        }

        // offload each overloaded site
        for(Integer overloadedSite : overloadedSites){
            
            // DEBUG
            System.out.println("#######################");
            System.out.println("offloading site " + overloadedSite);

            // get hottest vertices. the actual length of the array is min(Controller.MAX_MOVED_VERTICES, #tuples held site);
            List<String> hotVerticesNotMoved = getHottestVertices(overloadedSite, Controller.MAX_MOVED_VERTICES_PER_SOURCE_SITE);
            final int actualMaxMovedVertices = hotVerticesNotMoved.size();

            // DEBUG
//            System.out.println("hot vertices:");
//            for (String hotVertex : hotVerticesNotMoved){
//                System.out.println(hotVertex);
//            }

            int currHotVertex = 0;
            int numMovedVertices = 0;
            Set<String> movingVertices = new HashSet<String>();
            while(getLoadPerSite(overloadedSite) > maxLoadPerSite){
                
                System.out.println("--------------------");

                // FIRST add one vertex to the movingVertices set
                // add site if I have over-expanded movingVertices. I expand movingVertices only if all sites reject the previous movingVertices sets. 
                if (numMovedVertices + movingVertices.size() >= actualMaxMovedVertices){
                    System.out.println("Adding a new site");
                    if(addedSites < Controller.MAX_SITES_ADDED_RECONF){
                        addSite();
                        addedSites++;
                        currSites++;
                        currHotVertex = 0;
                        movingVertices.clear();
                    }
                    else{
                        System.out.println("Site " + overloadedSite + " cannot be offloaded. Already moved MAX_MOVED_VERTICES_PER_SOURCE_SITE");
                        return;
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
                    } while (m_vertexSite.get(nextHotVertex) != overloadedSite);
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
                        deltaEdgeExtension = getDeltaGiveVertices(movingVertices);
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
                    deltaHotTuple = getDeltaGiveVertices(movingVertices);
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
                for(int toSite = 0; toSite < currSites; toSite++){
                    if(!overloadedSites.contains(toSite)){
                        System.out.println("Trying with site " + toSite);
                        int deltaFromSite = getDeltaGiveVertices(movingVertices);
                        int deltaToSite = getDeltaReceiveVertices(movingVertices, toSite);
                        // check that I get enough overall gain and the additional load of the receiving site does not make it overloaded
                        System.out.println("Deltas from " + deltaFromSite + " - to " + deltaToSite);
                        if(deltaFromSite <= Controller.MIN_DELTA_FOR_MOVEMENT 
                                && getLoadPerSite(toSite) + deltaToSite < maxLoadPerSite){   // if gainToSite is negative, the load of the receiving site grows
                            System.out.println("Moving to site " + toSite);
                            System.out.println("Weights before moving " + getLoadPerSite(overloadedSite) + " " + getLoadPerSite(toSite));
                            moveVertices(movingVertices, overloadedSite, toSite);
                            System.out.println("Weights after moving " + getLoadPerSite(overloadedSite) + " " + getLoadPerSite(toSite));
                            hotVerticesNotMoved.removeAll(movingVertices);
                            numMovedVertices += movingVertices.size();
                            movingVertices.clear();
                        }
                    }
                }
            } // END while(getLoadPerSite(overloadedSite) <= maxLoadPerSite)
        }// END for(Integer overloadedSite : overloadedSites)
    }

    /*
     * finds the LOCAL vertex with the highest affinity
     * 
     * ASSUMES that all vertices are on the same site
     */
    public String getMostAffineExtension(Set<String> vertices){
        int maxAdjacency = -1;
        String res = null;
        int site = m_vertexSite.get(vertices.iterator().next());
        for(String vertex : vertices){
            Map<String,Integer> adjacency = m_edges.get(vertex);
            for(Map.Entry<String, Integer> edge : adjacency.entrySet()){
                if (edge.getValue() > maxAdjacency
                        && m_vertexSite.get(edge.getKey()) == site
                        && !vertices.contains(edge.getKey())){
                    maxAdjacency = edge.getValue();
                    res = edge.getKey();
                }
            }
        }
        return res;
    }
}
