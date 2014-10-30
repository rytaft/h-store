package org.qcri.affinityplanner;

import java.util.HashSet;
import java.util.Set;

public class GraphPartitioner extends AffinityGraph {
    public void repartition (int maxLoadPerSite) throws Exception{
        int currSites = getSitesNo();
        int addedSites = 0;
        
        // detect overloaded sites
        Set<Integer> overloadedSites = new HashSet<Integer>();
        for(int i = 0; i < currSites; i++){
            if (getLoadPerSite(i) > maxLoadPerSite){
                overloadedSites.add(i);
            }
        }
        
        // offload each of them
        for(Integer overloadedSite : overloadedSites){
            
            System.out.println("offloading site " + overloadedSite);
            
            // start by moving hottest vertex
            Set<String> movedVertices = new HashSet<String>();
            String[] hottestVertices = getHottestVertices(overloadedSite, Controller.MAX_MOVED_VERTICES);
            // will add at most one new site
            
            // DEBUG
            System.out.println("hot vertices:");
            for (String hotVertex : hottestVertices){
                System.out.println(hotVertex);
            }
            
            int currHotVertex = 0;
            movedVertices.add(hottestVertices[currHotVertex]);
            // iterate and expand set of moved vertices as needed
            while(movedVertices.size() < Controller.MAX_MOVED_VERTICES){
                boolean moved = false;
                
                // DEBUG
                System.out.println("Moving vertices: ");
                for (String vertex : movedVertices){
                    System.out.println(vertex);
                }
                
                // find site to offload
                for(int toSite = 0; toSite < currSites; toSite++){
                    if(!overloadedSites.contains(toSite)){
                        int gainFromSite = getRemoveVerticesGain(movedVertices);
                        int gainToSite = getAddVerticesGain(movedVertices, toSite);
                        // if gainToSite is negative, the load of the receiving site grows
                        if(gainFromSite + gainToSite >= Controller.MIN_GAIN_FOR_MOVEMENT 
                                && getLoadPerSite(toSite) - gainToSite < maxLoadPerSite){
                            System.out.println("Moving to site " + toSite);
                            moveVertices(movedVertices, overloadedSite, toSite);
                            moved = true;
                        }
                    }
                }
                if(getLoadPerSite(overloadedSite) <= maxLoadPerSite){
                    break;
                }
                else{
                    // site is still overloaded, find new tuple to move
                    if (moved){
                        // new moved set. get next hot vertex
                        movedVertices.clear();
                        String nextHotVertex = null;
                        do{
                            currHotVertex++;
                            nextHotVertex = hottestVertices[currHotVertex];
                        } while (m_vertexSite.get(nextHotVertex) != overloadedSite);
                        if (nextHotVertex == null){
                            System.out.println("Site " + overloadedSite + " cannot be offloaded");
                            throw new Exception();
                        }
                        else{
                            movedVertices.add(nextHotVertex);
                        }
                    }
                    else{
                        // extend current moved set
                        // assess gain with extension
                        int gainWithExtension = 0;                        
                        String extension = getMostAffineExtension(movedVertices);
                        if(extension != null){
                            movedVertices.add(extension);
                            gainWithExtension = getRemoveVerticesGain(movedVertices);
                            movedVertices.remove(extension);
                        }
                        // assess gain with next hot tuple
                        int gainWithHotTuple = 0;
                        String hotVertex = null;
                        if(currHotVertex + 1 < hottestVertices.length){
                            hotVertex = hottestVertices[currHotVertex+1];
                            movedVertices.add(hotVertex);
                            gainWithHotTuple = getRemoveVerticesGain(movedVertices);
                            movedVertices.remove(hotVertex);
                        }
                        // pick best available choice
                        if (extension == null && hotVertex == null){
                            // no more extensions or new hot tuples available. we need to add a new site (if we have not already done it) and restart
                            if(addedSites < Controller.MAX_SITES_ADDED_RECONF){
                                System.out.println("Adding a new site");
                                addedSites++;
                                currSites++;
                                currHotVertex = 0;
                                movedVertices.clear();
                                movedVertices.add(hotVertex);
                            }
                            else{
                                System.out.println("Site " + overloadedSite + " cannot be offloaded");
                                throw new Exception();
                            }
                        }
                        else if (extension != null && hotVertex == null){
                            movedVertices.add(extension);                       
                        }
                        else if (extension == null && hotVertex != null){
                            movedVertices.add(hotVertex);
                            currHotVertex++;
                        }
                        else if (gainWithExtension > gainWithHotTuple){
                            movedVertices.add(extension);
                        }
                        else{
                            movedVertices.add(hotVertex);
                            currHotVertex++;
                        }
                    } // END if(!moved)
                } // END if(getLoadPerSite(overloadedSite) > maxLoadPerSite)
            } // END while(movedVertices.size() < Controller.MAX_MOVED_VERTICES)
            if (getLoadPerSite(overloadedSite) > maxLoadPerSite){
                // there is nothing more we can do...
                System.out.println("Site " + overloadedSite + " cannot be offloaded");
                throw new Exception();
            }
        }
    }
}
