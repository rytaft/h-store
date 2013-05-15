/**
 * 
 */
package edu.brown.hstore.internal;

import edu.brown.hashing.ReconfigurationPlan.ReconfigurationRange;

/**
 * A message type for a fragment of migration work for a source to push a range
 * to a destination partition
 * 
 * @author aelmore
 */
public class AsyncLivePushRequestMessage extends InternalMessage {
    private ReconfigurationRange<? extends Comparable<?>> pushRange;

    public AsyncLivePushRequestMessage(ReconfigurationRange<? extends Comparable<?>> pushRange) {
        super();
        this.pushRange = pushRange;
    }

    public ReconfigurationRange<? extends Comparable<?>> getPushRange() {
        return pushRange;
    }
    
    
}
