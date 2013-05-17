package edu.brown.hstore.internal;

import edu.brown.hashing.ReconfigurationPlan.ReconfigurationRange;

public class AsyncLivePullRequestMessage extends InternalMessage {
    private ReconfigurationRange<? extends Comparable<?>> pullRange;

    public AsyncLivePullRequestMessage(ReconfigurationRange<? extends Comparable<?>> pullRange) {
        super();
        this.pullRange = pullRange;
    }

    public ReconfigurationRange<? extends Comparable<?>> getPullRange() {
        return pullRange;
    }
    
    
}
