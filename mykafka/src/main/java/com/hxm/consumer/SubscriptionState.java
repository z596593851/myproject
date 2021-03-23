package com.hxm.consumer;

import java.util.Collections;
import java.util.Set;

public class SubscriptionState {

    private Set<String> subscription;

    public SubscriptionState(){
        this.subscription= Collections.emptySet();
    }

    public void subscribe(Set<String> topics){
        if (!this.subscription.equals(topics)) {
            this.subscription = topics;
        }
    }
}
