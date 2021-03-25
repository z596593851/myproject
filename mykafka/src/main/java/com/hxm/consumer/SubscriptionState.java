package com.hxm.consumer;

import com.hxm.producer.TopicPartition;

import java.util.*;

public class SubscriptionState {

    private Set<String> subscription;

    //当前订阅主题的消费状态
    private final PartitionStates<TopicPartitionState> assignment;

    public SubscriptionState(){
        this.subscription= Collections.emptySet();
        this.assignment = new PartitionStates<>();
    }

    public void subscribe(Set<String> topics){
        if (!this.subscription.equals(topics)) {
            this.subscription = topics;
        }
    }

    public void position(TopicPartition tp, long offset) {
        assignedState(tp).position(offset);
    }

    public Long position(TopicPartition tp) {
        return assignedState(tp).position;
    }

    public void seek(TopicPartition tp, long offset) {
        assignedState(tp).seek(offset);
    }


    private TopicPartitionState assignedState(TopicPartition tp) {
        TopicPartitionState state = this.assignment.stateValue(tp);
        if (state == null) {
            throw new IllegalStateException("No current assignment for partition " + tp);
        }
        return state;
    }

    public boolean isFetchable(TopicPartition tp) {
        return isAssigned(tp) && assignedState(tp).isFetchable();
    }


    public boolean isAssigned(TopicPartition tp) {
        return assignment.contains(tp);
    }

    public void movePartitionToEnd(TopicPartition tp) {
        assignment.moveToEnd(tp);
    }

    public void assignFromSubscribed(Collection<TopicPartition> assignments) {
//        for (TopicPartition tp : assignments) {
//            if (!this.subscription.contains(tp.topic())) {
//                throw new IllegalArgumentException("Assigned partition " + tp + " for non-subscribed topic.");
//            }
//        }

        // after rebalancing, we always reinitialize the assignment value
        this.assignment.set(partitionToStateMap(assignments));
    }

    private Map<TopicPartition, TopicPartitionState> partitionToStateMap(Collection<TopicPartition> assignments) {
        Map<TopicPartition, TopicPartitionState> map = new HashMap<>(assignments.size());
        for (TopicPartition tp : assignments) {
            map.put(tp, new TopicPartitionState());
        }
        return map;
    }

    private static class TopicPartitionState {
        //下次要从broker获取的消息的offset
        private Long position; // last consumed position
        //最近一次提交的offset
        private OffsetAndMetadata committed;  // last committed position
        private boolean paused;  // whether this partition has been paused by the user

        public TopicPartitionState() {
            this.paused = false;
            this.position = null;
            this.committed = null;
        }

        public boolean hasValidPosition() {
            return position != null;
        }

        private void seek(long offset) {
            this.position = offset;
        }

        private void position(long offset) {
            if (!hasValidPosition()) {
                throw new IllegalStateException("Cannot set a new position without a valid current position");
            }
            this.position = offset;
        }

        private void committed(OffsetAndMetadata offset) {
            this.committed = offset;
        }

        private void pause() {
            this.paused = true;
        }

        private void resume() {
            this.paused = false;
        }

        private boolean isFetchable() {
            return !paused && hasValidPosition();
        }

    }
}
