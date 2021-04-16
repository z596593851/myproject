/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.hxm.client.common.requests;


import com.hxm.client.common.protocol.ApiKeys;
import com.hxm.client.common.protocol.ProtoUtils;
import com.hxm.client.common.protocol.Schema;
import com.hxm.client.common.protocol.Struct;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class MetadataRequest extends AbstractRequest {

    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentRequestSchema(ApiKeys.METADATA.id);
    private static final String TOPICS_KEY_NAME = "topics";

    private static final MetadataRequest ALL_TOPICS_REQUEST = new MetadataRequest((List<String>) null); // Unusual cast to work around constructor ambiguity

    private final List<String> topics;

    public static MetadataRequest allTopics() {
        return ALL_TOPICS_REQUEST;
    }

    /**
     * In v0 null is not allowed and and empty list indicates requesting all topics.
     * In v1 null indicates requesting all topics, and an empty list indicates requesting no topics.
     */
    public MetadataRequest(List<String> topics) {
        super(new Struct(CURRENT_SCHEMA));
        if (topics == null) {
            struct.set(TOPICS_KEY_NAME, null);
        } else {
            struct.set(TOPICS_KEY_NAME, topics.toArray());
        }
        this.topics = topics;
    }

    public MetadataRequest(Struct struct) {
        super(struct);
        Object[] topicArray = struct.getArray(TOPICS_KEY_NAME);
        if (topicArray != null) {
            topics = new ArrayList<>();
            for (Object topicObj: topicArray) {
                topics.add((String) topicObj);
            }
        } else {
            topics = null;
        }
    }

    @Override
    public AbstractRequestResponse getErrorResponse(int versionId, Throwable e) {
        return null;
    }


    public boolean isAllTopics() {
        return topics == null;
    }

    public List<String> topics() {
        return topics;
    }

    public static MetadataRequest parse(ByteBuffer buffer, int versionId) {
        return new MetadataRequest(ProtoUtils.parseRequest(ApiKeys.METADATA.id, versionId, buffer));
    }

    public static MetadataRequest parse(ByteBuffer buffer) {
        return new MetadataRequest(CURRENT_SCHEMA.read(buffer));
    }
}
