/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hxm.protocol;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static com.hxm.protocol.Type.*;

public class Protocol {

    public static final Schema REQUEST_HEADER = new Schema(new Field("api_key", INT16, "The id of the request type."),
                                                           new Field("api_version", INT16, "The version of the API."),
                                                           new Field("correlation_id",
                                                                     INT32,
                                                                     "A user-supplied integer value that will be passed back with the response"),
                                                           new Field("client_id",
                                                                     NULLABLE_STRING,
                                                                     "A user specified identifier for the client making the request.",
                                                                     ""));

    public static final Schema RESPONSE_HEADER = new Schema(new Field("correlation_id",
                                                                      INT32,
                                                                      "The user-supplied value passed in with the request"));

    /* Metadata api */


    /* Produce api */

    public static final Schema TOPIC_PRODUCE_DATA_V0 = new Schema(new Field("topic", STRING),
                                                                  new Field("data", new ArrayOf(new Schema(new Field("partition", INT32),
                                                                                                     new Field("record_set", BYTES)))));

    public static final Schema PRODUCE_REQUEST_V0 = new Schema(new Field("acks",
                                                                   INT16,
                                                                   "The number of acknowledgments the producer requires the leader to have received before considering a request complete. Allowed values: 0 for no acknowledgments, 1 for only the leader and -1 for the full ISR."),
                                                               new Field("timeout", INT32, "The time to await a response in ms."),
                                                               new Field("topic_data", new ArrayOf(TOPIC_PRODUCE_DATA_V0)));

    public static final Schema PRODUCE_RESPONSE_V0 = new Schema(new Field("responses",
                                                                    new ArrayOf(new Schema(new Field("topic", STRING),
                                                                                           new Field("partition_responses",
                                                                                                     new ArrayOf(new Schema(new Field("partition",
                                                                                                                                      INT32),
                                                                                                                            new Field("error_code",
                                                                                                                                      INT16),
                                                                                                                            new Field("base_offset",
                                                                                                                                      INT64))))))));
    /**
     * The body of PRODUCE_REQUEST_V1 is the same as PRODUCE_REQUEST_V0.
     * The version number is bumped up to indicate that the client supports quota throttle time field in the response.
     */
    public static final Schema PRODUCE_REQUEST_V1 = PRODUCE_REQUEST_V0;
    /**
     * The body of PRODUCE_REQUEST_V2 is the same as PRODUCE_REQUEST_V1.
     * The version number is bumped up to indicate that message format V1 is used which has relative offset and
     * timestamp.
     */
    public static final Schema PRODUCE_REQUEST_V2 = PRODUCE_REQUEST_V1;

    public static final Schema PRODUCE_RESPONSE_V1 = new Schema(new Field("responses",
                                                                          new ArrayOf(new Schema(new Field("topic", STRING),
                                                                                                 new Field("partition_responses",
                                                                                                           new ArrayOf(new Schema(new Field("partition",
                                                                                                                                            INT32),
                                                                                                                                  new Field("error_code",
                                                                                                                                            INT16),
                                                                                                                                  new Field("base_offset",
                                                                                                                                            INT64))))))),
                                                                new Field("throttle_time_ms",
                                                                          INT32,
                                                                          "Duration in milliseconds for which the request was throttled" +
                                                                              " due to quota violation. (Zero if the request did not violate any quota.)",
                                                                          0));
    /**
     * PRODUCE_RESPONSE_V2 added a timestamp field in the per partition response status.
     * The timestamp is log append time if the topic is configured to use log append time. Or it is NoTimestamp when create
     * time is used for the topic.
     */
    public static final Schema PRODUCE_RESPONSE_V2 = new Schema(new Field("responses",
                                                                new ArrayOf(new Schema(new Field("topic", STRING),
                                                                                       new Field("partition_responses",
                                                                                       new ArrayOf(new Schema(new Field("partition",
                                                                                                                        INT32),
                                                                                                              new Field("error_code",
                                                                                                                        INT16),
                                                                                                              new Field("base_offset",
                                                                                                                        INT64),
                                                                                                              new Field("timestamp",
                                                                                                                        INT64,
                                                                                                                        "The timestamp returned by broker after appending the messages. " +
                                                                                                                            "If CreateTime is used for the topic, the timestamp will be -1. " +
                                                                                                                            "If LogAppendTime is used for the topic, the timestamp will be " +
                                                                                                                            "the broker local time when the messages are appended."))))))),
                                                                new Field("throttle_time_ms",
                                                                          INT32,
                                                                          "Duration in milliseconds for which the request was throttled" +
                                                                              " due to quota violation. (Zero if the request did not violate any quota.)",
                                                                          0));
    public static final Schema[] PRODUCE_REQUEST = new Schema[] {PRODUCE_REQUEST_V0, PRODUCE_REQUEST_V1, PRODUCE_REQUEST_V2};
    public static final Schema[] PRODUCE_RESPONSE = new Schema[] {PRODUCE_RESPONSE_V0, PRODUCE_RESPONSE_V1, PRODUCE_RESPONSE_V2};

    /* Offset fetch api */

    /* Fetch api */
    public static final Schema FETCH_REQUEST_PARTITION_V0 = new Schema(new Field("partition",
                                                                                 INT32,
                                                                                 "Topic partition id."),
                                                                       new Field("fetch_offset",
                                                                                 INT64,
                                                                                 "Message offset."),
                                                                       new Field("max_bytes",
                                                                                 INT32,
                                                                                 "Maximum bytes to fetch."));

    public static final Schema FETCH_REQUEST_TOPIC_V0 = new Schema(new Field("topic", STRING, "Topic to fetch."),
                                                                   new Field("partitions",
                                                                             new ArrayOf(FETCH_REQUEST_PARTITION_V0),
                                                                             "Partitions to fetch."));

    public static final Schema FETCH_REQUEST_V0 = new Schema(new Field("replica_id",
                                                                       INT32,
                                                                       "Broker id of the follower. For normal consumers, use -1."),
                                                             new Field("max_wait_time",
                                                                       INT32,
                                                                       "Maximum time in ms to wait for the response."),
                                                             new Field("min_bytes",
                                                                       INT32,
                                                                       "Minimum bytes to accumulate in the response."),
                                                             new Field("topics",
                                                                       new ArrayOf(FETCH_REQUEST_TOPIC_V0),
                                                                       "Topics to fetch."));

    // The V1 Fetch Request body is the same as V0.
    // Only the version number is incremented to indicate a newer client
    public static final Schema FETCH_REQUEST_V1 = FETCH_REQUEST_V0;
    // The V2 Fetch Request body is the same as V1.
    // Only the version number is incremented to indicate the client support message format V1 which uses
    // relative offset and has timestamp.
    public static final Schema FETCH_REQUEST_V2 = FETCH_REQUEST_V1;
    // FETCH_REQUEST_V3 added top level max_bytes field - the total size of partition data to accumulate in response.
    // The partition ordering is now relevant - partitions will be processed in order they appear in request.
    public static final Schema FETCH_REQUEST_V3 = new Schema(new Field("replica_id",
                                                                       INT32,
                                                                       "Broker id of the follower. For normal consumers, use -1."),
                                                             new Field("max_wait_time",
                                                                       INT32,
                                                                       "Maximum time in ms to wait for the response."),
                                                             new Field("min_bytes",
                                                                       INT32,
                                                                       "Minimum bytes to accumulate in the response."),
                                                             new Field("max_bytes",
                                                                       INT32,
                                                                       "Maximum bytes to accumulate in the response. Note that this is not an absolute maximum, " +
                                                                       "if the first message in the first non-empty partition of the fetch is larger than this " +
                                                                       "value, the message will still be returned to ensure that progress can be made."),
                                                             new Field("topics",
                                                                       new ArrayOf(FETCH_REQUEST_TOPIC_V0),
                                                                       "Topics to fetch in the order provided."));

    //todo
    public static final Schema FETCH_RESPONSE_PARTITION_V0 = new Schema(new Field("partition",
                                                                                  INT32,
                                                                                  "Topic partition id."),
                                                                        new Field("error_code", INT16),
                                                                        new Field("high_watermark",
                                                                                  INT64,
                                                                                  "Last committed offset."),
                                                                        new Field("record_set", BYTES));

    public static final Schema FETCH_RESPONSE_TOPIC_V0 = new Schema(new Field("topic", STRING),
                                                                    new Field("partition_responses",
                                                                              new ArrayOf(FETCH_RESPONSE_PARTITION_V0)));

    public static final Schema FETCH_RESPONSE_V0 = new Schema(new Field("responses",
                                                                        new ArrayOf(FETCH_RESPONSE_TOPIC_V0)));
    public static final Schema FETCH_RESPONSE_V1 = new Schema(new Field("throttle_time_ms",
                                                                        INT32,
                                                                        "Duration in milliseconds for which the request was throttled" +
                                                                            " due to quota violation. (Zero if the request did not violate any quota.)",
                                                                        0),
                                                              new Field("responses",
                                                                      new ArrayOf(FETCH_RESPONSE_TOPIC_V0)));
    // Even though fetch response v2 has the same protocol as v1, the record set in the response is different. In v1,
    // record set only includes messages of v0 (magic byte 0). In v2, record set can include messages of v0 and v1
    // (magic byte 0 and 1). For details, see ByteBufferMessageSet.
    public static final Schema FETCH_RESPONSE_V2 = FETCH_RESPONSE_V1;
    public static final Schema FETCH_RESPONSE_V3 = FETCH_RESPONSE_V2;

    public static final Schema[] FETCH_REQUEST = new Schema[] {FETCH_REQUEST_V0, FETCH_REQUEST_V1, FETCH_REQUEST_V2, FETCH_REQUEST_V3};
    public static final Schema[] FETCH_RESPONSE = new Schema[] {FETCH_RESPONSE_V0, FETCH_RESPONSE_V1, FETCH_RESPONSE_V2, FETCH_RESPONSE_V3};

    /* Update metadata api */


    /* an array of all requests and responses with all schema versions; a null value in the inner array means that the
     * particular version is not supported */
    public static final Schema[][] REQUESTS = new Schema[ApiKeys.MAX_API_KEY + 1][];
    public static final Schema[][] RESPONSES = new Schema[ApiKeys.MAX_API_KEY + 1][];
    public static final short[] MIN_VERSIONS = new short[ApiKeys.MAX_API_KEY + 1];

    /* the latest version of each api */
    public static final short[] CURR_VERSION = new short[ApiKeys.MAX_API_KEY + 1];

    static {
        REQUESTS[ApiKeys.PRODUCE.id] = PRODUCE_REQUEST;
        REQUESTS[ApiKeys.FETCH.id] = FETCH_REQUEST;


        RESPONSES[ApiKeys.PRODUCE.id] = PRODUCE_RESPONSE;
        RESPONSES[ApiKeys.FETCH.id] = FETCH_RESPONSE;


        /* set the minimum and maximum version of each api */
        for (ApiKeys api : ApiKeys.values()) {
            CURR_VERSION[api.id] = (short) (REQUESTS[api.id].length - 1);
            for (int i = 0; i < REQUESTS[api.id].length; ++i) {
                if (REQUESTS[api.id][i] != null) {
                    MIN_VERSIONS[api.id] = (short) i;
                    break;
                }
            }
        }

        /* sanity check that:
         *   - we have the same number of request and response versions for each api
         *   - we have a consistent set of request and response versions for each api */
        for (ApiKeys api : ApiKeys.values()) {
            if (REQUESTS[api.id].length != RESPONSES[api.id].length) {
                throw new IllegalStateException(REQUESTS[api.id].length + " request versions for api " + api.name
                        + " but " + RESPONSES[api.id].length + " response versions.");
            }

            for (int i = 0; i < REQUESTS[api.id].length; ++i) {
                if ((REQUESTS[api.id][i] == null && RESPONSES[api.id][i] != null) ||
                        (REQUESTS[api.id][i] != null && RESPONSES[api.id][i] == null)) {
                    throw new IllegalStateException("Request and response for version " + i + " of API "
                            + api.id + " are defined inconsistently. One is null while the other is not null.");
                }
            }
        }
    }

    private static String indentString(int size) {
        StringBuilder b = new StringBuilder(size);
        for (int i = 0; i < size; i++) {
            b.append(" ");
        }
        return b.toString();
    }

    private static void schemaToBnfHtml(Schema schema, StringBuilder b, int indentSize) {
        final String indentStr = indentString(indentSize);
        final Map<String, Type> subTypes = new LinkedHashMap<>();

        // Top level fields
        for (Field field: schema.fields()) {
            if (field.type instanceof ArrayOf) {
                b.append("[");
                b.append(field.name);
                b.append("] ");
                Type innerType = ((ArrayOf) field.type).type();
                if (!subTypes.containsKey(field.name)) {
                    subTypes.put(field.name, innerType);
                }
            } else if (field.type instanceof Schema) {
                b.append(field.name);
                b.append(" ");
                if (!subTypes.containsKey(field.name)) {
                    subTypes.put(field.name, field.type);
                }
            } else {
                b.append(field.name);
                b.append(" ");
                if (!subTypes.containsKey(field.name)) {
                    subTypes.put(field.name, field.type);
                }
            }
        }
        b.append("\n");

        // Sub Types/Schemas
        for (Map.Entry<String, Type> entry: subTypes.entrySet()) {
            if (entry.getValue() instanceof Schema) {
                // Complex Schema Type
                b.append(indentStr);
                b.append(entry.getKey());
                b.append(" => ");
                schemaToBnfHtml((Schema) entry.getValue(), b, indentSize + 2);
            } else {
                // Standard Field Type
                b.append(indentStr);
                b.append(entry.getKey());
                b.append(" => ");
                b.append(entry.getValue());
                b.append("\n");
            }
        }
    }

    private static void populateSchemaFields(Schema schema, Set<Field> fields) {
        for (Field field: schema.fields()) {
            fields.add(field);
            if (field.type instanceof ArrayOf) {
                Type innerType = ((ArrayOf) field.type).type();
                if (innerType instanceof Schema) {
                    populateSchemaFields((Schema) innerType, fields);
                }
            } else if (field.type instanceof Schema) {
                populateSchemaFields((Schema) field.type, fields);
            }
        }
    }

    private static void schemaToFieldTableHtml(Schema schema, StringBuilder b) {
        Set<Field> fields = new LinkedHashSet<>();
        populateSchemaFields(schema, fields);

        b.append("<table class=\"data-table\"><tbody>\n");
        b.append("<tr>");
        b.append("<th>Field</th>\n");
        b.append("<th>Description</th>\n");
        b.append("</tr>");
        for (Field field : fields) {
            b.append("<tr>\n");
            b.append("<td>");
            b.append(field.name);
            b.append("</td>");
            b.append("<td>");
            b.append(field.doc);
            b.append("</td>");
            b.append("</tr>\n");
        }
        b.append("</table>\n");
    }

    public static String toHtml() {
        final StringBuilder b = new StringBuilder();
        b.append("<h5>Headers:</h5>\n");

        b.append("<pre>");
        b.append("Request Header => ");
        schemaToBnfHtml(REQUEST_HEADER, b, 2);
        b.append("</pre>\n");
        schemaToFieldTableHtml(REQUEST_HEADER, b);

        b.append("<pre>");
        b.append("Response Header => ");
        schemaToBnfHtml(RESPONSE_HEADER, b, 2);
        b.append("</pre>\n");
        schemaToFieldTableHtml(RESPONSE_HEADER, b);

        for (ApiKeys key : ApiKeys.values()) {
            // Key
            b.append("<h5>");
            b.append(key.name);
            b.append(" API (Key: ");
            b.append(key.id);
            b.append("):</h5>\n\n");
            // Requests
            b.append("<b>Requests:</b><br>\n");
            Schema[] requests = REQUESTS[key.id];
            for (int i = 0; i < requests.length; i++) {
                Schema schema = requests[i];
                // Schema
                if (schema != null) {
                    b.append("<p>");
                    // Version header
                    b.append("<pre>");
                    b.append(key.name);
                    b.append(" Request (Version: ");
                    b.append(i);
                    b.append(") => ");
                    schemaToBnfHtml(requests[i], b, 2);
                    b.append("</pre>");
                    schemaToFieldTableHtml(requests[i], b);
                }
                b.append("</p>\n");
            }

            // Responses
            b.append("<b>Responses:</b><br>\n");
            Schema[] responses = RESPONSES[key.id];
            for (int i = 0; i < responses.length; i++) {
                Schema schema = responses[i];
                // Schema
                if (schema != null) {
                    b.append("<p>");
                    // Version header
                    b.append("<pre>");
                    b.append(key.name);
                    b.append(" Response (Version: ");
                    b.append(i);
                    b.append(") => ");
                    schemaToBnfHtml(responses[i], b, 2);
                    b.append("</pre>");
                    schemaToFieldTableHtml(responses[i], b);
                }
                b.append("</p>\n");
            }
        }

        return b.toString();
    }

    public static void main(String[] args) {
        System.out.println(toHtml());
    }

}
