package com.hxm.client.common.protocol;

/**
 * Identifiers for all the Kafka APIs
 */
public enum ApiKeys {
    PRODUCE(0, "Produce"),
    FETCH(1, "Fetch"),
    METADATA(3, "Metadata");
    private static final ApiKeys[] ID_TO_TYPE;
    private static final int MIN_API_KEY = 0;
    public static final int MAX_API_KEY;

    static {
        int maxKey = -1;
        for (ApiKeys key : ApiKeys.values()) {
            maxKey = Math.max(maxKey, key.id);
        }
        ApiKeys[] idToType = new ApiKeys[maxKey + 1];
        for (ApiKeys key : ApiKeys.values()) {
            idToType[key.id] = key;
        }
        ID_TO_TYPE = idToType;
        MAX_API_KEY = maxKey;
    }

    /** the permanent and immutable id of an API--this can't change ever */
    public final short id;

    /** an english description of the api--this is for debugging and can change */
    public final String name;

    private ApiKeys(int id, String name) {
        this.id = (short) id;
        this.name = name;
    }

    public static ApiKeys forId(int id) {
        if (id < MIN_API_KEY || id > MAX_API_KEY) {
            throw new IllegalArgumentException(String.format("Unexpected ApiKeys id `%s`, it should be between `%s` " +
                    "and `%s` (inclusive)", id, MIN_API_KEY, MAX_API_KEY));
        }
        return ID_TO_TYPE[id];
    }

    private static String toHtml() {
        final StringBuilder b = new StringBuilder();
        b.append("<table class=\"data-table\"><tbody>\n");
        b.append("<tr>");
        b.append("<th>Name</th>\n");
        b.append("<th>Key</th>\n");
        b.append("</tr>");
        for (ApiKeys key : ApiKeys.values()) {
            b.append("<tr>\n");
            b.append("<td>");
            b.append(key.name);
            b.append("</td>");
            b.append("<td>");
            b.append(key.id);
            b.append("</td>");
            b.append("</tr>\n");
        }
        b.append("</table>\n");
        return b.toString();
    }

    public static void main(String[] args) {
        System.out.println(toHtml());
    }

}
