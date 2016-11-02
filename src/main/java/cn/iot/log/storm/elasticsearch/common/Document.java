package cn.iot.log.storm.elasticsearch.common;

import java.io.Serializable;

public class Document<T> implements Serializable {
    private static final long serialVersionUID = 1L;
    private String name;
    private String type;
    private T source;
    private String id;
    private String parentId;

    public Document(String name, String type, T source) {
        this(name, type, source, null, null);
    }

    public Document(String name, String type, T source, String id) {
        this(name, type, source, id, null);
    }

    public Document(String name, String type, T source, String id, String parentId) {
        this.name = name;
        this.type = type;
        this.source = source;
        this.id = id;
        this.parentId = parentId;
    }

    public String getName() {
        return this.name;
    }

    public String getType() {
        return this.type;
    }

    public T getSource() {
        return this.source;
    }

    public String getId() {
        return this.id;
    }

    public String getParentId() {
        return this.parentId;
    }
}
