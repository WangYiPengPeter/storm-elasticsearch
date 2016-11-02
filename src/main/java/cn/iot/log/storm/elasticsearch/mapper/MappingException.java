package cn.iot.log.storm.elasticsearch.mapper;


public class MappingException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public MappingException(String message, Throwable source) {
        super(message, source);
    }
}
