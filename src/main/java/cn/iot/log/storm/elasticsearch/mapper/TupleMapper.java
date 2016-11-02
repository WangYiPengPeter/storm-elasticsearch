package cn.iot.log.storm.elasticsearch.mapper;

import java.io.Serializable;

import org.apache.storm.tuple.Tuple;

public interface TupleMapper<T> extends Serializable {
   T map(Tuple input);
}