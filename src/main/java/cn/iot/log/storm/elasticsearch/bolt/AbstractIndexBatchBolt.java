package cn.iot.log.storm.elasticsearch.bolt;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.tuple.Tuple;

public abstract class AbstractIndexBatchBolt implements IRichBolt{
    private static final long serialVersionUID = 1L;
    private long emitFrequency;
    
    public AbstractIndexBatchBolt(long emitFrequency, TimeUnit unit) {
        this.emitFrequency = unit.toSeconds(emitFrequency);
    }

    private static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequency);
        return conf;
    }

    @Override
    public void execute(Tuple tuple) {
        if( isTickTuple(tuple) ) {
            executeTickTuple(tuple);
        } else {
            executeTuple(tuple);
        }
    }

    protected abstract void executeTickTuple(Tuple tuple);

    protected abstract void executeTuple(Tuple tuple);

}
