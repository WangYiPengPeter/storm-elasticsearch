package cn.iot.log.storm.elasticsearch.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.iot.log.storm.elasticsearch.common.ClientFactory;
import cn.iot.log.storm.elasticsearch.common.Document;
import cn.iot.log.storm.elasticsearch.mapper.TupleMapper;

public class IndexBatchBolt<T> extends AbstractIndexBatchBolt {
    private static final long serialVersionUID = 1L;

    private static final Logger logger = LoggerFactory.getLogger(IndexBatchBolt.class);

    public static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.SECONDS;

    public static final long DEFAULT_EMIT_FREQUENCY = 10;

    private static final int QUEUE_MAX_SIZE = 1000;

    private int batchSize = QUEUE_MAX_SIZE;

    private OutputCollector outputCollector;

    private Client client;

    private ClientFactory<Client> clientFactory;

    private LinkedBlockingQueue<Tuple> queue;

    private TupleMapper<Document<T>> mapper;

    public IndexBatchBolt(long emitFrequency, TimeUnit unit) {
        super(emitFrequency, unit);
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;
        this.client = clientFactory.makeClient(stormConf);
        this.queue = new LinkedBlockingQueue<Tuple>(batchSize);
    }

    @Override
    public void cleanup() {
        if (this.client != null) {
            this.client.close();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    protected void executeTickTuple(Tuple tuple) {
        bulkUpdateIndexes();
        outputCollector.ack(tuple);
    }

    @Override
    protected void executeTuple(Tuple tuple) {
        if (!queue.offer(tuple)) {
            bulkUpdateIndexes();
            queue.add(tuple);
        }
    }

    protected void bulkUpdateIndexes() {
        List<Tuple> inputs = new ArrayList<>(queue.size());
        queue.drainTo(inputs);
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for (Tuple input : inputs) {
            Document<T> doc = mapper.map(input);
            IndexRequestBuilder request =
                    client.prepareIndex(doc.getName(), doc.getType(), doc.getId()).setSource((Map) doc.getSource());
            if (doc.getParentId() != null) {
                request.setParent(doc.getParentId());
            }
            bulkRequest.add(request);
        }
        try {
            if (bulkRequest.numberOfActions() > 0) {
                BulkResponse bulkItemResponses = bulkRequest.execute().actionGet();
                if (bulkItemResponses.hasFailures()) {
                    BulkItemResponse[] items = bulkItemResponses.getItems();
                    for (int i = 0; i < items.length; i++) {
                        ackOrFail(items[i], inputs.get(i));
                    }
                } else {
                    ackAll(inputs);
                }
            }
        } catch (ElasticsearchException e) {
            logger.error("Unable to process bulk request, {} tuples are in failure", inputs.size(), e);
            outputCollector.reportError(e.getRootCause());
            failAll(inputs);
        }
    }

    private void ackOrFail(BulkItemResponse item, Tuple tuple) {
        if (item.isFailed()) {
            logger.error("Failed to process tuple :{} ", mapper.map(tuple));
            outputCollector.fail(tuple);
        } else {
            outputCollector.ack(tuple);
        }
    }

    protected void ackAll(List<Tuple> inputs) {
        for (Tuple t : inputs) {
            outputCollector.ack(t);
        }
    }

    protected void failAll(List<Tuple> inputs) {
        for (Tuple t : inputs) {
            outputCollector.fail(t);
        }
    }

    public ClientFactory<Client> getClientFactory() {
        return clientFactory;
    }

    public void setClientFactory(ClientFactory<Client> clientFactory) {
        this.clientFactory = clientFactory;
    }

    public TupleMapper<Document<T>> getMapper() {
        return mapper;
    }

    public void setMapper(TupleMapper<Document<T>> mapper) {
        this.mapper = mapper;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

}
