package com.baidu.kafka.elasticsearch;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.lang.String;
import java.lang.Integer;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("rawtypes")
public class ElasticsearchInsert implements Runnable {
    private int threadNum;
    private KafkaStream stream;
    private int bulkSize;
    private String kafkaMsg;
    private BulkProcessor bulkProcess;
    protected String productName;
    protected TransportClient client;
    protected String indexName;
    protected String typeName;
    protected Map<String,NodeInfo> nodesMap;
    protected String elasticSearchHost = ConfigFile.ES_HOSTS;
    protected Integer elasticSearchPort = ConfigFile.ES_PORT;
    protected String elasticSearchCluster = ConfigFile.ES_CLUSTER_NAME;
    protected static Logger LOG = LoggerFactory.getLogger(ElasticsearchInsert.class);

    public ElasticsearchInsert(KafkaStream stream, String esHost, int threadNum, int bulkSize, String esCluster) {
        this.stream = stream;
        this.threadNum = threadNum;
        this.bulkSize = bulkSize;
        elasticSearchCluster = esCluster;
        elasticSearchHost = esHost;
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("action.bulk.compress", true)
                .put("network.tcp.send_buffer_size", 131072)
                .put("network.tcp.receive_buffer_size", 8192)
                .put("transport.tcp.compress", true)
                .put("cluster.name", elasticSearchCluster)
                .build();

        client = new TransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(elasticSearchHost, elasticSearchPort));

        NodesInfoResponse response = client.admin().cluster().nodesInfo(new NodesInfoRequest().timeout("60")).actionGet();
        nodesMap = response.getNodesMap();
        for(String k: nodesMap.keySet()){
            if(!elasticSearchHost.equals(nodesMap.get(k).getHostname())) {
                client.addTransportAddress(new InetSocketTransportAddress(nodesMap.get(k).getHostname(), elasticSearchPort));
            }
        }

        bulkProcess = BulkProcessor.builder(client, new EsBulkProcessor())
                .setBulkActions(this.bulkSize)
                .setConcurrentRequests(ConfigFile.bulkConcurrent)
                .build();

        LOG.info("init es");
    }

    public boolean insertES(JSONObject jsonObject) {
        String document = null;
        try {
            SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String dateTime = sf.format(System.currentTimeMillis());
            String dateTimeYMD[] = dateTime.split(" ");
            if (jsonObject.has("product")) {
                productName = (String)jsonObject.get("product");
            } else {
                productName = "default";
            }

            indexName = productName + dateTimeYMD[0];
            typeName = "dtrace";
            document = jsonObject.toString();

            bulkProcess.add(new IndexRequest(indexName, typeName).source(document));

            return true;
        } catch(Exception e) {
            LOG.info("Unable to index Document[ " + document + "], Type[" + typeName + "], Index[" + indexName + "]", e);
            return false;
        }
    }

    public void run() {
        LOG.info("Inert ElasticSearch");
        LOG.info("thread num: " + threadNum);
        System.out.println("thread num: " + threadNum);
        while(true) {
            ConsumerIterator<byte[], byte[]> msgStream = stream.iterator();
            int num = 0;

            try {
                while(msgStream.hasNext()) {
                    kafkaMsg = new String(msgStream.next().message(), "UTF-8");
                    JSONObject json = new JSONObject(kafkaMsg);

                    json.put("@version", "1");
                    SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
                    json.put("@timestamp", sf.format(System.currentTimeMillis()));
//                    insertES(json);
                    num += 1;
                }
                Thread.sleep(1000);
            } catch (Exception e) {
                LOG.info("failed to construct index request "+ e.getMessage());
            }
            System.out.println(num);
        }
    }
}
