package cn.iot.log.storm.elasticsearch.common;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.LocalTransportAddress;

import com.google.common.base.Preconditions;

public interface ClientFactory<T extends Client> extends Serializable {

    public static final int DEFAULT_PORT = 9300;
    public static final String NAME = "storm.elasticsearch.cluster.name";
    public static final String HOSTS = "storm.elasticsearch.hosts";
    public static final char PORT_SEPARATOR = ':';
    public static final char HOST_SEPARATOR = ',';

    T makeClient(Map conf);

    public static class TransportClientFactory implements ClientFactory<TransportClient> {
        private static final long serialVersionUID = 1L;
        private Map<String, String> settings;

        public TransportClientFactory() {}

        public TransportClientFactory(Map<String, String> settings) {
            this.settings = settings;
        }

        @Override
        public TransportClient makeClient(Map conf) {
            String clusterHosts = (String) conf.get(HOSTS);
            String clusterName = (String) conf.get(NAME);
            Preconditions.checkNotNull(clusterHosts,
                    "no setting found for Transport Client, make sure that you set property " + HOSTS);
            TransportClient client = TransportClient.builder().settings(buildSettings(clusterName)).build();
            for (String hostAndPort : StringUtils.split(clusterHosts, HOST_SEPARATOR)) {
                int portPos = hostAndPort.indexOf(PORT_SEPARATOR);
                boolean noPortDefined = portPos == -1;
                int port = (noPortDefined) ? DEFAULT_PORT
                        : Integer.parseInt(hostAndPort.substring(portPos + 1, hostAndPort.length()));
                String host = (noPortDefined) ? hostAndPort : hostAndPort.substring(0, portPos);
                client.addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress(host, port)));
            }
            return client;
        }

        private Settings buildSettings(String clusterName) {
            Settings.Builder sb = Settings.settingsBuilder();
            if (StringUtils.isNotEmpty(clusterName)) {
                sb.put("cluster.name", clusterName);
            }
            if (settings != null) {
                sb.put(settings);
            }
            return sb.build();
        }
    }

    public static class LocalClientFactory implements ClientFactory<TransportClient> {

        private static final long serialVersionUID = 1L;
        private Map<String, String> settings;

        public LocalClientFactory() {}

        public LocalClientFactory(Map<String, String> settings) {
            this.settings = settings;
        }

        @Override
        public TransportClient makeClient(Map conf) {
            TransportClient client = TransportClient.builder().settings(buildSettings()).build();
            client.addTransportAddress(new LocalTransportAddress("1"));
            return client;
        }

        protected Settings buildSettings() {
            Settings.Builder sb = Settings.settingsBuilder().put("node.local", "true");
            if (settings != null) {
                sb.put(settings);
            }
            return sb.build();
        }
    }
}
