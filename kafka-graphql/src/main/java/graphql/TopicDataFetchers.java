package graphql;
import com.google.common.collect.ImmutableMap;
import graphql.schema.DataFetcher;
import io.confluent.kafkarest.DefaultKafkaRestContext;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.KafkaRestContext;
import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.PartitionReplica;
import io.confluent.kafkarest.entities.Topic;
import io.confluent.rest.RestConfigException;
import kafka.cluster.Replica;
import org.apache.avro.generic.GenericData;
import org.springframework.stereotype.Component;

import javax.xml.crypto.Data;
import java.util.*;

@Component
public class TopicDataFetchers {

    KafkaRestContext ctx;

    {
        try {
            ctx = new DefaultKafkaRestContext(new KafkaRestConfig("config/kafka-rest.properties"),
                        null,
                        null,
                        null,
                        null);
        } catch (RestConfigException e) {
            e.printStackTrace();
        }
    }



    public DataFetcher getTopic(){
        return dataFetchingEnvironment -> {
            String topicName = dataFetchingEnvironment.getArgument("topic");
            Topic topic = ctx.getAdminClientWrapper().getTopic(topicName);
            if (topic == null) return null;
            List<Partition> partitions = topic.getPartitions();
            Properties properties = topic.getConfigs();
            return ImmutableMap.of("name", topicName,
                    "configs", getConfigs(properties),
                    "partitions", getPartitions(partitions));
        };
    }

    public DataFetcher getTopics(){
        return dataFetchingEnvironment -> {
            List<Map<String, String>> topics = new ArrayList<>();
            for (String s : ctx.getAdminClientWrapper().getTopicNames()) {
                topics.add(ImmutableMap.of("name", s));
            }
            return topics;
        };
    }

    public DataFetcher getPartitionMetadata() {
        return dataFetchingEnvironment -> {
            String topic = dataFetchingEnvironment.getArgument("topic");
            Integer partition = dataFetchingEnvironment.getArgument("partition");
            Partition part = ctx.getAdminClientWrapper().getTopicPartition(topic, partition);
            if (part == null) return null;
            return ImmutableMap.of("id", part.getPartition(),
                    "leader", part.getLeader(),
                    "replicas", getReplicas(part.getReplicas()));
        };
    }

    public DataFetcher getOffsets(){
        return dataFetchingEnvironment -> {
            String topic = dataFetchingEnvironment.getArgument("topic");
            Integer partition = dataFetchingEnvironment.getArgument("partition");
            if (ctx.getAdminClientWrapper().getTopicPartition(topic, partition) == null)
                return null;
            return ImmutableMap.of("beginning_offset", ctx.getKafkaConsumerManager().getBeginningOffset(topic, partition),
                    "end_offset", ctx.getKafkaConsumerManager().getEndOffset(topic, partition));
        };
    }
    

    public List<Map<String, String>> getConfigs(Properties properties){
        List<Map<String, String>> propertyList = new ArrayList<>();
        properties.keySet().stream()
                .forEach(key -> propertyList.add(
                        ImmutableMap.of(
                                "key", key.toString(),
                                "value", properties.getProperty(key.toString()))
                ));
        return propertyList;
    }



    public List<Map<String, Object>> getPartitions(List<Partition> partitions){
        List<Map<String, Object>> partitionList = new ArrayList<>();
        partitions.stream()
                .forEach(partition -> partitionList.add(
                        ImmutableMap.of("id", partition.getPartition(),
                                "leader", partition.getLeader(),
                                "replicas", getReplicas(partition.getReplicas()))
                ));
        return partitionList;
    }

    public List<Map<String, Object>> getReplicas(List<PartitionReplica> replicas){
        List<Map<String, Object>> replicaList = new ArrayList<>();
        replicas.stream()
                .forEach(replica -> replicaList.add(
                        ImmutableMap.of(
                                "broker", replica.getBroker(),
                                "leader", replica.isLeader(),
                                "in_sync", replica.isInSync()
                        )
                ));
        return replicaList;
    }


}
