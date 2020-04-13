package graphql;

import com.google.common.collect.ImmutableMap;
import graphql.schema.DataFetcher;
import io.confluent.kafkarest.*;
import io.confluent.kafkarest.entities.ConsumerInstanceConfig;
import io.confluent.kafkarest.entities.ConsumerRecord;
import io.confluent.kafkarest.entities.ConsumerSubscriptionRecord;
import io.confluent.kafkarest.entities.ConsumerSubscriptionResponse;
import io.confluent.kafkarest.v2.BinaryKafkaConsumerState;
import io.confluent.kafkarest.v2.KafkaConsumerState;
import io.confluent.rest.RestConfigException;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
public class ConsumerDataFetchers {
    KafkaRestContext ctx;
    {
        try {
            KafkaRestConfig conf = new KafkaRestConfig("config/kafka-rest.properties");
            ctx = new DefaultKafkaRestContext(conf,
                    null,
                    null,
                    null,
                    null);
        } catch (RestConfigException e) {
            e.printStackTrace();
        }
    }

    public DataFetcher addConsumer(){
        return dataFetchingEnvironment -> {
            String group = dataFetchingEnvironment.getArgument("group");
            Map<String, Object> consumer = dataFetchingEnvironment.getArgument("consumer");
            String name = consumer.get("name").toString();
            String format = consumer.get("format").toString();
            String auto_offset_reset = consumer.get("auto_offset_reset").toString();
            String auto_commit_enable = consumer.get("auto_commit_enable").toString();
            Integer fetch_min_bytes = (Integer) consumer.get("fetch_min_bytes");
            Integer request_timeout_ms = (Integer) consumer.get("request_timeout_ms");
            ConsumerInstanceConfig cic = new ConsumerInstanceConfig(name, name, format, auto_offset_reset, auto_commit_enable, fetch_min_bytes, request_timeout_ms);
            String instanceId = ctx.getKafkaConsumerManager().createConsumer(group, cic);
            return ImmutableMap.of("instance_id", instanceId);
        };
    }

    public DataFetcher subscribe(){
        return dataFetchingEnvironment -> {
            String group = dataFetchingEnvironment.getArgument("group");
            String instance = dataFetchingEnvironment.getArgument("instance");
            List<String> topics = dataFetchingEnvironment.getArgument("topics");
            ConsumerSubscriptionRecord subscription = new ConsumerSubscriptionRecord(topics, null);
            ctx.getKafkaConsumerManager().subscribe(group, instance, subscription);
            return null;
        };
    }

    public DataFetcher getSubscriptions(){
        return dataFetchingEnvironment -> {
            String group = dataFetchingEnvironment.getArgument("group");
            String instance = dataFetchingEnvironment.getArgument("instance");
            ConsumerSubscriptionResponse csr = ctx.getKafkaConsumerManager().subscription(group, instance);
            return csr.topics;
        };
    }

    public <ClientKeyT, ClientValueT> DataFetcher consumeBinary(){
        return dataFetchingEnvironment -> {
            String group = dataFetchingEnvironment.getArgument("group");
            String instance = dataFetchingEnvironment.getArgument("instance");
            long timeout = 3000;
            long maxBytes = 300000;

            List<? extends ConsumerRecord<ClientKeyT, ClientValueT>> records;
            return readRecords(group, instance, timeout, maxBytes, BinaryKafkaConsumerState.class);
        };
    }
    private <KafkaKeyT, KafkaValueT, ClientKeyT, ClientValueT> List<Map<String, Object>> readRecords(
            String group, String instance,
            long timeout, long maxBytes,
            Class<? extends KafkaConsumerState<KafkaKeyT, KafkaValueT, ClientKeyT, ClientValueT>>
                    consumerStateType){
        Boolean[] complete = {false};
        List<Map<String, Object>> records2 = new ArrayList<>();

        ctx.getKafkaConsumerManager().readRecords(
                group, instance, consumerStateType, timeout, maxBytes,
                new ConsumerReadCallback<ClientKeyT, ClientValueT>() {
                    @Override
                    public void onCompletion(
                            List<? extends ConsumerRecord<ClientKeyT, ClientValueT>> records,
                            Exception e
                    ) {
                        if (e != null) System.out.println(e);
                        records.stream()
                                .forEach(entry -> records2.add(
                                        ImmutableMap.of("topic", entry.getTopic(),
                                                "key", entry.getKey() == null ? "null" : new String((byte[]) entry.getKey()),
                                                "value", new String((byte[]) entry.getValue()),
                                                "partition", entry.getPartition(),
                                                "offset", entry.getOffset())
                                ));
                        complete[0] = true;
                    }
                }
        );
        while (!complete[0]){}
        System.out.println(records2);
        return records2;
    }


}
