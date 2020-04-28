package graphql;


import com.google.common.collect.ImmutableMap;
import graphql.schema.DataFetcher;
import io.confluent.kafkarest.*;
import io.confluent.kafkarest.entities.*;
import io.confluent.rest.RestConfigException;
import org.springframework.stereotype.Component;

import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Vector;

@Component
public class ProducerDataFetchers {

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

    public DataFetcher produceBinary() {
        return dataFetchingEnvironment -> {
            String topic = dataFetchingEnvironment.getArgument("topic");
            List<Map<String, Object>> records = dataFetchingEnvironment.getArgument("records");
            TopicProduceRequest<BinaryTopicProduceRecord> recordsBinary = new TopicProduceRequest<>();
            List<BinaryTopicProduceRecord> recordsPre = new ArrayList<>();
            for (Map<String, Object> entry : records){
                Object key = entry.get("key");
                Object value = entry.get("value");
                Object partition = entry.get("partition");
                if (key == null){
                    if (partition == null){
                        recordsPre.add(new BinaryTopicProduceRecord(value.toString().getBytes()));
                    } else recordsPre.add(new BinaryTopicProduceRecord(value.toString().getBytes(), (Integer) partition));
                }
                else if (partition == null) recordsPre.add(new BinaryTopicProduceRecord(key.toString().getBytes(), value.toString().getBytes()));
                else recordsPre.add(new BinaryTopicProduceRecord(key.toString().getBytes(), value.toString().getBytes(), (Integer) partition));
            }
            recordsBinary.setRecords(recordsPre);

            List<PartitionOffset> offsets = new Vector<PartitionOffset>();
            final Boolean[] done = {false};
            ctx.getProducerPool().produce(
                    topic, null, EmbeddedFormat.BINARY, recordsBinary, recordsBinary.getRecords(),
                    new ProducerPool.ProduceRequestCallback() {
                        public void onCompletion(
                                Integer keySchemaId, Integer valueSchemaId,
                                List<RecordMetadataOrException> results
                        ) {

                            ProduceResponse response = new ProduceResponse();
                            //List<PartitionOffset> offsets = new Vector<PartitionOffset>();
                            for (RecordMetadataOrException result : results) {
                                if (result.getException() != null) {
                                    int errorCode =
                                            Utils.errorCodeFromProducerException(result.getException());
                                    String errorMessage = result.getException().getMessage();
                                    offsets.add(new PartitionOffset(null, null, errorCode, errorMessage));
                                } else {
                                    offsets.add(new PartitionOffset(result.getRecordMetadata().partition(),
                                            result.getRecordMetadata().offset(),
                                            null, null
                                    ));
                                }
                            }
                            response.setOffsets(offsets);
                            response.setKeySchemaId(keySchemaId);
                            response.setValueSchemaId(valueSchemaId);
                            done[0] = true;
                        }
                    });
            List<Map<String, Integer>> output = new ArrayList<>();


            while (!done[0]){} // DIRTY HACK? wait for completion of callback

            offsets.stream()
                    .forEach(offset -> output.add(ImmutableMap.of("partition", offset.getPartition(),
                            "offset", offset.getOffset().intValue())));
            return output;
        };
    }

    public DataFetcher produceAvro() {
        return dataFetchingEnvironment -> {
            String topic = dataFetchingEnvironment.getArgument("topic");
            List<Map<String, Object>> records = dataFetchingEnvironment.getArgument("records");
            for (Map<String, Object> record : records) {
                Integer key_schema_id = (Integer) record.get("key_schema_id");
                Integer value_schema_id = (Integer) record.get("value_schema_id");
                String key_schema = (String) record.get("key_schema");
                String value_schema = (String) record.get("key_schema");
                String key = (String) record.get("key");
                String value = (String) record.get("value");
                if (key_schema_id == null){
                    // schema = key_schema
                } else {
                    // schema = registry.get(key_schema_id)
                }
                if (value_schema_id == null) {
                    // schema = value_schema
                } else {
                    // schema = registry.get(value_schema)
                }
            }
            return null;
        };
    }

}
