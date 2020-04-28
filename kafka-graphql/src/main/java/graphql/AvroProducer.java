package graphql;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Properties;

@Component
public class AvroProducer {
    Properties props = new Properties();
    {
        props.put("bootstrap.servers", "Localhost:9092");
        props.put("schema.registry.url", "http://localhost:8081");
        props.put("key.serializer", KafkaAvroSerializer.class.getName());
        props.put("value.serializer", KafkaAvroSerializer.class.getName());
    }
    KafkaProducer<GenericRecord, GenericRecord> producer = new KafkaProducer<>(props);

    public void publish(String topic, String key, String value, String keySchema, String valueSchema) throws IOException {

        valueSchema = "{\"type\":\"record\",\"name\":\"test\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]}";
        value = "{\"name\": \"Mike\"}";
        DecoderFactory decoderFactory = new DecoderFactory();
        Schema.Parser schemaParser = new Schema.Parser();
        Schema schema = schemaParser.parse(valueSchema);

        Decoder decoder = decoderFactory.jsonDecoder(schema, value);
        DatumReader<GenericData.Record> reader = new GenericDatumReader<>(schema);
        GenericRecord genericrecord = reader.read(null, decoder);

        System.out.println(genericrecord);
        ProducerRecord<GenericRecord, GenericRecord> record = new ProducerRecord(topic, genericrecord, genericrecord);
        producer.send(record);
    }
}
