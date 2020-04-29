package graphql;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
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
import sun.net.www.content.text.Generic;

import java.io.IOException;
import java.util.Properties;

@Component
public class AvroProducer {

    public class KVPair{
        int keyId;
        int valueId;

        public KVPair(int keyId, int valueId){
            this.keyId = keyId;
            this.valueId = valueId;
        }
    }

    SchemaRegistryClient src = new CachedSchemaRegistryClient("http://localhost:8081", 100);
    Properties props = new Properties();
    {
        props.put("bootstrap.servers", "Localhost:9092");
        props.put("schema.registry.url", "http://localhost:8081");
        props.put("key.serializer", KafkaAvroSerializer.class.getName());
        props.put("value.serializer", KafkaAvroSerializer.class.getName());
    }
    KafkaProducer<GenericRecord, GenericRecord> producer = new KafkaProducer<>(props);

    public KVPair publish(String topic, String key, String value, String keySchema, String valueSchema) throws IOException, RestClientException {

        /*
        keySchema = "{\"type\":\"record\",\"name\":\"test\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]}";
        key = "{\"name\": \"Key\"}";
        valueSchema = "{\"type\":\"record\",\"name\":\"test\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]}";
        value = "{\"name\": \"Value\"}";
        */

        Schema.Parser schemaParser = new Schema.Parser();
        Schema parsedValueSchema = schemaParser.parse(valueSchema);
        int valueSchemaId = src.register(topic+"-value", parsedValueSchema);

        Schema.Parser schemaParser1 = new Schema.Parser();
        Schema parsedKeySchema = schemaParser1.parse(keySchema);
        int keySchemaId = src.register(topic+"-key", parsedKeySchema);

        return publish(topic, key, value, keySchemaId, valueSchemaId);

    }


    public KVPair publish(String topic, String key, String value, int keySchemaId, int valueSchemaId) throws IOException, RestClientException {
        Schema keySchema = src.getById(keySchemaId);
        Schema valueSchema = src.getById(valueSchemaId);

        DecoderFactory decoderFactory = new DecoderFactory();
        Decoder decoder = decoderFactory.jsonDecoder(valueSchema, value);
        DatumReader<GenericData.Record> reader = new GenericDatumReader<>(valueSchema);
        GenericRecord genericValueRecord = reader.read(null, decoder);

        DecoderFactory decoderFactory1 = new DecoderFactory();
        Decoder decoder1 = decoderFactory1.jsonDecoder(keySchema, key);
        DatumReader<GenericData.Record> reader1 = new GenericDatumReader<>(keySchema);
        GenericRecord genericKeyRecord = reader1.read(null, decoder1);

        ProducerRecord<GenericRecord, GenericRecord> record = new ProducerRecord(topic, genericKeyRecord, genericValueRecord);
        System.out.println("AVRO: sent: " + genericKeyRecord + " " + genericValueRecord);
        producer.send(record);

        return new KVPair(keySchemaId, valueSchemaId);
    }
}
