package graphql;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;

public class AvroTestMain {
    public static void main(String[] args) throws IOException, InterruptedException, RestClientException {
        AvroProducer ap = new AvroProducer();
        while (true){
            ap.publish("test123", null, null, null, null);
            Thread.sleep(5000);
        }

    }
}
