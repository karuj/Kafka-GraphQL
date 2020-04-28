package graphql;

import java.io.IOException;

public class testingOK {
    public static void main(String[] args) throws IOException, InterruptedException {
        AvroProducer ap = new AvroProducer();
        while (true){
            ap.publish("test123", null, null, null, null);
            Thread.sleep(2000);
        }

    }
}
