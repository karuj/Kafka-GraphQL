/*
 * Run a spring boot server to query the graphql api
 */

package graphql;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class GraphQLMain {
    public static void main(String[] args) throws Exception {

        SpringApplication.run(GraphQLMain.class, args);
    }
}
