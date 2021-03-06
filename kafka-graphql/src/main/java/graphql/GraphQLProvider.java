package graphql;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLSchema;
import graphql.schema.TypeResolver;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import javax.annotation.PostConstruct;
import java.net.URL;

import static graphql.schema.idl.TypeRuntimeWiring.newTypeWiring;

@Component
public class GraphQLProvider {


    @Autowired
    TopicDataFetchers topicDataFetchers;
    @Autowired
    ProducerDataFetchers producerDataFetchers;
    @Autowired
    ConsumerDataFetchers consumerDataFetchers;

    private GraphQL graphQL;

    @PostConstruct
    public void init() throws Exception {
        URL url = Resources.getResource("schema.graphql");
        String sdl = Resources.toString(url, Charsets.UTF_8);
        GraphQLSchema graphQLSchema = buildSchema(sdl);
        this.graphQL = GraphQL.newGraphQL(graphQLSchema).build();
    }

    private GraphQLSchema buildSchema(String sdl) throws Exception {
        TypeDefinitionRegistry typeRegistry = new SchemaParser().parse(sdl);
        RuntimeWiring runtimeWiring = buildWiring();
        SchemaGenerator schemaGenerator = new SchemaGenerator();
        return schemaGenerator.makeExecutableSchema(typeRegistry, runtimeWiring);
    }

    private RuntimeWiring buildWiring() throws Exception {

        return RuntimeWiring.newRuntimeWiring()
                .type(newTypeWiring("Query")
                        .dataFetcher("topics", topicDataFetchers.getTopics()))
                .type(newTypeWiring("Query")
                        .dataFetcher("topic", topicDataFetchers.getTopic()))
                .type(newTypeWiring("Query")
                        .dataFetcher("partitionMetaData", topicDataFetchers.getPartitionMetadata()))
                .type(newTypeWiring("Query")
                        .dataFetcher("getOffsets", topicDataFetchers.getOffsets()))
                .type(newTypeWiring("Mutation")
                        .dataFetcher("produceBinary", producerDataFetchers.produceBinary()))
                .type(newTypeWiring("Mutation")
                        .dataFetcher("produceAvro", producerDataFetchers.produceAvro()))
                .type(newTypeWiring("Mutation")
                        .dataFetcher("addConsumer", consumerDataFetchers.addConsumer()))
                .type(newTypeWiring("Mutation")
                        .dataFetcher("subscribe", consumerDataFetchers.subscribe()))
                .type(newTypeWiring("Query")
                        .dataFetcher("getSubscriptions", consumerDataFetchers.getSubscriptions()))
                .type(newTypeWiring("Query")
                        .dataFetcher("consumeBinary", consumerDataFetchers.consumeBinary()))
                .type(newTypeWiring("Query")
                        .dataFetcher("consumeAvro", consumerDataFetchers.consumeAvro()))
                .build();

    }

    @Bean
    public GraphQL graphQL() {
        return graphQL;
    }

}