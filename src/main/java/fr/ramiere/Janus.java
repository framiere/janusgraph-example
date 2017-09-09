package fr.ramiere;

import com.github.javafaker.Faker;
import lombok.extern.slf4j.Slf4j;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.Mapping;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.apache.tinkerpop.gremlin.process.traversal.Order.decr;
import static org.apache.tinkerpop.gremlin.process.traversal.P.eq;


@Slf4j
public class Janus {
    /**
     * The configuration file path relative to the execute path of this code.
     * It is assumed you will run within the distributions folder.
     */
    private static final String CONFIG_FILE = "src/conf/janusgraph.properties";
    private static final String BACKING_INDEX = "search";

    private static final String USER = "user";
    private static final String USER_NAME = "userName";

    private static final String STATUS_UPDATE = "statusUpdate";
    private static final String CONTENT = "content";

    private static final String CREATED_AT = "createdAt";

    private static final String POSTS = "posts";
    private static final String FOLLOWS = "follows";

    private static final LocalDate LAST_WEEK = LocalDate.now().minus(1, ChronoUnit.WEEKS);
    public static final String KNOWN_USERNAME = "one";

    private final JanusGraph graph;
    private final JanusGraphManagement mgt;

    private static final Faker faker = new Faker();

    public static void main(String args[]) {
        new Janus(CONFIG_FILE);
    }

    public Janus(String configFile) {
        graph = JanusGraphFactory.open(configFile);
        mgt = graph.openManagement();

        if (mgt.getGraphIndex(indexName(USER, USER_NAME, String.class)) == null) {
            buildSchema();
            populate();
        }


        GraphTraversalSource g = graph.traversal();
//        print(g.V().hasLabel(USER).has(USER_NAME, eq(KNOWN_USERNAME)).in(FOLLOWS));
//        print(g.V().hasLabel(USER).has(USER_NAME, eq(KNOWN_USERNAME)));
//        print(g.V().hasLabel(USER).has(USER_NAME, eq(KNOWN_USERNAME)).out(POSTS));
//        print(g.V().hasLabel(USER).has(USER_NAME, eq(KNOWN_USERNAME)).aggregate("ignore").in(FOLLOWS));
        print(g.V().hasLabel(USER).has(USER_NAME, eq(KNOWN_USERNAME))
                .aggregate("users")
                .out(FOLLOWS)
                .aggregate("users")
                .cap("users")
                .unfold()
                .outE(POSTS)
                .order().by(CREATED_AT, decr)
                .limit(10)
                .inV());

        close();
    }

    public void print(GraphTraversal<Vertex, Vertex> traversal) {
        GraphTraversal<Vertex, Map<String, Object>> valueMap = traversal.valueMap(true);
        int count = 0;

        for (GraphTraversal<Vertex, Map<String, Object>> it = valueMap; it.hasNext(); ) {
            Map<String, Object> item = it.next();
            log.info(" {}: {} ", count++, item.toString());
        }
    }


    private void populate() {
        List<Vertex> users = IntStream.range(0, 1000).mapToObj(i -> addUser(faker.name().lastName())).collect(toList());
        Vertex one = addUser("one");
        users.add(one);
        users.add(addUser("two"));
        users.add(addUser("three"));
        users.add(addUser("four"));
        int i = 0;
        for (Vertex user : users) {
            System.out.println((i++) + " Building " + user.property(USER_NAME));
            IntStream.range(0, 20).forEach(e -> addUpdateStatus(user));

            user.addEdge(FOLLOWS, users.get(faker.number().numberBetween(0, users.size())), CREATED_AT, getTimestamp());
            user.addEdge(FOLLOWS, one, CREATED_AT, getTimestamp());
        }
    }

    private void buildSchema() {
        createUserSchema();
        createStatusUpdateSchema();
        createEdgeSchema();
    }

    private void addUpdateStatus(Vertex user) {
        Vertex statusUpdate = graph.addVertex(STATUS_UPDATE);
        statusUpdate.property(CONTENT, faker.lorem().sentence());
        user.addEdge(POSTS, statusUpdate, CREATED_AT, getTimestamp());
    }

    private Long getTimestamp() {
        return faker.date().between(asDate(LAST_WEEK), new Date()).getTime();
    }

    private static Date asDate(LocalDate localDate) {
        return Date.from(localDate.atStartOfDay().atZone(ZoneId.systemDefault()).toInstant());
    }

    private Vertex addUser(String name) {
        Vertex vertex = graph.addVertex(USER);
        vertex.property(USER_NAME, name);
        return vertex;
    }

    private void createUserSchema() {
        log.info("Create {} schema", USER);
        mgt.buildIndex(indexName(USER, USER_NAME, String.class), Vertex.class)
                .addKey(mgt.makePropertyKey(USER_NAME).dataType(String.class).make(), Mapping.STRING.asParameter())
                .indexOnly(mgt.makeVertexLabel(USER).make())
                .buildMixedIndex(BACKING_INDEX);
    }

    private void createStatusUpdateSchema() {
        log.info("Create {} schema", STATUS_UPDATE);
        mgt.buildIndex(indexName(STATUS_UPDATE, CONTENT, String.class), Vertex.class)
                .addKey(mgt.makePropertyKey(CONTENT).dataType(String.class).make(), Mapping.TEXTSTRING.asParameter())
                .indexOnly(mgt.makeVertexLabel(STATUS_UPDATE).make())
                .buildMixedIndex(BACKING_INDEX);
    }

    private void createEdgeSchema() {
        log.info("Create edges schema");
        PropertyKey createdAt = mgt.makePropertyKey(CREATED_AT).dataType(Long.class).make();

        mgt.buildIndex(indexName(POSTS, CREATED_AT, Date.class), Edge.class)
                .addKey(createdAt)
                .indexOnly(mgt.makeEdgeLabel(POSTS).make())
                .buildMixedIndex(BACKING_INDEX);

        mgt.buildIndex(indexName(FOLLOWS, CREATED_AT, Date.class), Edge.class)
                .addKey(createdAt)
                .indexOnly(mgt.makeEdgeLabel(FOLLOWS).make())
                .buildMixedIndex(BACKING_INDEX);
    }

    private void close() {
        // we need to commit the Management changes or else they are not applied.
        mgt.commit();
        graph.tx().commit();
        graph.close();
    }


    public static String indexName(String label, String propertyKey, Class<?> clazz) {
        return label + ":by:" + propertyKey;
    }

}