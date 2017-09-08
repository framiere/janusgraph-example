package fr.ramiere;

import lombok.extern.slf4j.Slf4j;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.Mapping;

import java.util.Date;


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

    private final JanusGraph graph;
    private final JanusGraphManagement mgt;

    public static void main(String args[]) {
        new Janus(CONFIG_FILE);
    }

    public Janus(String configFile) {
        log.info("Connecting graph");
        graph = JanusGraphFactory.open(configFile);
        log.info("Getting management");
        mgt = graph.openManagement();

        createUserSchema();
        createStatusUpdateSchema();
        createEdgeSchema();
        close();
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