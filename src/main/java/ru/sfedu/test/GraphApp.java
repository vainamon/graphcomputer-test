package ru.sfedu.test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Future;


import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.search.path.ShortestPathVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphApp {
    private static final Logger LOGGER = LoggerFactory.getLogger(GraphApp.class);

    protected String propFileName;
    protected Configuration conf;
    protected Graph graph;
    protected GraphTraversalSource g;
    protected boolean supportsTransactions;
    protected boolean supportsSchema;

    /**
     * Constructs a graph app using the given properties.
     * @param fileName location of the properties file
     */
    public GraphApp(final String fileName) {
        propFileName = fileName;
    }

    /**
     * Opens the graph instance. If the graph instance does not exist, a new
     * graph instance is initialized.
     */
    public GraphTraversalSource openGraph() throws ConfigurationException {
        LOGGER.info("opening graph");
        conf = new PropertiesConfiguration(propFileName);
        graph = GraphFactory.open(conf);
        g = graph.traversal();
        return g;
    }

    /**
     * Closes the graph instance.
     */
    public void closeGraph() throws Exception {
        LOGGER.info("closing graph");
        try {
            if (g != null) {
                g.close();
            }
            if (graph != null) {
                graph.close();
            }
        } finally {
            g = null;
            graph = null;
        }
    }

    /**
     * Drops the graph instance. The default implementation does nothing.
     */
    public void dropGraph() throws Exception {
    }

    /**
     * Creates the graph schema. The default implementation does nothing.
     */
    public void createSchema() {
    }

    /**
     * Adds the vertices, edges, and properties to the graph.
     */
    public void createElements() {
        try {
            // naive check if the graph was previously created
            if (g.V().has("label", "A").hasNext()) {
                if (supportsTransactions) {
                    g.tx().rollback();
                }
                return;
            }
            LOGGER.info("creating elements");

            final Vertex A = g.addV("vertex").property("label", "A").next();
            final Vertex B = g.addV("vertex").property("label", "B").next();
            final Vertex C = g.addV("vertex").property("label", "C").next();
            final Vertex D = g.addV("vertex").property("label", "D").next();
            final Vertex E = g.addV("vertex").property("label", "E").next();
            final Vertex F = g.addV("vertex").property("label", "F").next();
            final Vertex G = g.addV("vertex").property("label", "G").next();
            final Vertex H = g.addV("vertex").property("label", "H").next();
            final Vertex I = g.addV("vertex").property("label", "I").next();

            g.V(A).as("a").V(B).addE("adjacent").property("distance", 7).from("a").next();
            g.V(A).as("a").V(C).addE("adjacent").property("distance", 10).from("a").next();

            g.V(B).as("a").V(G).addE("adjacent").property("distance", 27).from("a").next();
            g.V(B).as("a").V(F).addE("adjacent").property("distance", 9).from("a").next();

            g.V(C).as("a").V(F).addE("adjacent").property("distance", 8).from("a").next();
            g.V(C).as("a").V(E).addE("adjacent").property("distance", 31).from("a").next();

            g.V(F).as("a").V(H).addE("adjacent").property("distance", 11).from("a").next();

            g.V(E).as("a").V(D).addE("adjacent").property("distance", 32).from("a").next();

            g.V(G).as("a").V(I).addE("adjacent").property("distance", 15).from("a").next();

            g.V(H).as("a").V(D).addE("adjacent#1").property("distance", 17).from("a").next();
            g.V(H).as("a").V(I).addE("adjacent").property("distance", 15).from("a").next();

            g.V(D).as("a").V(H).addE("adjacent#2").property("distance", 17).from("a").next();
            g.V(D).as("a").V(I).addE("adjacent").property("distance", 21).from("a").next();

            if (supportsTransactions) {
                g.tx().commit();
            }

        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            if (supportsTransactions) {
                g.tx().rollback();
            }
        }
    }

    /**
     * Returns the geographical coordinates as a float array.
     */
    protected float[] getGeoFloatArray(final float lat, final float lon) {
        final float[] fa = { lat, lon };
        return fa;
    }

    /**
     * Runs some traversal queries to get data from the graph.
     */
    public void readElements() {
        try {
            if (g == null) {
                return;
            }

            LOGGER.info("reading elements");

            final Optional<Map<Object, Object>> v = g.V().has("label", "A").valueMap().tryNext();
            if (v.isPresent()) {
                LOGGER.info(v.get().toString());
            } else {
                LOGGER.warn("A not found");
            }

            // look up an incident edge
            final Optional<Map<Object, Object>> edge = g.V().has("label", "F").outE("adjacent").as("e").inV()
                    .has("label", "H").select("e").valueMap().tryNext();
            if (edge.isPresent()) {
                LOGGER.info(edge.get().toString());
            } else {
                LOGGER.warn("F -> H not found");
            }

            // H might be deleted
            final boolean HExists = g.V().has("label", "H").hasNext();
            if (HExists) {
                LOGGER.info("H exists");
            } else {
                LOGGER.warn("H not found");
            }

            final List<Object> adjacent = g.V().has("label", "F").both("adjacent").values("label").dedup().toList();
            LOGGER.info("F's adjacent: " + adjacent.toString());

        } finally {
            // the default behavior automatically starts a transaction for
            // any graph interaction, so it is best to finish the transaction
            // even for read-only graph query operations
            if (supportsTransactions) {
                g.tx().rollback();
            }
        }
    }

    /**
     * Makes an update to the existing graph structure. Does not create any
     * new vertices or edges.
     */
    public void updateElements() {
        try {
            if (g == null) {
                return;
            }
            LOGGER.info("updating elements");
            final long ts = System.currentTimeMillis();
            g.V().has("label", "A").property("ts", ts).iterate();
            if (supportsTransactions) {
                g.tx().commit();
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            if (supportsTransactions) {
                g.tx().rollback();
            }
        }
    }

    /**
     * Deletes elements from the graph structure. When a vertex is deleted,
     * its incident edges are also deleted.
     */
    public void deleteElements() {
        try {
            if (g == null) {
                return;
            }
            LOGGER.info("deleting elements");
            // note that this will succeed whether or not H exists
            g.V().has("label", "H").drop().iterate();
            if (supportsTransactions) {
                g.tx().commit();
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            if (supportsTransactions) {
                g.tx().rollback();
            }
        }
    }

    public void runShortestPathComputer() {
        try {
            if (g == null) {
                return;
            }
            LOGGER.info("run shortest path");


            ShortestPathVertexProgram spvp = ShortestPathVertexProgram.build()
                    .includeEdges(true)
                    .distanceProperty("distance")
                    .source(__.has("label", "A"))
                    .target(__.has("label", "D"))
                    .create();

            ComputerResult result = graph.compute().program(spvp).submit().get();

            List<Path> paths = result.memory().get(ShortestPathVertexProgram.SHORTEST_PATHS);

            LOGGER.info(String.valueOf(paths.size()));
            LOGGER.info(paths.get(0).toString());

            paths.forEach(p -> {LOGGER.info("Path " + paths.indexOf(p)); p.forEach(re -> LOGGER.info(re instanceof  ReferenceVertex ?
                    g.V(re).next().properties("label").next().toString()
                    : g.E(re).next().properties("distance").next().toString() + " " + g.E(re).next().toString())); });

            if (supportsTransactions) {
                g.tx().commit();
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            if (supportsTransactions) {
                g.tx().rollback();
            }
        }
    }

    /**
     * Run the entire application:
     * 1. Open and initialize the graph
     * 2. Define the schema
     * 3. Build the graph
     * 4. Run traversal queries to get data from the graph
     * 5. Make updates to the graph
     * 6. Close the graph
     */
    public void runApp() {
        try {
            // open and initialize the graph
            openGraph();

            // define the schema before loading data
            if (supportsSchema) {
                createSchema();
            }

            // build the graph structure
            createElements();
            // read to see they were made
            readElements();

            for (int i = 0; i < 2; i++) {
                try {
                    Thread.sleep((long) (Math.random() * 500) + 500);
                } catch (InterruptedException e) {
                    LOGGER.error(e.getMessage(), e);
                }
                // update some graph elements with changes
                updateElements();
                // read to see the changes were made
                readElements();
            }

            runShortestPathComputer();

            // delete some graph elements
            deleteElements();
            // read to see the changes were made
            readElements();

            runShortestPathComputer();

            // close the graph
            closeGraph();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }
}
