package org.monarch.golr;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.junit.BeforeClass;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.prefixcommons.CurieUtil;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;

import io.scigraph.frames.CommonProperties;
import io.scigraph.frames.NodeProperties;
import io.scigraph.owlapi.OwlRelationships;

public class SimpleLoadSetup extends io.scigraph.util.GraphTestBase {

    static Node a, b, c, d, e, f;
    static CurieUtil curieUtil;
    static ClosureUtil closureUtil;
    static Map<String, String> curieMap = new HashMap<>();

    static String getFixture(String name) throws IOException {
        URL url = Resources.getResource(name);
        return Resources.toString(url, Charsets.UTF_8);
    }

    static void populateGraph(GraphDatabaseService graphDb) {
        try (Transaction tx = graphDb.beginTx()) {

            Node gene = createNode("http://x.org/geneA");
            gene.setProperty(NodeProperties.LABEL, "SHH");
            gene.addLabel(Label.label("gene"));
            gene.addLabel(Label.label("cliqueLeader"));
            Node taxa = createNode("http://x.org/taxa");
            taxa.setProperty(NodeProperties.LABEL, "Homo sapiens");
            taxa.addLabel(Label.label("organism"));
            taxa.addLabel(Label.label("cliqueLeader"));
            Node geneB = createNode("http://x.org/geneB");
            gene.createRelationshipTo(taxa, RelationshipType.withName("http://purl.obolibrary.org/obo/RO_0002162"));
            gene.createRelationshipTo(geneB, RelationshipType.withName("http://purl.obolibrary.org/obo/RO_0002435"));


            tx.success();
        }
    }

    @BeforeClass
    public static void buildGraph() {
        populateGraph(graphDb);
        curieMap.put("X", "http://x.org/");
        curieUtil = new CurieUtil(curieMap);
        closureUtil = new ClosureUtil(graphDb, curieUtil);
    }

}