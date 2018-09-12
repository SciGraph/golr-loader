package org.monarch.golr;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;


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
    static Map<String, List<String>> eqCurieMap = new HashMap<>();

    static String getFixture(String name) throws IOException {
        URL url = Resources.getResource(name);
        return Resources.toString(url, Charsets.UTF_8);
    }

    static void populateGraph(GraphDatabaseService graphDb) {
        try (Transaction tx = graphDb.beginTx()) {

            Node gene = createNode("http://x.org/geneA");
            gene.setProperty(NodeProperties.LABEL, "SHH");
            gene.addLabel(Label.label("gene"));
            gene.addLabel(Label.label("Node"));
            gene.addLabel(Label.label("cliqueLeader"));
            Node taxa = createNode("http://x.org/taxa");
            taxa.setProperty(NodeProperties.LABEL, "Homo sapiens");
            taxa.addLabel(Label.label("organism"));
            taxa.addLabel(Label.label("cliqueLeader"));
            Node geneB = createNode("http://x.org/geneB");
            gene.createRelationshipTo(taxa, RelationshipType.withName("http://purl.obolibrary.org/obo/RO_0002162"));
            gene.createRelationshipTo(geneB, RelationshipType.withName("http://purl.obolibrary.org/obo/RO_0002435"));

            Node bnode1 = createNode("_:1234");
            bnode1.setProperty(NodeProperties.LABEL, "some bnode");
            bnode1.addLabel(Label.label("cliqueLeader"));

            Node bnode2 = createNode("https://monarchinitiative.org/.well-known/genid/121002-41751VL");
            bnode2.setProperty(NodeProperties.LABEL, "bnode variant");
            bnode2.addLabel(Label.label("cliqueLeader"));

            Node eqGene = createNode("http://x.org/eqGeneA");
            gene.createRelationshipTo(eqGene, OwlRelationships.OWL_SAME_AS);

            tx.success();
        }
    }

    @BeforeClass
    public static void buildGraph() {
        populateGraph(graphDb);
        curieMap.put("X", "http://x.org/");
        List<String> eq = new ArrayList<>();
        eq.add("Y");
        eqCurieMap.put("X", eq);
        curieUtil = new CurieUtil(curieMap);
        closureUtil = new ClosureUtil(graphDb, curieUtil);
    }

}