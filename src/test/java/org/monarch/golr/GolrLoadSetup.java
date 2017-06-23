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

public class GolrLoadSetup extends io.scigraph.util.GraphTestBase {

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
      Relationship r1 = addRelationship("http://x.org/a_a", "http://x.org/a_b", OwlRelationships.RDFS_SUBCLASS_OF);
      Relationship r2 = addRelationship("http://x.org/a_b", "http://x.org/a_c", OwlRelationships.RDFS_SUBCLASS_OF);
      Relationship r3 = addRelationship("http://x.org/a_c", "http://x.org/a_d", OwlRelationships.RDF_TYPE);
      Relationship r4 = addRelationship("http://x.org/a_e", "http://x.org/a_d", RelationshipType.withName("CAUSES"));
      Relationship r5 = addRelationship("http://x.org/a_f", "http://x.org/a_e", RelationshipType.withName("partOf"));
      graph.setRelationshipProperty(r4.getId(), CommonProperties.IRI, "http://x.org/a_causes");
      addRelationship("http://x.org/a_causes_parent", "http://x.org/a_causes", OwlRelationships.RDFS_SUB_PROPERTY_OF);
      addRelationship("_:anon", "http://x.org/a_b", OwlRelationships.RDFS_SUBCLASS_OF);
      r1.getEndNode().setProperty(NodeProperties.LABEL, "A");
      r2.getStartNode().setProperty(NodeProperties.LABEL, "C");
      a = r1.getEndNode();
      b = r2.getEndNode();
      c = r2.getStartNode();
      d = r3.getStartNode();
      e = r4.getEndNode();
      f = r5.getEndNode();
      Node assn = createNode("http://x.org/a_assn");
      Node assnParent = createNode("http://x.org/a_assn_parent");
      assn.createRelationshipTo(assnParent, OwlRelationships.RDFS_SUBCLASS_OF);
      Node evidence = createNode("http://x.org/a_evidence");
      assn.createRelationshipTo(evidence, RelationshipType.withName("http://purl.obolibrary.org/obo/RO_0002558"));
      assn.createRelationshipTo(d, RelationshipType.withName("http://purl.org/oban/association_has_subject"));
      assn.createRelationshipTo(e, RelationshipType.withName("http://purl.org/oban/association_has_object"));
      
      Node gene = createNode("http://x.org/gene");
      gene.addLabel(Label.label("gene"));
      Node ortholog = createNode("http://x.org/gene_ortholog");
      ortholog.addLabel(Label.label("gene"));
      gene.createRelationshipTo(ortholog, RelationshipType.withName("http://purl.obolibrary.org/obo/RO_HOM0000017"));

      Node foo = createNode("http://x.org/gene");
      foo.addLabel(Label.label("gene"));

      Node forebrain = createNode("http://purl.obolibrary.org/obo/UBERON_0001890");
      forebrain.addLabel(Label.label("forebrain"));
      forebrain.addLabel(Label.label("anatomical entity"));
      Relationship foo2forebrain = foo.createRelationshipTo(forebrain, RelationshipType.withName("http://purl.obolibrary.org/obo/RO_0002206"));
      foo2forebrain.setProperty(CommonProperties.IRI, "http://purl.obolibrary.org/obo/RO_0002206");

      Node brain = createNode("http://purl.obolibrary.org/obo/UBERON_0000955");
      brain.addLabel(Label.label("brain"));
      brain.addLabel(Label.label("anatomical entity"));
      Relationship forebrain2brain = forebrain.createRelationshipTo(brain, RelationshipType.withName("http://purl.obolibrary.org/obo/BFO_0000050"));
      forebrain2brain.setProperty(CommonProperties.IRI, "http://purl.obolibrary.org/obo/BFO_0000050");

      Node neuralTube = createNode("http://purl.obolibrary.org/obo/UBERON_0001049");
      neuralTube.addLabel(Label.label("neural tube"));
      neuralTube.addLabel(Label.label("anatomical entity"));
      Relationship brain2neuralTube = brain.createRelationshipTo(neuralTube, RelationshipType.withName("http://purl.obolibrary.org/obo/RO_0002202"));
      brain2neuralTube.setProperty(CommonProperties.IRI, "http://purl.obolibrary.org/obo/RO_0002202");

      Node head = createNode("http://purl.obolibrary.org/obo/UBERON_0000033");
      head.addLabel(Label.label("head"));
      head.addLabel(Label.label("anatomical entity"));
      Relationship brain2head = brain.createRelationshipTo(head, RelationshipType.withName("http://purl.obolibrary.org/obo/BFO_0000050"));
      brain2head.setProperty(CommonProperties.IRI, "http://purl.obolibrary.org/obo/BFO_0000050");

      Node bodyPart = createNode("http://x.org/body_part");
      bodyPart.addLabel(Label.label("body part"));
      bodyPart.addLabel(Label.label("anatomical entity"));
      Relationship head2bodyPart = head.createRelationshipTo(bodyPart, OwlRelationships.RDFS_SUBCLASS_OF);
      head2bodyPart.setProperty(CommonProperties.IRI, OwlRelationships.RDFS_SUBCLASS_OF.name());

      Node anatomicalEntity = createNode("http://purl.obolibrary.org/obo/UBERON_0001062");
      anatomicalEntity.addLabel(Label.label("anatomical entity"));
      Relationship bodyPart2anatomicalEntity = bodyPart.createRelationshipTo(anatomicalEntity, OwlRelationships.RDFS_SUBCLASS_OF);
      bodyPart2anatomicalEntity.setProperty(CommonProperties.IRI, OwlRelationships.RDFS_SUBCLASS_OF.name());

      tx.success();
    }
  }

  @BeforeClass
  public static void buildGraph() {
    populateGraph(graphDb);
    curieMap.put("X", "http://x.org/a_");
    curieUtil = new CurieUtil(curieMap);
    closureUtil = new ClosureUtil(graphDb, curieUtil);
  }

}
