package org.monarch.golr;

import java.util.HashMap;
import java.util.Map;

import org.junit.BeforeClass;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import edu.sdsc.scigraph.frames.NodeProperties;
import edu.sdsc.scigraph.owlapi.OwlRelationships;
import edu.sdsc.scigraph.owlapi.curies.CurieUtil;
import edu.sdsc.scigraph.util.GraphTestBase;

public class GolrLoadSetup extends GraphTestBase {

  static Node a, b, c, d;
  static CurieUtil curieUtil;
  static ClosureUtil closureUtil;
  
  @BeforeClass
  public static void buildGraph() {
    try (Transaction tx = graphDb.beginTx()) {
      Relationship r1 = addRelationship("http://x.org/a_a", "http://x.org/a_b", OwlRelationships.RDFS_SUBCLASS_OF);
      Relationship r2 = addRelationship("http://x.org/a_b", "http://x.org/a_c", OwlRelationships.RDFS_SUBCLASS_OF);
      Relationship r3 = addRelationship("http://x.org/a_c", "http://x.org/a_d", OwlRelationships.RDF_TYPE);
      r1.getEndNode().setProperty(NodeProperties.LABEL, "A");
      r2.getStartNode().setProperty(NodeProperties.LABEL, "C");
      a = r1.getEndNode();
      b = r2.getEndNode();
      c = r2.getStartNode();
      d = r3.getStartNode();
      Map<String, String> curieMap = new HashMap<>();
      curieMap.put("X", "http://x.org/a_");
      curieUtil = new CurieUtil(curieMap);
      closureUtil = new ClosureUtil(graphDb, curieUtil);
      tx.success();
    }
  }
  
}
