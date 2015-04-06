package org.monarch;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.monarch.beans.Closure;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import edu.sdsc.scigraph.frames.NodeProperties;
import edu.sdsc.scigraph.neo4j.DirectedRelationshipType;
import edu.sdsc.scigraph.owlapi.OwlRelationships;
import edu.sdsc.scigraph.owlapi.curies.CurieUtil;
import edu.sdsc.scigraph.util.GraphTestBase;

public class ClosureTest extends GraphTestBase{

  Node a, b, c;
  ClosureUtil util;

  @Before
  public void setup() {
    Relationship r1 = addRelationship("http://x.org/a_a", "http://x.org/a_b", OwlRelationships.RDFS_SUBCLASS_OF);
    Relationship r2 = addRelationship("http://x.org/a_b", "http://x.org/a_c", OwlRelationships.RDFS_SUBCLASS_OF);
    r1.getEndNode().setProperty(NodeProperties.LABEL, "A");
    r2.getStartNode().setProperty(NodeProperties.LABEL, "C");
    a = r1.getEndNode();
    b = r2.getEndNode();
    c = r2.getStartNode();
    Map<String, String> curieMap = new HashMap<>();
    curieMap.put("X", "http://x.org/a_");
    CurieUtil curieUtil = new CurieUtil(curieMap);
    util = new ClosureUtil(graphDb, curieUtil);
  }

  @Test
  public void closure_areReturned() {
    DirectedRelationshipType type = new DirectedRelationshipType(OwlRelationships.RDFS_SUBCLASS_OF, Direction.OUTGOING);
    Closure closure = util.getClosure(c, type);
    assertThat(closure.getCuries(), contains("X:c", "X:b", "X:a"));
    assertThat(closure.getLabels(), contains("C", "A"));
    closure = util.getClosure(b, type);
    assertThat(closure.getCuries(), contains("X:b", "X:a"));
    assertThat(closure.getLabels(), contains("A"));
    closure = util.getClosure(a, type);
    assertThat(closure.getCuries(), contains("X:a"));
    assertThat(closure.getLabels(), contains("A"));
  }

}
