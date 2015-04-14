package org.monarch.golr;

import static com.google.common.collect.Sets.newHashSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

import java.util.Set;

import org.junit.Test;
import org.monarch.golr.beans.Closure;
import org.neo4j.graphdb.Direction;

import edu.sdsc.scigraph.neo4j.DirectedRelationshipType;
import edu.sdsc.scigraph.owlapi.OwlRelationships;

public class ClosureTest extends GolrLoadSetup {

  @Test
  public void closures_areReturned() {
    DirectedRelationshipType type = new DirectedRelationshipType(OwlRelationships.RDFS_SUBCLASS_OF, Direction.OUTGOING);
    Set<DirectedRelationshipType> types = newHashSet(type);
    Closure closure = closureUtil.getClosure(c, types);
    assertThat(closure.getCuries(), contains("X:c", "X:b", "X:a"));
    assertThat(closure.getLabels(), contains("C", "X:b", "A"));
    closure = closureUtil.getClosure(b, types);
    assertThat(closure.getCuries(), contains("X:b", "X:a"));
    assertThat(closure.getLabels(), contains("X:b", "A"));
    closure = closureUtil.getClosure(a, types);
    assertThat(closure.getCuries(), contains("X:a"));
    assertThat(closure.getLabels(), contains("A"));
  }

  @Test
  public void multipleRelationships_areFollowed() {
    DirectedRelationshipType type1 = new DirectedRelationshipType(OwlRelationships.RDFS_SUBCLASS_OF, Direction.OUTGOING);
    DirectedRelationshipType type2 = new DirectedRelationshipType(OwlRelationships.RDF_TYPE, Direction.OUTGOING);
    Set<DirectedRelationshipType> types = newHashSet(type1, type2);
    Closure closure = closureUtil.getClosure(d, types);
    assertThat(closure.getCuries(), contains("X:d", "X:c", "X:b", "X:a"));
    assertThat(closure.getLabels(), contains("X:d", "C", "X:b", "A"));
  }

}
