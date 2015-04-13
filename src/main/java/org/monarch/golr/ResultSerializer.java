package org.monarch.golr;

import static com.google.common.collect.Iterables.getFirst;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import javax.inject.Inject;

import org.monarch.golr.beans.Closure;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;

import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.inject.assistedinject.Assisted;

import edu.sdsc.scigraph.frames.CommonProperties;
import edu.sdsc.scigraph.frames.NodeProperties;
import edu.sdsc.scigraph.neo4j.DirectedRelationshipType;
import edu.sdsc.scigraph.neo4j.GraphUtil;
import edu.sdsc.scigraph.owlapi.OwlRelationships;
import edu.sdsc.scigraph.owlapi.curies.CurieUtil;

class ResultSerializer {

  private static final String ID_SUFFIX = "_id";
  private static final String ID_CLOSURE_SUFFIX = "_id_closure";
  private static final String LABEL_SUFFIX = "_label";
  private static final String LABEL_CLOSURE_SUFFIX = "_label_closure";

  private static final DirectedRelationshipType SUBCLASS =
      new DirectedRelationshipType(OwlRelationships.RDFS_SUBCLASS_OF, Direction.OUTGOING);
  private static final DirectedRelationshipType TYPE = 
      new DirectedRelationshipType(OwlRelationships.RDF_TYPE, Direction.OUTGOING);
  static final Collection<DirectedRelationshipType> DEFAULT_CLOSURE_TYPES =
      ImmutableSet.of(SUBCLASS, TYPE);

  private final JsonGenerator generator;
  private final ClosureUtil closureUtil;
  private final CurieUtil curieUtil;

  @Inject
  ResultSerializer(@Assisted JsonGenerator generator, CurieUtil curieUtil, ClosureUtil closureUtil) {
    this.generator = generator;
    this.closureUtil = closureUtil;
    this.curieUtil = curieUtil;
  }

  void writeArray(String fieldName, List<String> values) throws IOException {
    generator.writeArrayFieldStart(fieldName);
    for (String value: values) {
      generator.writeString(value);
    }
    generator.writeEndArray();
  }

  void serialize(String fieldName, Node value, Collection<DirectedRelationshipType> types) throws IOException {
    String iri = GraphUtil.getProperty(value, CommonProperties.URI, String.class).get();
    Optional<String> curie = curieUtil.getCurie(iri);
    generator.writeStringField(fieldName + ID_SUFFIX , curie.or(iri));
    Collection<String> labels = GraphUtil.getProperties(value, NodeProperties.LABEL, String.class);
    if (!labels.isEmpty()) {
      generator.writeStringField(fieldName + LABEL_SUFFIX, getFirst(labels, ""));
    }
    Closure closure = closureUtil.getClosure(value, types);
    writeArray(fieldName + ID_CLOSURE_SUFFIX, closure.getCuries());
    writeArray(fieldName + LABEL_CLOSURE_SUFFIX, closure.getLabels());
  }
  
  void serialize(String fieldName, Node value) throws IOException {
    serialize(fieldName, value, DEFAULT_CLOSURE_TYPES);
  }

  void serialize(String fieldName, String value) throws IOException {
    generator.writeStringField(fieldName, value);
  }

  void serialize(String fieldName, Boolean value) throws IOException {
    generator.writeBooleanField(fieldName, value);
  }

  void serialize(String fieldName, Integer value) throws IOException {
    generator.writeNumberField(fieldName, value);
  }

  void serialize(String fieldName, Long value) throws IOException {
    generator.writeNumberField(fieldName, value);
  }

  void serialize(String fieldName, Float value) throws IOException {
    generator.writeNumberField(fieldName, value);
  }

  void serialize(String fieldName, Double value) throws IOException {
    generator.writeNumberField(fieldName, value);
  }

  // TODO: A better pattern for this
  void serialize(String fieldName, Object value) throws IOException {
    if (value instanceof Node) {
      serialize(fieldName, (Node)value);
    } else if (value instanceof String) {
      serialize(fieldName, (String)value);
    } else if (value instanceof Boolean) {
      serialize(fieldName, (Boolean)value);
    } else if (value instanceof Integer) {
      serialize(fieldName, (Integer)value);
    } else if (value instanceof Long) {
      serialize(fieldName, (Long)value);
    } else if (value instanceof Float) {
      serialize(fieldName, (Float)value);
    } else if (value instanceof Double) {
      serialize(fieldName, (Double)value);
    }
  }

}
