package org.monarch.golr;

import static java.util.Collections.singleton;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.inject.Inject;

import org.monarch.golr.beans.Closure;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.google.inject.assistedinject.Assisted;

import edu.sdsc.scigraph.neo4j.DirectedRelationshipType;
import edu.sdsc.scigraph.owlapi.OwlRelationships;

class ResultSerializer {

  static final String ID_SUFFIX = "";
  static final String ID_CLOSURE_SUFFIX = "_closure";
  static final String LABEL_SUFFIX = "_label";
  static final String LABEL_CLOSURE_SUFFIX = "_closure_label";
  static final String CLOSURE_MAP_SUFFIX = "_closure_map";

  private static final DirectedRelationshipType SUBCLASS =
      new DirectedRelationshipType(OwlRelationships.RDFS_SUBCLASS_OF, Direction.OUTGOING);
  private static final DirectedRelationshipType EQUIVALENT_CLASS =
      new DirectedRelationshipType(OwlRelationships.OWL_EQUIVALENT_CLASS, Direction.BOTH);
  private static final DirectedRelationshipType SAME_AS =
      new DirectedRelationshipType(OwlRelationships.OWL_SAME_AS, Direction.BOTH);
  private static final DirectedRelationshipType TYPE = 
      new DirectedRelationshipType(OwlRelationships.RDF_TYPE, Direction.OUTGOING);
  private static final DirectedRelationshipType SUBPROPERTY = 
      new DirectedRelationshipType(OwlRelationships.RDFS_SUB_PROPERTY_OF, Direction.OUTGOING);
  static final Collection<DirectedRelationshipType> DEFAULT_CLOSURE_TYPES =
      ImmutableSet.of(EQUIVALENT_CLASS, SUBCLASS, TYPE, SAME_AS, SUBPROPERTY);

  private final ObjectMapper mapper = new ObjectMapper();
  private final JsonGenerator generator;
  private final ClosureUtil closureUtil;

  @Inject
  ResultSerializer(@Assisted JsonGenerator generator, ClosureUtil closureUtil) {
    this.generator = generator;
    this.closureUtil = closureUtil;
  }

  void writeArray(String fieldName, List<String> values) throws IOException {
    generator.writeArrayFieldStart(fieldName);
    for (String value: values) {
      generator.writeString(value);
    }
    generator.writeEndArray();
  }

  void serialize(String fieldName, Collection<Node> values, Collection<DirectedRelationshipType> types) throws IOException {
    List<Closure> closures = new ArrayList<>();
    for (Node node: values) {
      closures.add(closureUtil.getClosure(node, types));
    }
    writeQuint(fieldName, closures);
  }
  
  void serialize(String fieldName, Collection<Node> values) throws IOException {
    serialize(fieldName, values, DEFAULT_CLOSURE_TYPES);
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
      serialize(fieldName, singleton((Node)value));
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
    } else {
      throw new IllegalArgumentException("Don't know how to serialize " + value.getClass());
    }
  }

  void writeQuint(String baseName, List<Closure> closures)
      throws IOException {
    writeArray(baseName + ID_SUFFIX, ClosureUtil.collectIds(closures));
    writeArray(baseName + LABEL_SUFFIX,
        ClosureUtil.collectLabels(closures));
    writeArray(baseName + ID_CLOSURE_SUFFIX,
        ClosureUtil.collectIdClosure(closures));
    writeArray(baseName + LABEL_CLOSURE_SUFFIX,
        ClosureUtil.collectLabelClosure(closures));
    StringWriter writer = new StringWriter();
    mapper.writeValue(writer, ClosureUtil.collectClosureMap(closures));
    generator.writeStringField(baseName + CLOSURE_MAP_SUFFIX, writer.toString());
    
  }

}
