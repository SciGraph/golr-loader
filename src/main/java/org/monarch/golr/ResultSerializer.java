package org.monarch.golr;

import static com.google.common.collect.Iterables.getFirst;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.inject.Inject;

import org.monarch.golr.beans.Closure;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
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
  private static final Collection<DirectedRelationshipType> DEFAULT_CLOSURE_TYPES =
      ImmutableSet.of(SUBCLASS, TYPE);

  private static final String FIELD_REGEX = "(.*)\\$(.*)-(IN|OUT)";
  private static final Pattern FIELD_PATTERN = Pattern.compile(FIELD_REGEX);

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

  static String getFieldname(String fieldName) {
    return fieldName.contains("$") ? fieldName.substring(0, fieldName.indexOf('$')) : fieldName;
  }

  static Collection<DirectedRelationshipType> getRelationship(String fieldName) {
    Collection<DirectedRelationshipType> types = new HashSet<>();
    Matcher matcher = FIELD_PATTERN.matcher(fieldName);
    if (matcher.matches()) {
      Direction direction;
      switch (matcher.group(3)) {
        case "IN":
          direction = Direction.INCOMING;
          break;
        case "OUT":
          direction = Direction.OUTGOING;
          break;
        default:
          throw new IllegalArgumentException("Unknown direction " + matcher.group(3));
      }
      types.add(new DirectedRelationshipType(DynamicRelationshipType.withName(matcher.group(2)), direction));
    }
    return types;
  }

  void serialize(String fieldName, Node value) throws IOException {
    String iri = GraphUtil.getProperty(value, CommonProperties.URI, String.class).get();
    Collection<DirectedRelationshipType> types = getRelationship(fieldName);
    Optional<String> curie = curieUtil.getCurie(iri);
    fieldName = getFieldname(fieldName);
    generator.writeStringField(fieldName + ID_SUFFIX , curie.or(iri));
    Collection<String> labels = GraphUtil.getProperties(value, NodeProperties.LABEL, String.class);
    if (!labels.isEmpty()) {
      generator.writeStringField(fieldName + LABEL_SUFFIX, getFirst(labels, ""));
    }
    if (types.isEmpty()) {
      types.addAll(DEFAULT_CLOSURE_TYPES);
    }
    Closure closure = closureUtil.getClosure(value, types);
    writeArray(fieldName + ID_CLOSURE_SUFFIX, closure.getCuries());
    writeArray(fieldName + LABEL_CLOSURE_SUFFIX, closure.getLabels());
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
