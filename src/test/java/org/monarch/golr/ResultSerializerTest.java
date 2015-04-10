package org.monarch.golr;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URL;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.skyscreamer.jsonassert.JSONAssert;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;

import edu.sdsc.scigraph.neo4j.DirectedRelationshipType;

public class ResultSerializerTest extends GolrLoadSetup {

  StringWriter writer = new StringWriter();
  ResultSerializer serializer;
  JsonGenerator generator;

  @Before
  public void setup() throws Exception {
    generator = new JsonFactory().createGenerator(writer);
    serializer = new ResultSerializer(generator, curieUtil, closureUtil);
    generator.writeStartObject();
  }

  static String getFixture(String name) throws IOException {
    URL url = Resources.getResource(name);
    return Resources.toString(url, Charsets.UTF_8);
  }

  String getActual() throws IOException {
    generator.writeEndObject();
    generator.close();
    return writer.toString();
  }

  @Test
  public void serializePrimitiveTypes() throws Exception {
    serializer.serialize("string", "foo");
    serializer.serialize("boolean", true);
    serializer.serialize("int", 1);
    serializer.serialize("long", 1L);
    serializer.serialize("float", 1.0F);
    serializer.serialize("double", 1.0F);
    JSONAssert.assertEquals(getFixture("fixtures/primitives.json"), getActual(), false);
  }
  
  @Test
  public void serializeObjectTypes() throws Exception {
    serializer.serialize("string", (Object)"foo");
    serializer.serialize("boolean", (Object)true);
    serializer.serialize("int", (Object)1);
    serializer.serialize("long", (Object)1L);
    serializer.serialize("float", (Object)1.0F);
    serializer.serialize("double", (Object)1.0F);
    JSONAssert.assertEquals(getFixture("fixtures/primitives.json"), getActual(), false);
  }

  @Test
  public void serializeNode() throws Exception {
    serializer.serialize("node", b);
    JSONAssert.assertEquals(getFixture("fixtures/node.json"), getActual(), false);
  }

  @Test
  public void getNoDynamicRelationship() {
    assertThat(ResultSerializer.getRelationship("foo"), is(empty()));
  }

  @Test
  public void singleDynamicRelationship() {
    DirectedRelationshipType type = new DirectedRelationshipType(DynamicRelationshipType.withName("hasPart"), Direction.INCOMING);
    assertThat(ResultSerializer.getRelationship("foo$hasPart-IN"), contains(type));
  }

  @Test
  public void serializeNodeWithDynamicType() throws Exception {
    a.createRelationshipTo(b, DynamicRelationshipType.withName("hasPart"));
    serializer.serialize("node$hasPart-IN", b);
    JSONAssert.assertEquals(getFixture("fixtures/node.json"), getActual(), false);
  }

}
