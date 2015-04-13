package org.monarch.golr;

import static com.google.common.collect.Sets.newHashSet;

import java.io.IOException;
import java.io.StringWriter;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.skyscreamer.jsonassert.JSONAssert;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

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
  public void serializeNodeWithDynamicType() throws Exception {
    a.createRelationshipTo(b, DynamicRelationshipType.withName("hasPart"));
    serializer.serialize("node", b, newHashSet(new DirectedRelationshipType("hasPart", "INCOMING")));
    JSONAssert.assertEquals(getFixture("fixtures/node.json"), getActual(), false);
  }

}
