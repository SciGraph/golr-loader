package org.monarch.golr;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import org.monarch.golr.beans.GolrCypherQuery;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.guava.GuavaModule;

import edu.sdsc.scigraph.neo4j.DirectedRelationshipType;

public class YamlSerializationTest {

  ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

  @Before
  public void setup() {
    mapper.registerModule(new GuavaModule());
  }

  @Test
  public void test() throws JsonParseException, JsonMappingException, IOException {
    GolrCypherQuery query = mapper.readValue(
        "query: cypher query\n"
        + "types:\n"
        + "  foo:\n"
        + "    - type: partOf\n"
        + "      direction: OUTGOING\n", GolrCypherQuery.class);
    assertThat(query.getQuery(), is("cypher query"));
    assertThat(query.getTypes().get("foo"), contains(new DirectedRelationshipType("partOf", "OUTGOING")));
  }

}
