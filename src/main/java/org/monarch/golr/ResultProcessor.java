package org.monarch.golr;

import java.io.IOException;
import java.io.Writer;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import javax.inject.Inject;

import org.neo4j.graphdb.Result;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

public class ResultProcessor {

  private final ResultSerializerFactory factory;

  @Inject
  ResultProcessor(ResultSerializerFactory factory) {
    this.factory = factory;
  }

  void process(Result result, Writer writer, Collection<String> originalProjection) throws IOException {
    JsonGenerator generator = new JsonFactory().createGenerator(writer);
    ResultSerializer serializer = factory.create(generator);
    generator.writeStartArray();
    while (result.hasNext()) {
      generator.writeStartObject();
      Map<String, Object> row = result.next();
      for (Entry<String, Object> entry: row.entrySet()) {
        if (originalProjection.contains(entry.getKey())) {
          serializer.serialize(entry.getKey(), entry.getValue());
        }
      }
      generator.writeEndObject();
    }
    generator.writeEndArray();
    generator.close();
  }

}
