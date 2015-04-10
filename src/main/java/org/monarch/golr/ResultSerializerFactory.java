package org.monarch.golr;

import com.fasterxml.jackson.core.JsonGenerator;

public interface ResultSerializerFactory {

  ResultSerializer create(JsonGenerator generator);

}
