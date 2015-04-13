package org.monarch.golr;

import com.fasterxml.jackson.core.JsonGenerator;

interface ResultSerializerFactory {

  ResultSerializer create(JsonGenerator generator);

}
