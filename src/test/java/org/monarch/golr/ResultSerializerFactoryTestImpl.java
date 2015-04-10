package org.monarch.golr;

import com.fasterxml.jackson.core.JsonGenerator;

class ResultSerializerFactoryTestImpl implements ResultSerializerFactory {

  @Override
  public ResultSerializer create(JsonGenerator generator) {
    return new ResultSerializer(generator, GolrLoadSetup.curieUtil, GolrLoadSetup.closureUtil);
  }

}
