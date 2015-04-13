package org.monarch.golr;

import com.google.inject.AbstractModule;
import com.google.inject.assistedinject.FactoryModuleBuilder;

class GolrLoaderModule extends AbstractModule {

  @Override
  protected void configure() {
    install(new FactoryModuleBuilder()
    .implement(ResultSerializer.class, ResultSerializer.class)
    .build(ResultSerializerFactory.class));
    
  }

}
