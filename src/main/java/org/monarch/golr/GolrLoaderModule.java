package org.monarch.golr;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.assistedinject.FactoryModuleBuilder;

import edu.sdsc.scigraph.neo4j.Graph;
import edu.sdsc.scigraph.neo4j.GraphTransactionalImpl;

class GolrLoaderModule extends AbstractModule {

  @Override
  protected void configure() {
    install(new FactoryModuleBuilder()
    .implement(ResultSerializer.class, ResultSerializer.class)
    .build(ResultSerializerFactory.class));
    bind(Graph.class).to(GraphTransactionalImpl.class).in(Singleton.class);
  }

}
