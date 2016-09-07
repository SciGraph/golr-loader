package org.monarch.golr;

import io.scigraph.neo4j.Graph;
import io.scigraph.neo4j.GraphTransactionalImpl;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;

public class SimpleLoaderModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(Graph.class).to(GraphTransactionalImpl.class).in(Singleton.class);
  }

}