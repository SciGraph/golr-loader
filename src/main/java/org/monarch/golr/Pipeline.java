package org.monarch.golr;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.monarch.golr.beans.GolrCypherQuery;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;

import io.scigraph.internal.EvidenceAspect;
import io.scigraph.internal.GraphAspect;
import io.scigraph.neo4j.Neo4jConfiguration;
import io.scigraph.neo4j.Neo4jModule;

public class Pipeline {

  private static final Logger logger = Logger.getLogger(Pipeline.class.getName());

  private static ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

  private static final Object SOLR_LOCK = new Object();

  static {
    mapper.registerModules(new GuavaModule());
  }

  public static Options getOptions() {
    Options options = new Options();
    Option option = Option.builder("g").longOpt("graph").required().hasArg()
        .desc("The Neo4j graph configuration").build();
    options.addOption(option);
    option = Option.builder("q").longOpt("query").required().hasArg()
        .desc("The query configuration").build();
    options.addOption(option);
    option = Option.builder("s").longOpt("solr-server").required().hasArg()
        .desc("An optional Solr server to update").build();
    options.addOption(option);
    return options;
  }

  public static void main(String[] args) throws JsonParseException, JsonMappingException,
      IOException, URISyntaxException, ExecutionException, InterruptedException,
      KeyManagementException, NoSuchAlgorithmException, KeyStoreException, SolrServerException {
    Options options = getOptions();
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd;
    Neo4jConfiguration neo4jConfig = null;
    File filePath = null;
    String solrServer = null;
    try {
      cmd = parser.parse(options, args);
      solrServer = cmd.getOptionValue("s");
      neo4jConfig = mapper.readValue(new File(cmd.getOptionValue("g")), Neo4jConfiguration.class);
      filePath = new File(cmd.getOptionValue("q"));
    } catch (ParseException e) {
      e.printStackTrace();
      new HelpFormatter().printHelp("GolrLoad", options);
      System.exit(-1);
    }

    Injector i = Guice.createInjector(new GolrLoaderModule(), new Neo4jModule(neo4jConfig),
          new AbstractModule() {
            @Override
            protected void configure() {
              bind(GraphAspect.class).to(EvidenceAspect.class);
            }
            
          });

    GolrLoader loader = i.getInstance(GolrLoader.class);

    final ExecutorService pool =
        Executors.newFixedThreadPool(25);
      List<Future<Boolean>> futures = new ArrayList<>();

    for (final File fileEntry : filePath.listFiles()) {
      GolrCypherQuery query = mapper.readValue(fileEntry, GolrCypherQuery.class);
      Optional<String> queryName = Optional.of(fileEntry.getName());

      final Future<Boolean> contentFuture = pool.submit(new GolrWorker(solrServer,
            loader, query, SOLR_LOCK, queryName));
      futures.add(contentFuture);
    }

    for (Future<Boolean> future : futures) {
      future.get();
    }
    pool.shutdown();
    pool.awaitTermination(10, TimeUnit.DAYS);
    
    logger.info("Golr load completed");
    
    logger.info("Optimizing solr");
    SolrClient solrClient = new HttpSolrClient.Builder(solrServer).build();
    solrClient.optimize();
    solrClient.close();
    logger.info("Optimization complete");

  }
}
