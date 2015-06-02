package org.monarch.golr;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.http.client.fluent.Request;
import org.apache.http.entity.ContentType;
import org.monarch.golr.beans.GolrCypherQuery;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.base.Optional;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;

import edu.sdsc.scigraph.internal.EvidenceAspect;
import edu.sdsc.scigraph.internal.GraphAspect;
import edu.sdsc.scigraph.neo4j.Neo4jConfiguration;
import edu.sdsc.scigraph.neo4j.Neo4jModule;

public class Pipeline {

  private static final Logger logger = Logger.getLogger(Pipeline.class.getName());

  private static ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

  private static final String SOLR_JSON_URL_SUFFIX = "update/json?commit=true";

  static {
    mapper.registerModules(new GuavaModule());
  }

  public static Options getOptions() {
    Options options = new Options();
    Option option = Option.builder("g").longOpt("graph").required().hasArg().desc("The Neo4j graph configuration").build();
    options.addOption(option);
    option = Option.builder("q").longOpt("query").required().hasArg().desc("The query configuration").build();
    options.addOption(option);
    option = Option.builder("s").longOpt("solr-server").required(false).hasArg().desc("An optional Solr server to update").build();
    options.addOption(option);
    option = Option.builder("o").longOpt("output").required(false).hasArg().desc("An optional output file for the JSON").build();
    options.addOption(option);
    return options;
  }

  public static void main(String[] args) throws JsonParseException, JsonMappingException, IOException, URISyntaxException, ExecutionException {
    Options options = getOptions();
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd;
    Neo4jConfiguration neo4jConfig = null;
    GolrCypherQuery query = null;
    Optional<String> solrServer = Optional.absent();
    Optional<String> outputFile = Optional.absent();
    try {
      cmd = parser.parse(options, args);
      neo4jConfig = mapper.readValue(new File(cmd.getOptionValue("g")), Neo4jConfiguration.class);
      query = mapper.readValue(new File(cmd.getOptionValue("q")), GolrCypherQuery.class);
      if (cmd.hasOption("s")) {
        solrServer = Optional.of(cmd.getOptionValue("s"));
      }
      if (cmd.hasOption("o")) {
        outputFile = Optional.of(cmd.getOptionValue("o"));
      }
    } catch (ParseException e) {
      new HelpFormatter().printHelp("GolrLoad", options);
      System.exit(-1);
    }

    Injector i = Guice.createInjector(
        new GolrLoaderModule(),
        new Neo4jModule(neo4jConfig),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(GraphAspect.class).to(EvidenceAspect.class);
          }
        });

    GolrLoader loader = i.getInstance(GolrLoader.class);
    File out = null;
    if (outputFile.isPresent()) {
      out = new File(outputFile.get());
    } else {
      out = Files.createTempFile("golr-load", ".json").toFile();
      out.deleteOnExit();
    }
    logger.info("Writing JSON to: " + out.getAbsolutePath());
    try (FileWriter writer = new FileWriter(out)) {
      long recordCount = loader.process(query, writer);
      logger.info("Wrote " + recordCount +  " documents to: " + out.getAbsolutePath());
    }
    logger.info("...done");
    if (solrServer.isPresent()) {
      logger.info("Posting JSON to " + solrServer.get());
      try {
        String result = Request
            .Post(new URI(solrServer.get() +
                          (solrServer.get().endsWith("/") ? "" : "/") + 
                          SOLR_JSON_URL_SUFFIX))
            .bodyFile(out, ContentType.APPLICATION_JSON)
            .execute().returnContent().asString();
        logger.info(result);
      } catch (Exception e) {
        logger.log(Level.SEVERE, "Failed to post JSON", e);
        System.exit(-1);
      }
      logger.info("...done");
    }

  }
}