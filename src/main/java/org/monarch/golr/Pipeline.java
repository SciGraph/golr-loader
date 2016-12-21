package org.monarch.golr;

import io.scigraph.internal.EvidenceAspect;
import io.scigraph.internal.GraphAspect;
import io.scigraph.neo4j.Neo4jConfiguration;
import io.scigraph.neo4j.Neo4jModule;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.ssl.SSLContext;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.http.client.fluent.Executor;
import org.apache.http.client.fluent.Request;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.TrustStrategy;
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

public class Pipeline {

  private static final Logger logger = Logger.getLogger(Pipeline.class.getName());

  private static ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

  private static final String SOLR_JSON_URL_SUFFIX = "update/json?commit=true";

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
    option = Option.builder("s").longOpt("solr-server").required(false).hasArg()
        .desc("An optional Solr server to update").build();
    options.addOption(option);
    option = Option.builder("o").longOpt("output").required(false).hasArg()
        .desc("An optional output file for the JSON").build();
    options.addOption(option);
    option = Option.builder("onlyupload").longOpt("onlyupload").required(false)
        .desc("To only upload the JSON. The -o  and -s arguments are mandatory with this option.")
        .build();
    options.addOption(option);
    option = Option.builder("d").longOpt("delete-json").required(false)
        .desc("Do not keep the generated json files.").build();
    options.addOption(option);
    return options;
  }

  public static void main(String[] args) throws JsonParseException, JsonMappingException,
      IOException, URISyntaxException, ExecutionException, InterruptedException,
      KeyManagementException, NoSuchAlgorithmException, KeyStoreException {
    Options options = getOptions();
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd;
    Neo4jConfiguration neo4jConfig = null;
    File filePath = null;
    Optional<String> solrServer = Optional.absent();
    Optional<String> outputFolder = Optional.absent();
    boolean onlyUpload = false;
    boolean deleteJson = false;
    try {
      cmd = parser.parse(options, args);
      if (cmd.hasOption("onlyupload")) {
        onlyUpload = true;
      }
      if (cmd.hasOption("delete-json")) {
        deleteJson = true;
      }
      if (cmd.hasOption("s")) {
        solrServer = Optional.of(cmd.getOptionValue("s"));
      }
      if (cmd.hasOption("o")) {
        outputFolder = Optional.of(cmd.getOptionValue("o"));
      }
      if (!onlyUpload) {
        neo4jConfig = mapper.readValue(new File(cmd.getOptionValue("g")), Neo4jConfiguration.class);
        filePath = new File(cmd.getOptionValue("q"));
      }
    } catch (ParseException e) {
      e.printStackTrace();
      new HelpFormatter().printHelp("GolrLoad", options);
      System.exit(-1);
    }


    if (onlyUpload) {
      logger.info("Upload only");
      // ignore ssl certs because letsencrypt is not supported by Oracle yet
      SSLContext sslContext = new SSLContextBuilder().loadTrustMaterial(null, new TrustStrategy() {
        @Override
        public boolean isTrusted(java.security.cert.X509Certificate[] arg0, String arg1)
            throws java.security.cert.CertificateException {
          // TODO Auto-generated method stub
          return true;
        }
      }).build();
      File outputPath = new File(outputFolder.get());
      for (final File fileEntry : outputPath.listFiles()) {
        logger.info("Posting JSON " + fileEntry.getName() + " to " + solrServer.get());
        try {
          CloseableHttpClient httpClient =
              HttpClients.custom().setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                  .setSSLContext(sslContext).build();
          Request request =
              Request.Post(new URI(solrServer.get() + (solrServer.get().endsWith("/") ? "" : "/")
                  + SOLR_JSON_URL_SUFFIX)).bodyFile(fileEntry, ContentType.APPLICATION_JSON);

          Executor executor = Executor.newInstance(httpClient);
          String result = executor.execute(request).returnContent().asString();

          logger.info(result);
        } catch (Exception e) {
          logger.log(Level.SEVERE, "Failed to post JSON " + fileEntry.getName(), e);
        }
      }
    } else {
      Injector i = Guice.createInjector(new GolrLoaderModule(), new Neo4jModule(neo4jConfig),
          new AbstractModule() {
            @Override
            protected void configure() {
              bind(GraphAspect.class).to(EvidenceAspect.class);
            }
          });

      GolrLoader loader = i.getInstance(GolrLoader.class);

      final ExecutorService pool =
          Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
      List<Future<Boolean>> futures = new ArrayList<>();


      for (final File fileEntry : filePath.listFiles()) {
        GolrCypherQuery query = mapper.readValue(fileEntry, GolrCypherQuery.class);
        File outputFile = null;
        if (outputFolder.isPresent()) {
          outputFile = new File(outputFolder.get() + "/" + fileEntry.getName() + ".json");
        } else {
          outputFile = Files.createTempFile("golr-load", ".json").toFile();
          outputFile.deleteOnExit();
        }
        final Future<Boolean> contentFuture = pool.submit(new GolrWorker(solrServer, outputFile,
            loader, query, SOLR_JSON_URL_SUFFIX, SOLR_LOCK, deleteJson));
        futures.add(contentFuture);
      }

      for (Future<Boolean> future : futures) {
        future.get();
      }
      pool.shutdown();
      pool.awaitTermination(10, TimeUnit.DAYS);

    }
    logger.info("Golr load completed");

  }
}
