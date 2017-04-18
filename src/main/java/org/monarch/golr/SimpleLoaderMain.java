package org.monarch.golr;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.base.Stopwatch;
import com.google.inject.Guice;
import com.google.inject.Injector;

import io.scigraph.neo4j.Neo4jConfiguration;
import io.scigraph.neo4j.Neo4jModule;


public class SimpleLoaderMain {

  private static ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

  static {
    mapper.registerModules(new GuavaModule());
  }

  public static Options getOptions() {
    Options options = new Options();
    Option option =
        Option.builder("g").longOpt("graph").required().hasArg()
            .desc("The Neo4j graph configuration").build();
    options.addOption(option);
    option =
        Option.builder("o").longOpt("output").required(false).hasArg()
            .desc("An optional output file for the JSON").build();
    options.addOption(option);
    return options;
  }

  public static void main(String[] args) throws JsonParseException, JsonMappingException,
      IOException {
    Options options = getOptions();
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd;
    Neo4jConfiguration neo4jConfig = null;
    Optional<String> outputFile = Optional.empty();
    try {
      cmd = parser.parse(options, args);
      if (cmd.hasOption("o")) {
        outputFile = Optional.of(cmd.getOptionValue("o"));
      }
      neo4jConfig = mapper.readValue(new File(cmd.getOptionValue("g")), Neo4jConfiguration.class);
    } catch (ParseException e) {
      e.printStackTrace();
      new HelpFormatter().printHelp("SimpleLoader", options);
      System.exit(-1);
    }

    Injector i = Guice.createInjector(new SimpleLoaderModule(), new Neo4jModule(neo4jConfig));
    SimpleLoader loader = i.getInstance(SimpleLoader.class);

    Stopwatch sw = Stopwatch.createStarted();
    loader.generate(outputFile);
    System.out.println("Completed in " + sw.stop());

  }
}
