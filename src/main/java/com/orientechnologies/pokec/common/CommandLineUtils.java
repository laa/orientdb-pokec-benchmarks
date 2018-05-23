package com.orientechnologies.pokec.common;

import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.File;

public class CommandLineUtils {
  private static final int DEFAULT_NUM_THREADS = 8;

  private static final String DEFAULT_DB_NAME          = "pokec";
  private static final String DEFAULT_ENGINE_DIRECTORY = "./build/databases";

  private static final String EMBEDDED          = "embedded";
  private static final String ENGINE_DIRECTORY  = "engineDirectory";
  private static final String DB_NAME           = "dbName";
  private static final String REMOTE_URL        = "remoteURL";
  private static final String NUM_THREADS       = "numThreads";
  private static final String INDEX_TYPE        = "indexType";
  private static final String CSV_SUFFIX        = "csvSuffix";
  private static final String WARMUP_OPERATIONS = "warmUpOperations";
  private static final String OPERATIONS        = "operations";

  private static final String TREE_INDEX        = "tree";
  private static final String HASH_INDEX        = "hash";
  private static final String AUTOSHARDED_INDEX = "autosharded";

  public static Options generateCommandLineOptions() {
    Options options = new Options();

    Option embedded = Option.builder(EMBEDDED).argName(EMBEDDED)
        .desc("Whether embedded or remote storage will be used. " + "Embedded is default one.").
            hasArg().required(false).build();
    Option engineDirectory = Option.builder(ENGINE_DIRECTORY).argName(ENGINE_DIRECTORY).desc(
        "Path to the directory where all embedded databases will be stored."
            + " In case if embedded storage have been chosen for benchmarks.").hasArg().required(false).build();
    Option dbName = Option.builder(DB_NAME).argName(DB_NAME).desc("Name of the database which will be used for benchmarks.")
        .hasArg().required(false).build();
    Option csvSuffix = Option.builder(CSV_SUFFIX).argName(CSV_SUFFIX).desc("Suffix which is added to any CSV report.").hasArg()
        .required(false).build();
    Option remoteURL = Option.builder(REMOTE_URL).argName(REMOTE_URL).hasArg().required(false)
        .desc("URL to the remote storage. If remote storage has chosen for benchmarking").build();
    Option numThreadsOpt = Option.builder(NUM_THREADS).argName(NUM_THREADS).desc("Amount of threads to use for benchmark").hasArg().
        required(false).build();
    Option indexType = Option.builder(INDEX_TYPE).argName(INDEX_TYPE).hasArg().required(false).
        desc("Type of index is used in pokec benchmark, possible values are: " + TREE_INDEX + ", " + HASH_INDEX + ", "
            + AUTOSHARDED_INDEX + ". " + AUTOSHARDED_INDEX + " is used by default").build();
    Option warmUpOperations = Option.builder(WARMUP_OPERATIONS).argName(WARMUP_OPERATIONS)
        .desc("Amount of operations to be executed during warmup").hasArg().
            required(false).build();
    Option operations = Option.builder(OPERATIONS).argName(OPERATIONS).desc("Amount of operations to be executed during workload")
        .hasArg().
            required(false).build();

    options.addOption(embedded);
    options.addOption(engineDirectory);
    options.addOption(dbName);
    options.addOption(remoteURL);
    options.addOption(numThreadsOpt);
    options.addOption(indexType);
    options.addOption(csvSuffix);
    options.addOption(warmUpOperations);
    options.addOption(operations);

    return options;
  }

  public static String getCsvSuffix(CommandLine cmd) {
    if (cmd.hasOption(CSV_SUFFIX)) {
      return cmd.getOptionValue(CSV_SUFFIX);
    }

    return "";
  }

  public static int getWarmUpOperations(CommandLine cmd, int profilesCount) {
    if (cmd.hasOption(WARMUP_OPERATIONS)) {
      return Integer.parseInt(cmd.getOptionValue(WARMUP_OPERATIONS));
    }

    return 2 * profilesCount;
  }

  public static int getOperations(CommandLine cmd, int profilesCount) {
    if (cmd.hasOption(OPERATIONS)) {
      return Integer.parseInt(cmd.getOptionValue(OPERATIONS));
    }

    return 4 * profilesCount;
  }

  public static OrientDB createOrientDBInstance(CommandLine cmd) {
    if (cmd.hasOption(EMBEDDED)) {
      String embeddedValue = cmd.getOptionValue(EMBEDDED);
      if (Boolean.parseBoolean(embeddedValue)) {
        return createEmbedded(cmd);
      } else {
        if (cmd.hasOption(REMOTE_URL)) {
          return new OrientDB(cmd.getOptionValue(REMOTE_URL), OrientDBConfig.defaultConfig());
        } else {
          throw new IllegalArgumentException("URL which is pointing to remote database should be provided");
        }
      }
    } else {
      return createEmbedded(cmd);
    }
  }

  public static boolean isEmbedded(CommandLine cmd) {
    if (cmd.hasOption(EMBEDDED)) {
      final String embeddedValue = cmd.getOptionValue(EMBEDDED);
      return Boolean.parseBoolean(embeddedValue);
    } else {
      return true;
    }
  }

  public static String path(CommandLine cmd) {
    if (cmd.hasOption(EMBEDDED)) {
      String embeddedValue = cmd.getOptionValue(EMBEDDED);
      if (Boolean.parseBoolean(embeddedValue)) {
        return getEmbeddedPath(cmd);
      } else {
        return cmd.getOptionValue(REMOTE_URL);
      }
    } else {
      return getEmbeddedPath(cmd);
    }
  }

  public static int numThreads(CommandLine cmd) {
    if (cmd.hasOption(NUM_THREADS)) {
      String numThreadsValue = cmd.getOptionValue(NUM_THREADS);
      return Integer.parseInt(numThreadsValue);
    }

    return DEFAULT_NUM_THREADS;
  }

  public static String dbName(CommandLine cmd) {
    String dbName;
    if (cmd.hasOption(DB_NAME)) {
      dbName = cmd.getOptionValue(DB_NAME);
    } else {
      dbName = DEFAULT_DB_NAME;
    }

    return dbName;
  }

  public static OClass.INDEX_TYPE getIndexType(CommandLine cmd) {
    if (cmd.hasOption(INDEX_TYPE)) {
      final String indexTypeValue = cmd.getOptionValue(INDEX_TYPE);
      switch (indexTypeValue) {
      case TREE_INDEX:
        return OClass.INDEX_TYPE.UNIQUE;
      case HASH_INDEX:
        return OClass.INDEX_TYPE.UNIQUE_HASH_INDEX;
      case AUTOSHARDED_INDEX:
        return OClass.INDEX_TYPE.UNIQUE;
      default:
        throw new IllegalArgumentException("Invalid index type " + indexTypeValue);
      }
    } else {
      return OClass.INDEX_TYPE.UNIQUE;
    }
  }

  public static boolean isAutosharded(CommandLine cmd) {
    if (cmd.hasOption(INDEX_TYPE)) {
      final String indexTypeValue = cmd.getOptionValue(INDEX_TYPE);
      switch (indexTypeValue) {
      case TREE_INDEX:
        return false;
      case HASH_INDEX:
        return false;
      case AUTOSHARDED_INDEX:
        return true;
      default:
        throw new IllegalArgumentException("Invalid index type '" + indexTypeValue + "'");
      }
    } else {
      return true;
    }
  }

  private static String getEmbeddedPath(CommandLine cmd) {
    String embeddedDirectory;
    if (cmd.hasOption(ENGINE_DIRECTORY)) {
      embeddedDirectory = cmd.getOptionValue(ENGINE_DIRECTORY);
    } else {
      embeddedDirectory = DEFAULT_ENGINE_DIRECTORY;
    }

    String dbName;
    if (cmd.hasOption(DB_NAME)) {
      dbName = cmd.getOptionValue(DB_NAME);
    } else {
      dbName = DEFAULT_DB_NAME;
    }

    return embeddedDirectory + File.separator + dbName;
  }

  private static OrientDB createEmbedded(CommandLine cmd) {
    String embeddedDirectory;
    if (cmd.hasOption(ENGINE_DIRECTORY)) {
      embeddedDirectory = cmd.getOptionValue(ENGINE_DIRECTORY);
    } else {
      embeddedDirectory = DEFAULT_ENGINE_DIRECTORY;
    }

    return new OrientDB("plocal:" + embeddedDirectory, OrientDBConfig.defaultConfig());
  }
}
