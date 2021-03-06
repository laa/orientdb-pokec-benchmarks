package com.orientechnologies.pokec.load;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.pokec.common.CommandLineUtils;
import com.orientechnologies.pokec.common.FNVHash;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.GZIPInputStream;

public class PokecLoad {
  private static final long NANOS_IN_HOURS   = 1_000_000_000L * 60 * 60;
  private static final long NANOS_IN_MINUTES = 1_000_000_000L * 60;
  private static final long NANOS_IN_SECONDS = 1_000_000_000L;

  private static final String DEFAULT_PROFILES_FILE  = "soc-pokec-profiles.txt.gz";
  private static final String DEFAULT_RELATIONS_FILE = "soc-pokec-relationships.txt.gz";

  private static final String NULL_STRING = "null";

  public static final String PROFILE_CLASS = "Profile";

  public static final String[] DATA_FIELDS = { "body", "i_am_working_in_field", "spoken_languages", "hobbies",
      "i_most_enjoy_good_food", "pets", "body_type", "my_eyesight", "eye_color", "hair_color", "hair_type",
      "completed_level_of_education", "favourite_color", "relation_to_smoking", "relation_to_alcohol", "sign_in_zodiac",
      "on_pokec_i_am_looking_for", "love_is_for_me", "relation_to_casual_sex", "my_partner_should_be", "marital_status", "children",
      "relation_to_children", "i_like_movies", "i_like_watching_movie", "i_like_music", "i_mostly_like_listening_to_music",
      "the_idea_of_good_evening", "i_like_specialties_from_kitchen", "fun", "i_am_going_to_concerts", "my_active_sports",
      "my_passive_sports", "profession", "i_like_books", "life_style", "music", "cars", "politics", "relationships", "art_culture",
      "hobbies_interests", "science_technologies", "computers_internet", "education", "sport", "movies", "travelling", "health",
      "companies_brands", "more" };

  public static void main(String[] args) throws Exception {
    Options options = CommandLineUtils.generateCommandLineOptions();

    CommandLineParser parser = new DefaultParser();
    try {
      CommandLine cmd = parser.parse(options, args);

      final String profileStatistics;
      final String relationStatistics;

      try (OrientDB orientDB = CommandLineUtils.createOrientDBInstance(cmd)) {
        final String path = CommandLineUtils.path(cmd);

        System.out.printf("Starting of load of data of pokec database %s\n", path);
        final boolean embedded = CommandLineUtils.isEmbedded(cmd);

        final String dbName = CommandLineUtils.dbName(cmd);

        if (embedded) {
          OFileUtils.deleteRecursively(new File(path));
          orientDB.create(dbName, ODatabaseType.PLOCAL);
        }

        final boolean isAutosharded = CommandLineUtils.isAutosharded(cmd);
        final OClass.INDEX_TYPE indexType = CommandLineUtils.getIndexType(cmd);

        if (isAutosharded) {
          System.out.println("Autosharded index will be used for indexing of DB keys");
        } else {
          System.out.printf("%s index will be used for indexing of keys\n", indexType.toString());
        }

        generateSchema(orientDB, path, dbName, isAutosharded, indexType);

        final ExecutorService executorService = Executors.newCachedThreadPool();

        final int numThreads = CommandLineUtils.numThreads(cmd);
        System.out.printf("%d threads will be used for data load\n", numThreads);

        final String csvSuffix = CommandLineUtils.getCsvSuffix(cmd);
        try (ODatabasePool pool = new ODatabasePool(orientDB, dbName, "admin", "admin")) {
          profileStatistics = loadProfiles(executorService, pool, path, numThreads, csvSuffix);
          relationStatistics = loadRelations(executorService, pool, path, numThreads, csvSuffix);

          executorService.shutdown();
        }

        System.out.printf("Load of data of pokec database into %s is completed\n", path);
        System.out.println("Following settings were used:");
        System.out.printf("Number of threads : %d \n", numThreads);
        if (isAutosharded) {
          System.out.println("Autosharded index was used for indexing of keys");
        } else {
          System.out.printf("%s index was used for indexing of keys\n", indexType.toString());
        }
        System.out.println("Load statistics:");
        System.out.print(profileStatistics);
        System.out.print(relationStatistics);
      }
    } catch (ParseException pe) {
      System.out.println(pe.getMessage());
    }

  }

  private static String loadRelations(ExecutorService executorService, ODatabasePool pool, String path,
      int numThreads, String csvSuffix)
      throws IOException, InterruptedException, java.util.concurrent.ExecutionException {
    System.out.printf("Start loading of relations for %s database\n", path);
    final File relationsFile = new File(DEFAULT_RELATIONS_FILE);

    final List<Future<Integer>> futures = new ArrayList<>();

    @SuppressWarnings("unchecked")
    ArrayBlockingQueue<int[]>[] relationsQueues = new ArrayBlockingQueue[numThreads];
    for (int i = 0; i < numThreads; i++) {
      final ArrayBlockingQueue<int[]> queue = new ArrayBlockingQueue<>(10 * 1024);
      futures.add(executorService.submit(new PokecRelationsLoader(queue, pool)));
      relationsQueues[i] = queue;
    }

    try (FileWriter csvWriter = new FileWriter(String.format("relationsLoad %tc%s.csv", new Date(), csvSuffix))) {
      try (CSVPrinter csvPrinter = new CSVPrinter(csvWriter, CSVFormat.DEFAULT)) {
        int relationCounter = 0;
        final long startRelationLoadTs = System.nanoTime();
        long ts = startRelationLoadTs;
        try (FileInputStream fileInputStream = new FileInputStream(relationsFile)) {
          try (GZIPInputStream gzipInputStream = new GZIPInputStream(fileInputStream)) {
            try (InputStreamReader reader = new InputStreamReader(gzipInputStream)) {
              try (BufferedReader bufferedReader = new BufferedReader(reader)) {
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                  final String[] relation = line.split("\\t");
                  final int[] fromTo = new int[2];

                  fromTo[0] = Integer.parseInt(relation[0]);
                  fromTo[1] = Integer.parseInt(relation[1]);

                  final int queueIndex = fromTo[0] % numThreads;
                  final ArrayBlockingQueue<int[]> queue = relationsQueues[queueIndex];
                  queue.put(fromTo);

                  relationCounter++;

                  if (relationCounter > 0 && relationCounter % 100_000 == 0) {
                    final long currentTimeStamp = System.nanoTime();
                    final long timePassed = currentTimeStamp - ts;
                    ts = currentTimeStamp;

                    final long timePerItem = timePassed / 100_000;
                    final long timePerItemMks = timePerItem / 1_000;
                    final long itemsPerSecond = 1_000_000_000 / timePerItem;

                    System.out
                        .printf("%d relations were processed, avg. insertion time %d us, throughput %d rel/s\n", relationCounter,
                            timePerItemMks, itemsPerSecond);
                    csvPrinter.printRecord(relationCounter, timePerItemMks, itemsPerSecond);
                  }
                }
              }
            }
          }
        }

        for (int i = 0; i < numThreads; i++) {
          relationsQueues[i].put(new int[] { -1, -1 });
        }

        int retries = 0;
        for (Future<Integer> future : futures) {
          retries += future.get();
        }

        final long endRelationLoadTs = System.nanoTime();
        final long relationLoadTime = endRelationLoadTs - startRelationLoadTs;

        final long loadTimePerRelation = relationLoadTime / relationCounter;
        final long relationsPerSecond = 1_000_000_000 / loadTimePerRelation;
        final long loadTimePerRelationMks = loadTimePerRelation / 1000;

        final long hours = relationLoadTime / NANOS_IN_HOURS;
        final long minutes = (relationLoadTime - hours * NANOS_IN_HOURS) / NANOS_IN_MINUTES;
        final long seconds = (relationLoadTime - hours * NANOS_IN_HOURS - minutes * NANOS_IN_MINUTES) / NANOS_IN_SECONDS;

        csvPrinter.printRecord(relationCounter, loadTimePerRelationMks, relationsPerSecond);
        System.out
            .printf("Loading of relations for %s database is completed in %d h. %d m. %d s.\n", path, hours, minutes, seconds);

        String statistics = String
            .format("Load time per relation %d us, throughput %d relation/s, %d relations were processed, %d retries were done\n",
                loadTimePerRelationMks, relationsPerSecond, relationCounter, retries);
        System.out.print(statistics);

        return statistics;
      }
    }
  }

  private static String loadProfiles(ExecutorService executorService, ODatabasePool pool, String path,
      int numThreads, String csvSuffix)
      throws IOException, InterruptedException, java.util.concurrent.ExecutionException {
    System.out.printf("Start loading of profiles for %s database\n", path);

    final ArrayBlockingQueue<PokecProfile> profileQueue = new ArrayBlockingQueue<>(256);
    final File profilesFile = new File(DEFAULT_PROFILES_FILE);

    final List<Future<Void>> futures = new ArrayList<>();
    for (int i = 0; i < numThreads; i++) {
      futures.add(executorService.submit(new PokecProfileLoader(pool, profileQueue)));
    }

    try (FileWriter csvWriter = new FileWriter(String.format("profileLoad %tc%s.csv", new Date(), csvSuffix))) {
      try (CSVPrinter csvPrinter = new CSVPrinter(csvWriter, CSVFormat.DEFAULT)) {
        int profileCounter = 0;
        final long startProfileLoadTs = System.nanoTime();
        long ts = startProfileLoadTs;
        final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd kk:mm:ss.n");
        try (FileInputStream fileInputStream = new FileInputStream(profilesFile)) {
          try (GZIPInputStream gzipInputStream = new GZIPInputStream(fileInputStream)) {
            try (InputStreamReader reader = new InputStreamReader(gzipInputStream)) {
              try (BufferedReader bufferedReader = new BufferedReader(reader)) {

                String line;

                while ((line = bufferedReader.readLine()) != null) {
                  final PokecProfile pokecProfile = fillPokecProfile(dateTimeFormatter, line);
                  pokecProfile.key = "key" + FNVHash.FNVhash64(profileCounter);
                  profileQueue.put(pokecProfile);

                  profileCounter++;
                  if (profileCounter > 0 && profileCounter % 100_000 == 0) {
                    final long currentTimeStamp = System.nanoTime();
                    final long timePassed = currentTimeStamp - ts;
                    ts = currentTimeStamp;

                    final long timePerItem = timePassed / 100_000;
                    final long timePerItemMks = timePerItem / 1_000;
                    final long itemsPerSecond = 1_000_000_000 / timePerItem;

                    System.out
                        .printf("%d profiles were processed, avg. insertion time %d us, throughput %d profiles/s\n", profileCounter,
                            timePerItemMks, itemsPerSecond);
                    csvPrinter.printRecord(profileCounter, timePerItemMks, itemsPerSecond);
                  }
                }
              }
            }
          }

        }

        final PokecProfile end = new PokecProfile();
        end.user_id = -1;
        for (int i = 0; i < numThreads; i++) {
          profileQueue.put(end);
        }

        for (Future<Void> future : futures) {
          future.get();
        }
        final long endProfileLoadTs = System.nanoTime();
        final long profileLoadTime = endProfileLoadTs - startProfileLoadTs;
        final long loadTimePerProfile = profileLoadTime / profileCounter;
        final long profilesPerSecond = 1_000_000_000 / loadTimePerProfile;
        final long loadTimePerProfileMks = loadTimePerProfile / 1000;

        final long hours = profileLoadTime / NANOS_IN_HOURS;
        final long minutes = (profileLoadTime - hours * NANOS_IN_HOURS) / NANOS_IN_MINUTES;
        final long seconds = (profileLoadTime - hours * NANOS_IN_HOURS - minutes * NANOS_IN_MINUTES) / NANOS_IN_SECONDS;

        csvPrinter.printRecord(profileCounter, loadTimePerProfileMks, profilesPerSecond);
        System.out
            .printf("Start loading of profiles for %s database is completed in %d h. %d m. %d s.\n", path, hours, minutes, seconds);
        String statistics = String
            .format("Load time per profile %d us, throughput %d profiles/s, %d profiles were processed\n", loadTimePerProfileMks,
                profilesPerSecond, profileCounter);
        System.out.print(statistics);

        return statistics;
      }
    }
  }

  private static void generateSchema(OrientDB orientDB, String path, String dbName, boolean isAutosharded,
      OClass.INDEX_TYPE indexType) {
    System.out.printf("Start schema generation for %s database\n", path);

    try (ODatabaseSession databaseSession = orientDB.open(dbName, "admin", "admin")) {
      final OMetadata metadata = databaseSession.getMetadata();
      final OSchema schema = metadata.getSchema();
      final OClass vertex = schema.getClass("V");
      final OClass profile = schema.createClass("Profile", vertex);

      profile.createProperty("key", OType.STRING);

      profile.createProperty("user_id", OType.INTEGER);
      profile.createProperty("public_profile", OType.BOOLEAN);
      profile.createProperty("completion_percentage", OType.INTEGER);
      profile.createProperty("gender", OType.BOOLEAN);
      profile.createProperty("region", OType.STRING);
      profile.createProperty("last_login", OType.DATETIME);
      profile.createProperty("registration", OType.DATETIME);
      profile.createProperty("age", OType.INTEGER);

      profile.createProperty("body", OType.STRING);
      profile.createProperty("i_am_working_in_field", OType.STRING);
      profile.createProperty("spoken_languages", OType.STRING);
      profile.createProperty("hobbies", OType.STRING);
      profile.createProperty("i_most_enjoy_good_food", OType.STRING);
      profile.createProperty("pets", OType.STRING);
      profile.createProperty("body_type", OType.STRING);
      profile.createProperty("my_eyesight", OType.STRING);
      profile.createProperty("eye_color", OType.STRING);
      profile.createProperty("hair_color", OType.STRING);
      profile.createProperty("hair_type", OType.STRING);
      profile.createProperty("completed_level_of_education", OType.STRING);
      profile.createProperty("favourite_color", OType.STRING);
      profile.createProperty("relation_to_smoking", OType.STRING);
      profile.createProperty("relation_to_alcohol", OType.STRING);
      profile.createProperty("sign_in_zodiac", OType.STRING);
      profile.createProperty("on_pokec_i_am_looking_for", OType.STRING);
      profile.createProperty("love_is_for_me", OType.STRING);
      profile.createProperty("relation_to_casual_sex", OType.STRING);
      profile.createProperty("my_partner_should_be", OType.STRING);
      profile.createProperty("marital_status", OType.STRING);
      profile.createProperty("children", OType.STRING);
      profile.createProperty("relation_to_children", OType.STRING);
      profile.createProperty("i_like_movies", OType.STRING);
      profile.createProperty("i_like_watching_movie", OType.STRING);
      profile.createProperty("i_like_music", OType.STRING);
      profile.createProperty("i_mostly_like_listening_to_music", OType.STRING);
      profile.createProperty("the_idea_of_good_evening", OType.STRING);
      profile.createProperty("i_like_specialties_from_kitchen", OType.STRING);
      profile.createProperty("fun", OType.STRING);
      profile.createProperty("i_am_going_to_concerts", OType.STRING);
      profile.createProperty("my_active_sports", OType.STRING);
      profile.createProperty("my_passive_sports", OType.STRING);
      profile.createProperty("profession", OType.STRING);
      profile.createProperty("i_like_books", OType.STRING);
      profile.createProperty("life_style", OType.STRING);
      profile.createProperty("music", OType.STRING);
      profile.createProperty("cars", OType.STRING);
      profile.createProperty("politics", OType.STRING);
      profile.createProperty("relationships", OType.STRING);
      profile.createProperty("art_culture", OType.STRING);
      profile.createProperty("hobbies_interests", OType.STRING);
      profile.createProperty("science_technologies", OType.STRING);
      profile.createProperty("computers_internet", OType.STRING);
      profile.createProperty("education", OType.STRING);
      profile.createProperty("sport", OType.STRING);
      profile.createProperty("movies", OType.STRING);
      profile.createProperty("travelling", OType.STRING);
      profile.createProperty("health", OType.STRING);
      profile.createProperty("companies_brands", OType.STRING);
      profile.createProperty("more", OType.STRING);

      if (isAutosharded) {
        profile.createIndex("user_id_index", OClass.INDEX_TYPE.UNIQUE.toString(), null, null, "AUTOSHARDING",
            new String[] { "user_id" });

        profile.createIndex("key_index", OClass.INDEX_TYPE.UNIQUE.toString(), null, null, "AUTOSHARDING", new String[] { "key" });
      } else {
        profile.createIndex("user_id_index", indexType, "user_id");
        profile.createIndex("key_index", indexType, "key");
      }
    }

    System.out.printf("Start schema generation for %s database is completed\n", path);
  }

  private static PokecProfile fillPokecProfile(DateTimeFormatter dateTimeFormatter, String line) {
    final String[] fields = line.split("\\t");
    final PokecProfile pokecProfile = new PokecProfile();
    pokecProfile.user_id = Integer.parseInt(fields[0]);
    pokecProfile.public_profile = Integer.parseInt(fields[1]) == 1;
    pokecProfile.completion_percentage = Integer.parseInt(fields[2]);

    if (!fields[3].equals(NULL_STRING)) {
      pokecProfile.gender = Integer.parseInt(fields[3]) == 1;
    }

    pokecProfile.region = fields[4];

    if (!fields[5].equals(NULL_STRING)) {
      pokecProfile.last_login = convertToDateTime(fields[5], dateTimeFormatter);
    }

    if (!fields[6].equals(NULL_STRING)) {
      pokecProfile.registration = convertToDateTime(fields[6], dateTimeFormatter);
    }

    if (!fields[7].equals(NULL_STRING)) {
      pokecProfile.age = Integer.parseInt(fields[7]);
    }

    pokecProfile.body = processString(fields[8]);
    pokecProfile.i_am_working_in_field = processString(fields[9]);
    pokecProfile.spoken_languages = processString(fields[10]);
    pokecProfile.hobbies = processString(fields[11]);
    pokecProfile.i_most_enjoy_good_food = processString(fields[12]);
    pokecProfile.pets = processString(fields[13]);
    pokecProfile.body_type = processString(fields[14]);
    pokecProfile.my_eyesight = processString(fields[15]);
    pokecProfile.eye_color = processString(fields[16]);
    pokecProfile.hair_color = processString(fields[17]);
    pokecProfile.hair_type = processString(fields[18]);
    pokecProfile.completed_level_of_education = processString(fields[19]);
    pokecProfile.favourite_color = processString(fields[20]);
    pokecProfile.relation_to_smoking = processString(fields[21]);
    pokecProfile.relation_to_alcohol = processString(fields[22]);
    pokecProfile.sign_in_zodiac = processString(fields[23]);
    pokecProfile.on_pokec_i_am_looking_for = processString(fields[24]);
    pokecProfile.love_is_for_me = processString(fields[25]);
    pokecProfile.relation_to_casual_sex = processString(fields[26]);
    pokecProfile.my_partner_should_be = processString(fields[27]);
    pokecProfile.marital_status = processString(fields[28]);
    pokecProfile.children = processString(fields[29]);
    pokecProfile.relation_to_children = processString(fields[30]);
    pokecProfile.i_like_movies = processString(fields[31]);
    pokecProfile.i_like_watching_movie = processString(fields[32]);
    pokecProfile.i_like_music = processString(fields[33]);
    pokecProfile.i_mostly_like_listening_to_music = processString(fields[34]);
    pokecProfile.the_idea_of_good_evening = processString(fields[35]);
    pokecProfile.i_like_specialties_from_kitchen = processString(fields[36]);
    pokecProfile.fun = processString(fields[37]);
    pokecProfile.i_am_going_to_concerts = processString(fields[38]);
    pokecProfile.my_active_sports = processString(fields[39]);
    pokecProfile.my_passive_sports = processString(fields[40]);
    pokecProfile.profession = processString(fields[41]);
    pokecProfile.i_like_books = processString(fields[42]);
    pokecProfile.life_style = processString(fields[43]);
    pokecProfile.music = processString(fields[44]);
    pokecProfile.cars = processString(fields[45]);
    pokecProfile.politics = processString(fields[46]);
    pokecProfile.relationships = processString(fields[47]);
    pokecProfile.art_culture = processString(fields[48]);
    pokecProfile.hobbies_interests = processString(fields[49]);
    pokecProfile.science_technologies = processString(fields[50]);
    pokecProfile.computers_internet = processString(fields[51]);
    pokecProfile.education = processString(fields[52]);
    pokecProfile.sport = processString(fields[53]);
    pokecProfile.movies = processString(fields[54]);
    pokecProfile.travelling = processString(fields[55]);
    pokecProfile.health = processString(fields[56]);
    pokecProfile.companies_brands = processString(fields[57]);
    pokecProfile.more = processString(fields[58]);
    return pokecProfile;
  }

  private static Date convertToDateTime(String value, DateTimeFormatter dateTimeFormatter) {
    final LocalDateTime localDate = LocalDateTime.from(dateTimeFormatter.parse(value));
    return Date.from(localDate.atZone(ZoneId.systemDefault()).toInstant());
  }

  private static String processString(String value) {
    if (value.equals(NULL_STRING)) {
      return null;
    }

    return value;
  }
}
