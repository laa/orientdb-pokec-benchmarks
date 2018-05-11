package com.orientechnologies.pokec;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
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
  private static final String PROFILES_FILE = "soc-pokec-profiles.txt.gz";
  private static final String NULL_STRING   = "null";
  private static final String DB_NAME       = "pokec";

  public static void main(String[] args) throws Exception {
    try (OrientDB orientDB = new OrientDB("plocal:./build/databases", OrientDBConfig.defaultConfig())) {
      if (orientDB.exists(DB_NAME)) {
        orientDB.drop(DB_NAME);
      }

      orientDB.create(DB_NAME, ODatabaseType.PLOCAL);

      generateSchema(orientDB);

      try (ODatabasePool pool = new ODatabasePool(orientDB, DB_NAME, "admin", "admin")) {
        final ArrayBlockingQueue<PokecProfile> profileQueue = new ArrayBlockingQueue<>(256);
        final File profilesFile = new File(PROFILES_FILE);

        final ExecutorService executorService = Executors.newCachedThreadPool();
        final List<Future<Void>> futures = new ArrayList<>();
        final int numThreads = 8;
        for (int i = 0; i < numThreads; i++) {
          futures.add(executorService.submit(new PokecLoader(pool, profileQueue)));
        }

        int profileCounter = 0;
        final long startProfileLoadTs = System.nanoTime();
        final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd kk:mm:ss.n");
        try (FileInputStream fileInputStream = new FileInputStream(profilesFile)) {
          try (GZIPInputStream gzipInputStream = new GZIPInputStream(fileInputStream)) {
            try (InputStreamReader reader = new InputStreamReader(gzipInputStream)) {
              try (BufferedReader bufferedReader = new BufferedReader(reader)) {

                String line;

                while ((line = bufferedReader.readLine()) != null) {
                  final PokecProfile pokecProfile = fillPokecProfile(dateTimeFormatter, line);
                  profileQueue.put(pokecProfile);

                  profileCounter++;
                  if (profileCounter > 0 && profileCounter % 100_000 == 0) {
                    System.out.printf("%d profiles were processed\n", profileCounter);
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

        System.out
            .printf("Load time per profile %d us, throughput %d profiles/s, %d profiles were processed\n", loadTimePerProfileMks,
                profilesPerSecond, profileCounter);

        executorService.shutdown();
      }
    }
  }

  private static void generateSchema(OrientDB orientDB) {
    try (ODatabaseSession databaseSession = orientDB.open(DB_NAME, "admin", "admin")) {
      final OMetadata metadata = databaseSession.getMetadata();
      final OSchema schema = metadata.getSchema();
      final OClass vertex = schema.getClass("V");
      final OClass profile = schema.createClass("Profile", vertex);

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

      profile.createIndex("user_id_index", OClass.INDEX_TYPE.UNIQUE.toString(),
          null, null, "AUTOSHARDING",
          new String[] { "user_id" });
    }
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
