package com.orientechnologies.pokec;

import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.record.OVertex;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;

public class PokecLoader implements Callable<Void> {
  private final ODatabasePool                    pool;
  private final ArrayBlockingQueue<PokecProfile> profileQueue;

  PokecLoader(ODatabasePool pool, ArrayBlockingQueue<PokecProfile> profileQueue) {
    this.pool = pool;
    this.profileQueue = profileQueue;
  }

  @Override
  public Void call() throws Exception {
    try {
      while (true) {
        final PokecProfile pokecProfile = profileQueue.take();
        if (pokecProfile.user_id == -1) {
          return null;
        }

        try (ODatabaseSession session = pool.acquire()) {
          session.begin();
          OVertex vertex = session.newVertex("Profile");
          vertex.setProperty("user_id", pokecProfile.user_id);
          vertex.setProperty("public_profile", pokecProfile.public_profile);
          vertex.setProperty("completion_percentage", pokecProfile.completion_percentage);
          vertex.setProperty("gender", pokecProfile.gender);
          vertex.setProperty("region", pokecProfile.region);
          vertex.setProperty("last_login", pokecProfile.last_login);
          vertex.setProperty("age", pokecProfile.age);
          vertex.setProperty("body", pokecProfile.body);
          vertex.setProperty("i_am_working_in_field", pokecProfile.i_am_working_in_field);
          vertex.setProperty("spoken_languages", pokecProfile.spoken_languages);
          vertex.setProperty("hobbies", pokecProfile.hobbies);
          vertex.setProperty("i_most_enjoy_good_food", pokecProfile.i_most_enjoy_good_food);
          vertex.setProperty("body_type", pokecProfile.body_type);
          vertex.setProperty("my_eyesight", pokecProfile.my_eyesight);
          vertex.setProperty("eye_color", pokecProfile.eye_color);
          vertex.setProperty("hair_color", pokecProfile.hair_color);
          vertex.setProperty("hair_type", pokecProfile.hair_type);
          vertex.setProperty("completed_level_of_education", pokecProfile.completed_level_of_education);
          vertex.setProperty("favourite_color", pokecProfile.favourite_color);
          vertex.setProperty("relation_to_smoking", pokecProfile.relation_to_smoking);
          vertex.setProperty("relation_to_alcohol", pokecProfile.relation_to_alcohol);
          vertex.setProperty("sign_in_zodiac", pokecProfile.sign_in_zodiac);
          vertex.setProperty("on_pokec_i_am_looking_for", pokecProfile.on_pokec_i_am_looking_for);
          vertex.setProperty("love_is_for_me", pokecProfile.love_is_for_me);
          vertex.setProperty("relation_to_casual_sex", pokecProfile.relation_to_casual_sex);
          vertex.setProperty("my_partner_should_be", pokecProfile.my_partner_should_be);
          vertex.setProperty("marital_status", pokecProfile.marital_status);
          vertex.setProperty("children", pokecProfile.children);
          vertex.setProperty("relation_to_children", pokecProfile.relation_to_children);
          vertex.setProperty("i_like_movies", pokecProfile.i_like_movies);
          vertex.setProperty("i_like_watching_movie", pokecProfile.i_like_watching_movie);
          vertex.setProperty("i_like_music", pokecProfile.i_like_music);
          vertex.setProperty("i_mostly_like_listening_to_music", pokecProfile.i_mostly_like_listening_to_music);
          vertex.setProperty("the_idea_of_good_evening", pokecProfile.the_idea_of_good_evening);
          vertex.setProperty("i_like_specialties_from_kitchen", pokecProfile.i_like_specialties_from_kitchen);
          vertex.setProperty("fun", pokecProfile.fun);
          vertex.setProperty("i_am_going_to_concerts", pokecProfile.i_am_going_to_concerts);
          vertex.setProperty("my_active_sports", pokecProfile.my_active_sports);
          vertex.setProperty("my_passive_sports", pokecProfile.my_passive_sports);
          vertex.setProperty("profession", pokecProfile.profession);
          vertex.setProperty("i_like_books", pokecProfile.i_like_books);
          vertex.setProperty("life_style", pokecProfile.life_style);
          vertex.setProperty("music", pokecProfile.music);
          vertex.setProperty("cars", pokecProfile.cars);
          vertex.setProperty("politics", pokecProfile.politics);
          vertex.setProperty("relationships", pokecProfile.relationships);
          vertex.setProperty("art_culture", pokecProfile.art_culture);
          vertex.setProperty("hobbies_interests", pokecProfile.hobbies_interests);
          vertex.setProperty("science_technologies", pokecProfile.science_technologies);
          vertex.setProperty("computers_internet", pokecProfile.computers_internet);
          vertex.setProperty("education", pokecProfile.education);
          vertex.setProperty("sport", pokecProfile.sport);
          vertex.setProperty("movies", pokecProfile.movies);
          vertex.setProperty("travelling", pokecProfile.travelling);
          vertex.setProperty("health", pokecProfile.health);
          vertex.setProperty("companies_brands", pokecProfile.companies_brands);
          vertex.setProperty("more", pokecProfile.more);
          vertex.save();
          session.commit();
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }
}
