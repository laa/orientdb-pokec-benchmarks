package com.orientechnologies.pokec;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;

public class PokecRelationsLoader implements Callable<Integer> {
  private final ArrayBlockingQueue<int[]> relationsQueue;
  private final ODatabasePool             pool;

  PokecRelationsLoader(ArrayBlockingQueue<int[]> relationsQueue, ODatabasePool pool) {
    this.relationsQueue = relationsQueue;
    this.pool = pool;
  }

  @Override
  public Integer call() throws Exception {
    int retries = 0;

    while (true) {
      final int[] relation = relationsQueue.take();
      if (relation[0] == -1) {
        return retries;
      }

      try (ODatabaseSession databaseSession = pool.acquire()) {
        while (true) {
          try {
            databaseSession
                .command("create edge from (select from Profile where user_id=?) to (select from Profile where user_id=?)",
                    relation[0], relation[1]).close();
            break;
          } catch (ONeedRetryException e) {
            retries++;
            //continue
          }
        }
      }
    }
  }
}
