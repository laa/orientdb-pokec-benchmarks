package com.orientechnologies.pokec;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.pokec.common.KeyGenerator;
import com.orientechnologies.pokec.common.ZipfianGenerator;

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class PokecWorkloadTask implements Callable<Integer> {
  private final int              iterationsCount;
  private final ODatabasePool    pool;
  private final ZipfianGenerator zipfianGenerator;
  private final int              itemsCount;
  private final AtomicInteger    iterationsCounter;

  public PokecWorkloadTask(int iterationsCount, ODatabasePool pool, ZipfianGenerator zipfianGenerator, int itemsCount,
      AtomicInteger iterationsCounter) {
    this.iterationsCount = iterationsCount;
    this.pool = pool;
    this.zipfianGenerator = zipfianGenerator;
    this.itemsCount = itemsCount;
    this.iterationsCounter = iterationsCounter;
  }

  @Override
  public Integer call() {
    final ThreadLocalRandom random = ThreadLocalRandom.current();
    int retries = 0;

    try {
      for (int i = 0; i < iterationsCount; i++) {
        try (ODatabaseSession session = pool.acquire()) {
          final String key = KeyGenerator.generateKey(zipfianGenerator, itemsCount);

          final OVertex vertex;
          try (OResultSet resultSet = session.query("select from Profile where key = ?", key)) {
            final OResult result = resultSet.next();
            vertex = result.getVertex().orElseThrow(IllegalStateException::new);
          }

          while (true) {
            session.begin();
            try {
              execute(session, vertex, zipfianGenerator, itemsCount, random);
              session.commit();
              break;
            } catch (ONeedRetryException e) {
              retries++;
            }
          }

          iterationsCounter.incrementAndGet();
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }

    return retries;
  }

  public abstract void execute(ODatabaseSession session, OVertex vertex, ZipfianGenerator zipfianGenerator, int itemsCount,
      Random random);
}
