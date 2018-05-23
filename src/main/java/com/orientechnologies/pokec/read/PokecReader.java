package com.orientechnologies.pokec.read;

import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.pokec.PokecWorkloadTask;
import com.orientechnologies.pokec.common.ZipfianGenerator;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class PokecReader extends PokecWorkloadTask {
  public PokecReader(int iterationsCount, ODatabasePool pool, ZipfianGenerator zipfianGenerator, int itemsCount,
      AtomicInteger iterationsCounter) {
    super(iterationsCount, pool, zipfianGenerator, itemsCount, iterationsCounter);
  }

  @Override
  public void execute(ODatabaseSession session, OVertex vertex, ZipfianGenerator zipfianGenerator, int itemsCount, Random random) {
    for (String property : vertex.getPropertyNames()) {
      vertex.getProperty(property);
    }
  }
}
