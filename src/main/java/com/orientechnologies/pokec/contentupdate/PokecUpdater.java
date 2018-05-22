package com.orientechnologies.pokec.contentupdate;

import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.pokec.PokecWorkloadTask;
import com.orientechnologies.pokec.common.ZipfianGenerator;
import com.orientechnologies.pokec.load.PokecLoad;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class PokecUpdater extends PokecWorkloadTask {
  PokecUpdater(int iterationsCount, ODatabasePool pool, ZipfianGenerator zipfianGenerator, int itemsCount,
      AtomicInteger iterationsCounter) {
    super(iterationsCount, pool, zipfianGenerator, itemsCount, iterationsCounter);
  }

  @Override
  public void execute(ODatabaseSession session, OVertex vertex, ZipfianGenerator zipfianGenerator, int itemsCount, Random random) {
    final int fieldsCount = random.nextInt(3) + 1;

    for (int n = 0; n < fieldsCount; n++) {
      final int fieldIndex = random.nextInt(PokecLoad.DATA_FIELDS.length);
      final String field = PokecLoad.DATA_FIELDS[fieldIndex];

      final int fieldLen = random.nextInt(500) + 1;
      final byte[] rawValue = new byte[fieldLen];
      random.nextBytes(rawValue);

      final String value = new String(rawValue);
      vertex.setProperty(field, value);
    }

    vertex.save();
  }
}
