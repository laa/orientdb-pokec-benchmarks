package com.orientechnologies.pokec.edgesupdate;

import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.pokec.PokecWorkloadTask;
import com.orientechnologies.pokec.common.KeyGenerator;
import com.orientechnologies.pokec.common.ZipfianGenerator;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class PokecEdgesAdder extends PokecWorkloadTask {

  public PokecEdgesAdder(int iterationsCount, ODatabasePool pool, ZipfianGenerator zipfianGenerator, int itemsCount,
      AtomicInteger iterationsCounter) {
    super(iterationsCount, pool, zipfianGenerator, itemsCount, iterationsCounter);
  }

  @Override
  public void execute(ODatabaseSession session, OVertex vertex, ZipfianGenerator zipfianGenerator, int itemsCount, Random random) {
    final String keyToAdd = KeyGenerator.generateKey(zipfianGenerator, itemsCount);
    final OVertex vertexToAdd;

    try (OResultSet resultSet = session.query("select from Profile where key = ?", keyToAdd)) {
      final OResult result = resultSet.next();
      vertexToAdd = result.getVertex().orElseThrow(IllegalStateException::new);
    }

    vertex.addEdge(vertexToAdd);
    vertex.save();
  }
}
