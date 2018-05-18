package com.orientechnologies.pokec.edgesupdate;

import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.pokec.PokecWorkload;
import com.orientechnologies.pokec.PokecWorkloadTask;
import com.orientechnologies.pokec.common.ZipfianGenerator;

import java.util.concurrent.atomic.AtomicInteger;

public class PokecEdgesAdd extends PokecWorkload {
  public static void main(String[] args) throws Exception {
    new PokecEdgesAdd().run();
  }

  @Override
  public PokecWorkloadTask createTask(int iterationsCount, ODatabasePool pool, ZipfianGenerator zipfianGenerator, int itemsCount,
      AtomicInteger iterationsCounter) {
    return new PokecEdgesAdder(iterationsCount, pool, zipfianGenerator, itemsCount, iterationsCounter);
  }
}
