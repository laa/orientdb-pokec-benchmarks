package com.orientechnologies.pokec;

import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.pokec.common.ZipfianGenerator;
import com.orientechnologies.pokec.load.PokecLoad;
import com.orientechnologies.pokec.read.PokecReader;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static com.orientechnologies.pokec.load.PokecLoad.DEFAULT_DB_NAME;

public abstract class PokecWorkload {
  private static final long NANOS_IN_HOURS   = 1_000_000_000L * 60 * 60;
  private static final long NANOS_IN_MINUTES = 1_000_000_000L * 60;
  private static final long NANOS_IN_SECONDS = 1_000_000_000L * 60;

  protected void run() throws Exception {
    final long profilesCount;

    try (OrientDB orientDB = new OrientDB(PokecLoad.DEFAULT_DB_URL, OrientDBConfig.defaultConfig())) {
      System.out.println("Opening " + DEFAULT_DB_NAME + " database");
      try (ODatabaseSession databaseSession = orientDB.open(DEFAULT_DB_NAME, "admin", "admin")) {
        profilesCount = databaseSession.countClass(PokecLoad.PROFILE_CLASS);
      }
      System.out.printf("%d profiles were detected \n", profilesCount);

      final ZipfianGenerator generator = new ZipfianGenerator(profilesCount);
      final int numThreads = 8;

      final long iterationsPerThread = profilesCount / numThreads;

      final ExecutorService executorService = Executors.newCachedThreadPool();
      warmUp((int) profilesCount, orientDB, generator, numThreads, iterationsPerThread, executorService);
      workload((int) profilesCount, orientDB, generator, numThreads, iterationsPerThread, executorService);
    }
  }

  public abstract PokecWorkloadTask createTask(int iterationsCount, ODatabasePool pool, ZipfianGenerator zipfianGenerator,
      int itemsCount, AtomicInteger iterationsCounter);

  private void workload(int profilesCount, OrientDB orientDB, ZipfianGenerator generator, int numThreads, long iterationsPerThread,
      ExecutorService executorService) throws Exception {
    List<Future<Integer>> futures = new ArrayList<>();

    try (ODatabasePool pool = new ODatabasePool(orientDB, DEFAULT_DB_NAME, "admin", "admin")) {
      System.out.printf("Starting of workload with %d threads, %d iterations for each thread\n", numThreads, iterationsPerThread);
      final AtomicInteger iterationsCounter = new AtomicInteger();

      Timer statusTimer = new Timer();
      statusTimer.scheduleAtFixedRate(new TimerTask() {
        private long ts = -1;
        private long iterationsCount;

        @Override
        public void run() {
          if (ts == -1) {
            ts = System.nanoTime();
            iterationsCount = iterationsCounter.get();
          } else {
            long currentTs = System.nanoTime();
            long currentIterations = iterationsCounter.get();

            long timePassed = currentTs - ts;
            long iterationsPassed = currentIterations - iterationsCount;

            ts = currentTs;
            iterationsCount = currentIterations;

            if (iterationsPassed == 0) {
              return;
            }

            final long timePerIteration = timePassed / iterationsPassed;
            final long timePerIterationInMks = timePerIteration / 1000;
            final long iterationsPerSecond = 1_000_000_000 / timePerIteration;

            System.out.printf("%d iterations out of %d are passed, avg. iteration time is %d us, throughput %d iter/s\n",
                currentIterations, numThreads * iterationsPerThread, timePerIterationInMks, iterationsPerSecond);
          }
        }
      }, 10, 10 * 1000);

      final long workloadStartTs = System.nanoTime();
      for (int i = 0; i < numThreads; i++) {
        futures
            .add(executorService.submit(createTask((int) iterationsPerThread, pool, generator, profilesCount, iterationsCounter)));
      }

      for (Future<Integer> future : futures) {
        future.get();
      }
      final long workloadEndTs = System.nanoTime();
      final long worloadInterval = workloadEndTs - workloadStartTs;

      final long hours = worloadInterval / NANOS_IN_HOURS;
      final long minutes = (worloadInterval - hours * NANOS_IN_HOURS) / NANOS_IN_MINUTES;
      final long seconds = (worloadInterval - hours * NANOS_IN_HOURS - minutes * NANOS_IN_MINUTES) / NANOS_IN_SECONDS;

      final long timePerIteration = worloadInterval / (numThreads * iterationsPerThread);
      final long timePerIterationInMks = timePerIteration / 1000;
      final long iterationsPerSecond = 1_000_000_000 / timePerIteration;

      statusTimer.cancel();

      System.out
          .printf("Workload is completed for %d h. %d min. %d s.  avg. iteration time is %d us, throughput %d iter/s \n", hours,
              minutes, seconds, timePerIterationInMks, iterationsPerSecond);
    }
  }

  private void warmUp(int profilesCount, OrientDB orientDB, ZipfianGenerator generator, int numThreads, long iterationsPerThread,
      ExecutorService executorService) throws InterruptedException, java.util.concurrent.ExecutionException {
    List<Future<Integer>> futures = new ArrayList<>();

    try (ODatabasePool pool = new ODatabasePool(orientDB, DEFAULT_DB_NAME, "admin", "admin")) {
      System.out.printf("Starting of warm up with %d threads, %d iterations for each thread\n", numThreads, iterationsPerThread);
      final AtomicInteger iterationsCounter = new AtomicInteger();

      Timer statusTimer = new Timer();
      statusTimer.scheduleAtFixedRate(new TimerTask() {
        private long ts = -1;
        private long iterationsCount;

        @Override
        public void run() {
          if (ts == -1) {
            ts = System.nanoTime();
            iterationsCount = iterationsCounter.get();
          } else {
            long currentTs = System.nanoTime();
            long currentIterations = iterationsCounter.get();

            long timePassed = currentTs - ts;
            long iterationsPassed = currentIterations - iterationsCount;

            ts = currentTs;
            iterationsCount = currentIterations;

            if (iterationsPassed == 0) {
              return;
            }

            final long timePerIteration = timePassed / iterationsPassed;
            final long timePerIterationInMks = timePerIteration / 1000;
            final long iterationsPerSecond = 1_000_000_000 / timePerIteration;

            System.out.printf("%d iterations out of %d are passed, avg. iteration time is %d us, throughput %d iter/s\n",
                currentIterations, numThreads * iterationsPerThread, timePerIterationInMks, iterationsPerSecond);
          }
        }
      }, 10, 10 * 1000);

      final long warmUpStartTs = System.nanoTime();
      for (int i = 0; i < numThreads; i++) {
        futures.add(
            executorService.submit(new PokecReader((int) iterationsPerThread, pool, generator, profilesCount, iterationsCounter)));
      }

      for (Future<Integer> future : futures) {
        future.get();
      }
      final long warmUpEndTs = System.nanoTime();
      final long warmUpInterval = warmUpEndTs - warmUpStartTs;

      final long hours = warmUpInterval / NANOS_IN_HOURS;
      final long minutes = (warmUpInterval - hours * NANOS_IN_HOURS) / NANOS_IN_MINUTES;
      final long seconds = (warmUpInterval - hours * NANOS_IN_HOURS - minutes * NANOS_IN_MINUTES) / NANOS_IN_SECONDS;

      final long timePerIteration = warmUpInterval / (numThreads * iterationsPerThread);
      final long timePerIterationInMks = timePerIteration / 1000;
      final long iterationsPerSecond = 1_000_000_000 / timePerIteration;

      statusTimer.cancel();

      System.out
          .printf("Warm up is completed for %d h. %d min. %d s.  avg. iteration time is %d us, throughput %d iter/s \n", hours,
              minutes, seconds, timePerIterationInMks, iterationsPerSecond);
    }
  }
}
