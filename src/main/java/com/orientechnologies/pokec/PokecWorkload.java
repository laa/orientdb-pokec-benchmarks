package com.orientechnologies.pokec;

import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.pokec.common.CommandLineUtils;
import com.orientechnologies.pokec.common.ZipfianGenerator;
import com.orientechnologies.pokec.load.PokecLoad;
import com.orientechnologies.pokec.read.PokecReader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class PokecWorkload {
  private static final long NANOS_IN_HOURS   = 1_000_000_000L * 60 * 60;
  private static final long NANOS_IN_MINUTES = 1_000_000_000L * 60;
  private static final long NANOS_IN_SECONDS = 1_000_000_000L;

  protected void run(String[] args) throws Exception {
    Options options = CommandLineUtils.generateCommandLineOptions();

    CommandLineParser parser = new DefaultParser();
    try {
      CommandLine cmd = parser.parse(options, args);

      final long profilesCount;
      try (OrientDB orientDB = CommandLineUtils.createOrientDBInstance(cmd)) {
        final String dbName = CommandLineUtils.dbName(cmd);
        System.out.println("Opening " + dbName + " database");
        try (ODatabaseSession databaseSession = orientDB.open(dbName, "admin", "admin")) {
          profilesCount = databaseSession.countClass(PokecLoad.PROFILE_CLASS);
        }
        System.out.printf("%d profiles were detected \n", profilesCount);

        final ZipfianGenerator generator = new ZipfianGenerator(profilesCount);
        final int numThreads = CommandLineUtils.numThreads(cmd);

        final long iterationsPerThread = profilesCount / numThreads;

        final ExecutorService executorService = Executors.newCachedThreadPool();
        warmUp((int) profilesCount, orientDB, generator, numThreads, iterationsPerThread, executorService, dbName);

        final String path = CommandLineUtils.path(cmd);
        workload((int) profilesCount, orientDB, generator, numThreads, iterationsPerThread, executorService, dbName, path);
      }
    } catch (ParseException pe) {
      System.out.println(pe.getMessage());
    }
  }

  public abstract PokecWorkloadTask createTask(int iterationsCount, ODatabasePool pool, ZipfianGenerator zipfianGenerator,
      int itemsCount, AtomicInteger iterationsCounter);

  private void workload(int profilesCount, OrientDB orientDB, ZipfianGenerator generator, int numThreads, long iterationsPerThread,
      ExecutorService executorService, String dbName, String path) throws Exception {
    List<Future<Integer>> futures = new ArrayList<>();

    try (ODatabasePool pool = new ODatabasePool(orientDB, dbName, "admin", "admin")) {
      System.out.printf("Starting of workload with %d threads, %d operations for each thread\n", numThreads, iterationsPerThread);
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

            System.out.printf("%d operations out of %d are passed, avg. operation time is %d us, throughput %d op/s\n",
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

      System.out.printf("Workload is completed for %s in %d h. %d min. %d s. avg. operation time is %d us, throughput %d op/s, "
          + "number of threads %d\n", path, hours, minutes, seconds, timePerIterationInMks, iterationsPerSecond, numThreads);
    }
  }

  private void warmUp(int profilesCount, OrientDB orientDB, ZipfianGenerator generator, int numThreads, long iterationsPerThread,
      ExecutorService executorService, String dbName) throws InterruptedException, java.util.concurrent.ExecutionException {
    List<Future<Integer>> futures = new ArrayList<>();

    try (ODatabasePool pool = new ODatabasePool(orientDB, dbName, "admin", "admin")) {
      System.out.printf("Starting of warm up with %d threads, %d operations for each thread\n", numThreads, iterationsPerThread);
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

            System.out.printf("%d operations out of %d are passed, avg. operation time is %d us, throughput %d op/s\n",
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
          .printf("Warm up is completed for %d h. %d min. %d s. avg. operation time is %d us, throughput %d op/s \n", hours,
              minutes, seconds, timePerIterationInMks, iterationsPerSecond);
    }
  }
}
