import java.security.*;
import java.util.Date;
import java.util.concurrent.*;

import com.mongodb.*;


public class BlockstoreBenchmark {

    final static int MMAP_PORT=30017; //current behavior with mmap
    final static int WT_PORT=30018; //current behavior with WT block
    final static int WT2_PORT=30019; //current behavior with WT lsm
    final static int WT3_PORT=30020; //different groom behavior with WT ls

    final static int PORTS[] = { MMAP_PORT, WT_PORT, WT2_PORT, WT3_PORT};

    public static void main(String args[]) throws Exception {
        System.out.println("Starting: " + new Date());

        for (int port : PORTS) {
            System.out.println("\n\nRunning test on port: " + port);
            System.out.println("Starting: " + new Date());
            Mongo mongo = new MongoClient("localhost", port);

            if (port == WT3_PORT) {
                runTestWTNewGroom(mongo);
            }
            else {
                runTestCurrentBehavior(mongo);
            }
            System.out.println("Done: " + new Date());

            //leave the last one up for an unrelated test
            if ( port != WT3_PORT) {
                try {
                    mongo.getDB("admin").command("fsync");
                    mongo.getDB("admin").command("shutdown");
                } catch (Exception e) {}
            }

            Thread.sleep(5000);
        }
    }

    public static void runTestCurrentBehavior(final Mongo mongo) throws Exception{
        System.out.println("Running current insertion test");

        final long start = System.currentTimeMillis();

        ExecutorService execInsert = Executors.newFixedThreadPool(4);
        System.out.println("Starting insertion");

        for (int i = 0; i < 4; i++) {
            execInsert.submit(new BlockInserter(mongo, "db_"+i, 0, 1_000_000, true));
        }

        execInsert.shutdown();
        execInsert.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);

        long insertEnd1 = System.currentTimeMillis();

        System.out.println("Done inserting round 1 in seconds:" + (insertEnd1 - start) / 1000);

        Thread groomThread = new Thread() {
            public void run()
            {
                final long groomStart = System.currentTimeMillis();
                Thread groomThread = new Thread(new MmapStyleGroomer(mongo, "db_0", "db_0_B", .45));
                groomThread.start();
                try {
                    groomThread.join();
                } catch (InterruptedException e) {}

                final long groomEnd = System.currentTimeMillis();
                System.out.println("Done grooming in seconds: " + (groomEnd - groomStart) / 1000);
            }
        };
        groomThread.start();

        long insertStart2 = System.currentTimeMillis();
        ExecutorService execInsert2 = Executors.newFixedThreadPool(4);
        System.out.println("Starting insertion");

        for (int i = 0; i < 4; i++) {
            String dbName = "db_" + String.valueOf(i);
            if (i == 0) {
                dbName = "db_0_B";
            }

            execInsert2.submit(new BlockInserter(mongo, dbName, 1_000_000, 100_000, true));
        }
        execInsert2.shutdown();
        execInsert2.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);

        long insertEnd2 = System.currentTimeMillis();

        System.out.println("Done inserting round 2 in seconds:" + (insertEnd2 - insertStart2) / 1000);

        groomThread.join();

        System.out.println("Total runtime: " + (System.currentTimeMillis() - start) / 1000);
        //System.out.println("DB File size: " + getTotalDBSize(mongo));
    }

    public static void runTestWTNewGroom(final Mongo mongo) throws Exception {
        System.out.println("Running wt new groom test");

        final long start = System.currentTimeMillis();

        ExecutorService execInsert = Executors.newFixedThreadPool(4);
        System.out.println("Starting insertion");

        for (int i = 0; i < 4; i++) {
            execInsert.submit(new BlockInserter(mongo, "db_"+i, 0, 1_000_000, false));
        }

        execInsert.shutdown();
        execInsert.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);

        long insertEnd1 = System.currentTimeMillis();

        System.out.println("Done inserting round 1 in seconds:" + (insertEnd1 - start) / 1000);

        Thread groomThread = new Thread() {
            public void run()
            {
                final long groomStart = System.currentTimeMillis();
                Thread groomThread = new Thread(new WTStyleGroomer(mongo, "db_0", .45));
                groomThread.start();
                try {
                    groomThread.join();
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

                final long groomEnd = System.currentTimeMillis();
                System.out.println("Done grooming in seconds: " + (groomEnd - groomStart) / 1000);
            }
        };
        groomThread.start();

        long insertStart2 = System.currentTimeMillis();
        ExecutorService execInsert2 = Executors.newFixedThreadPool(4);
        System.out.println("Starting insertion");

        for (int i = 0; i < 4; i++) {
            String dbName = "db_" + String.valueOf(i);

            execInsert2.submit(new BlockInserter(mongo, dbName, 1_000_000, 100_000, false));
        }
        execInsert2.shutdown();
        execInsert2.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);

        long insertEnd2 = System.currentTimeMillis();

        System.out.println("Done inserting round 2 in seconds:" + (insertEnd2 - insertStart2) / 1000);

        groomThread.join();

        System.out.println("Total runtime: " + (System.currentTimeMillis() - start) / 1000);
    }


    public static double getTotalDBSize(Mongo mongo) {
        double size = 0;
        for(String dbName : mongo.getDatabaseNames()) {
            CommandResult result = mongo.getDB(dbName).getStats();
            size += ((Number) result.get("fileSize")).doubleValue();
        }

        return size;
    }

    public static String hash256(byte[] bytes) {
        try{
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(bytes);

            StringBuilder ret = new StringBuilder();
            for (byte hashByte : digest.digest())
                ret.append(Integer.toHexString(0xFF & hashByte));

            return ret.toString();
        }catch (java.security.NoSuchAlgorithmException noAlgo) {
            throw new RuntimeException(noAlgo);
        }
    }

    public static void ensureNoPowerOf2(Mongo mongo, String dbName, String collName) {
        DB db = mongo.getDB(dbName);

        if (!db.collectionExists(collName))
            db.createCollection(collName, new BasicDBObject());

        DBObject powerOf2Cmd = new BasicDBObject();
        powerOf2Cmd.put("collMod", collName);
        powerOf2Cmd.put("usePowerOf2Sizes", false);

        CommandResult result = db.command(powerOf2Cmd);
        result.throwOnError();

    }

    public static class BlockInserter implements Runnable {

        final DBCollection collection;
        final int startId;
        final int numToInsert;
        final boolean warmIndex;

        public BlockInserter (Mongo mongo, String dbName, int startId, int numToInsert, boolean warmIndex) {
            DB db = mongo.getDB(dbName);
            this.collection = db.getCollection("blocks");
            this.startId = startId;
            this.numToInsert = numToInsert;
            this.warmIndex = warmIndex;

            ensureNoPowerOf2(mongo, dbName, "blocks");
        }

        public void run() {

            for(int i = startId; i < startId + numToInsert; i++) {
                String id = hash256(String.valueOf(i).getBytes());

                if (warmIndex) {
                    collection.findOne(new BasicDBObject("_id", id), new BasicDBObject("_id", 1));
                }

                DBObject obj = new BasicDBObject();
                obj.put("_id", id);
                obj.put("bytes", new byte[20*1024]);

                collection.insert(obj);
                try {
                    Thread.sleep(2);
                } catch (InterruptedException e) {
                }
            }
        }
    }

    public static class MmapStyleGroomer implements Runnable {
        final DBCollection source;
        final DBCollection target;
        final double percentToCopy;

        public MmapStyleGroomer(Mongo mongo, String sourceDbName, String targetDbName, double percentToCopy) {
            source = mongo.getDB(sourceDbName).getCollection("blocks");
            target = mongo.getDB(targetDbName).getCollection("blocks");
            this.percentToCopy = percentToCopy;

            ensureNoPowerOf2(mongo, sourceDbName, "blocks");
            ensureNoPowerOf2(mongo, targetDbName, "blocks");
        }

        public void run() {
            System.out.println("Source collection records: " + source.count());
            DBCursor readCur = source.find().sort(new BasicDBObject("$natural", 1));
            while (readCur.hasNext()) {
                DBObject obj = readCur.next();
                if (Math.random() < percentToCopy) {
                    target.insert(obj);
                }
            }

            source.getDB().dropDatabase();
        }
    }

    public static class WTStyleGroomer implements Runnable {
        DBCollection source;
        double percentToCopy;

        public WTStyleGroomer(Mongo mongo, String sourceDbName, double percentToCopy) {
            source = mongo.getDB(sourceDbName).getCollection("blocks");
            this.percentToCopy = percentToCopy;

            ensureNoPowerOf2(mongo, sourceDbName, "blocks");
        }

        public void run() {
            long size = source.count();

            for(int i = 0; i < size; i++) {
                if (Math.random() < percentToCopy) {
                    source.remove(new BasicDBObject("_id", hash256(String.valueOf(i).getBytes())));
                }
            }
        }
    }

    public class Reader implements Runnable {

        public void run() {

        }
    }

}
