import java.security.*;
import java.util.concurrent.*;
import com.mongodb.*;


public class BlockstoreBenchmark {

    final static int MMAP_PORT=30017;
    final static int WT_PORT=30018;
    final static int WT2_PORT=30019;

    public static void main(String args[]) throws Exception {
        Mongo mongo = new MongoClient("localhost", MMAP_PORT);
        runTestCurrentBehavior(mongo);
    }

    public static void runTestCurrentBehavior(final Mongo mongo) throws Exception{
        System.out.println("Running mmap insertion test");

        final long start = System.currentTimeMillis();

        ExecutorService execInsert = Executors.newFixedThreadPool(4);
        System.out.println("Starting insertion");

        for (int i = 0; i < 4; i++) {
            execInsert.submit(new BlockInserter(mongo, "db_"+i, 0, 1_000_000));
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

            execInsert2.submit(new BlockInserter(mongo, dbName, 1_000_000, 100_000));
        }
        execInsert2.shutdown();
        execInsert2.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);

        long insertEnd2 = System.currentTimeMillis();

        System.out.println("Done inserting round 2 in seconds:" + (insertEnd2 - insertStart2) / 1000);

        groomThread.join();

        System.out.println("Total runtime: " + (System.currentTimeMillis() - start) / 1000);
        System.out.println("DB File size: " + getTotalDBSize(mongo));
    }

    public static void runTestWTNewGroom() {

    }


    public static long getTotalDBSize(Mongo mongo) {
        long size = 0;
        for(String dbName : mongo.getDatabaseNames()) {
            CommandResult result = mongo.getDB(dbName).getStats();
            size += (Long) result.get("fileSize");
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

        public BlockInserter (Mongo mongo, String dbName, int startId, int numToInsert) {
            DB db = mongo.getDB(dbName);
            this.collection = db.getCollection("blocks");
            this.startId = startId;
            this.numToInsert = numToInsert;

            ensureNoPowerOf2(mongo, dbName, "blocks");
        }

        public void run() {

            for(int i = startId; i < startId + numToInsert; i++) {
                String id = hash256(String.valueOf(i).getBytes());
                collection.findOne(new BasicDBObject("_id", id), new BasicDBObject("_id", 1));

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
            DBCursor readCur = source.find().sort(new BasicDBObject("$natural", 1));
            while (readCur.hasNext()) {
                DBObject obj = readCur.next();
                if (Math.random() < percentToCopy) {
                    target.insert(obj);
                }
            }

            source.drop();
        }
    }

    public class WTStyleGroomer implements Runnable {
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
