import java.security.*;
import java.util.*;
import java.util.concurrent.*;

import com.mongodb.*;


public class SimulateBlockstoreInserts {

    public static Random rnd = new Random();

    public static void main(String args[]) throws Exception {
        String hostport = System.getProperty("HOSTPORT");


        System.out.println("Starting: " + new Date());

        System.out.println("\n\nRunning test on: " + hostport);
        System.out.println("Starting: " + new Date());
        Mongo mongo = new MongoClient(hostport);

        runTestCurrentBehavior(mongo);

        System.out.println("Done: " + new Date());

    }

    public static void runTestCurrentBehavior(final Mongo mongo) throws Exception{
        System.out.println("Running current insertion test");

        final int numDBs = 10;
        final int threadsPerDB = 5;

        final long start = System.currentTimeMillis();

        ExecutorService execInsert = Executors.newFixedThreadPool(numDBs * threadsPerDB);
        System.out.println("Starting insertion");

        for (int i = 0; i < numDBs * threadsPerDB; i++) {
            execInsert.submit(new BlockInserter(mongo, "db_"+(i/threadsPerDB), 0, 1_000_000, true));
        }

        execInsert.shutdown();
        execInsert.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);

        long insertEnd1 = System.currentTimeMillis();

        System.out.println("Records in:" + (insertEnd1 - start) / 1000);
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
        powerOf2Cmd.put("noPadding", true);

        CommandResult result = db.command(powerOf2Cmd);
        result.throwOnError();

    }

    public static class BlockInserter implements Runnable {

        final DBCollection collection;
        final int startId;
        final int numToInsert;
        final boolean warmIndex;

        public BlockInserter (Mongo mongo, String dbName, int startId, int numToInsert, boolean warmIndex) {
            System.out.println("Starting inserter into DB: " + dbName + ", docsToInsert: " + numToInsert);

            DB db = mongo.getDB(dbName);
            this.collection = db.getCollection("blocks");
            this.startId = startId;
            this.numToInsert = numToInsert;
            this.warmIndex = warmIndex;

            ensureNoPowerOf2(mongo, dbName, "blocks");
        }

        public void touchIndexes() {
            DBObject touch = new BasicDBObject();
            touch.put("touch", collection.getName());
            touch.put("data", false);
            touch.put("index", true);

            collection.getDB().command(touch);
        }

        public void saveBlocks(List<DBObject> blocks) {
            try{
                collection.insert(blocks, WriteConcern.REPLICA_ACKNOWLEDGED);
            }catch (DuplicateKeyException dupKeyExc) {
                ;
            }
        }

        public void run() {
            byte[] data = new byte[10*1024];

            List<DBObject> toInsert = new ArrayList<>();

            for(int i = startId; i < startId + numToInsert; i++) {
                SimulateBlockstoreInserts.rnd.nextBytes(data);
                //Arrays.fill(data, (byte)3);
                String id = hash256(data);

                if (warmIndex && i % (32 * 1024 * 1024) == 0) {
                    touchIndexes();
                }

                DBObject obj = new BasicDBObject();
                obj.put("_id", id);
                obj.put("bytes", data);

                toInsert.add(obj);

                if (toInsert.size() == 5) {
                    saveBlocks(toInsert);
                    try {
                        Thread.sleep(2);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    toInsert.clear();
                }

            }
        }
    }
}
