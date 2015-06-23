import java.util.Random;

import com.mongodb.*;


public class PutBinaryData {

    public static void main(String args[]) throws Exception {
        Random rnd = new Random();
        Mongo mongo = new MongoClient("localhost:27000");

        byte[] dt = new byte[1024*1024];

        for(int i = 0; i < 10000; i++) {
            rnd.nextBytes(dt);
            mongo.getDB("test").getCollection("dt").insert(new BasicDBObject("dt", dt));
            Thread.sleep(50);

            if (i % 50 == 0) {
                System.out.println("Inserted " + i);
            }
        }
    }
}
