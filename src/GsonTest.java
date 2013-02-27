import java.util.*;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.mongodb.*;
import com.mongodb.util.JSON;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class GsonTest {

    Mongo mongo;
    Gson gson;
    
    A testObj;
    
    @Before
    public void before() throws Exception {
        mongo = new MongoClient();
        
        testObj = new A();
        testObj.str1 = "131";
        testObj.str2 = "abc";
        testObj.intList = Arrays.asList(1, 2, 3, 4, 5);
        
        testObj.b = new B();
        
        testObj.b.xyz = 999;
        testObj.b.strList = Arrays.asList("1", "2", "3");
        
        gson = new GsonBuilder().create();
    }

    @Test
    public void serializeB() {
        DBCollection coll = mongo.getDB("test").getCollection("gsonSerializationB");
        coll.drop();
        
        String json = gson.toJson(testObj.b);
        String className = testObj.b.getClass().getName();
        
        System.out.println(json);
        System.out.println(className);
        
        DBObject obj = (DBObject)JSON.parse(json);
        DBObject toStore = new BasicDBObject();
        toStore.put("className", className);
        toStore.put("obj", obj);
        
        assertEquals(999, obj.get("xyz"));
        assertEquals(3, ((List)obj.get("strList")).size());
        assertEquals("2", ((List)obj.get("strList")).get(1));
        
        coll.insert(toStore);
    }
    
    @Test
    public void serializeA() {
        DBCollection coll = mongo.getDB("test").getCollection("gsonSerializationA");
        coll.drop();
        
        String json = gson.toJson(testObj);
        String className = testObj.getClass().getName();
        
        System.out.println(json);
        System.out.println(className);
        
        DBObject obj = (DBObject)JSON.parse(json);
        DBObject toStore = new BasicDBObject();
        toStore.put("className", className);
        toStore.put("obj", obj);
        
        assertEquals("131", obj.get("str1"));
        assertEquals("abc", obj.get("str2"));
        assertEquals(5, ((List)obj.get("intList")).size());
        assertEquals(2, ((List)obj.get("intList")).get(1));
        
        coll.insert(toStore);
    }
    
    @Test
    public void deserializeA() throws Exception {
        DBCollection coll = mongo.getDB("test").getCollection("gsonSerializationA");
        DBObject retrievedObj = coll.findOne();
        
        String className = (String)retrievedObj.get("className");
        DBObject obj = (DBObject)retrievedObj.get("obj");
        
        String json = JSON.serialize(obj);
        
        System.out.println(json);
        System.out.println(className);
        
        A originalObj = (A)gson.fromJson(json, Class.forName(className));
        
        System.out.println("Got A: " + originalObj);
        
        assertEquals("131", originalObj.str1);
        assertEquals("abc", originalObj.str2);
        assertEquals(5, originalObj.intList.size());
        assertEquals(testObj.b.strList.size(), originalObj.b.strList.size());
        assertEquals(testObj.b, originalObj.b);
    }
    
    @Test
    public void deserializeB() throws Exception {
        DBCollection coll = mongo.getDB("test").getCollection("gsonSerializationB");
        DBObject retrievedObj = coll.findOne();
        
        String className = (String)retrievedObj.get("className");
        DBObject obj = (DBObject)retrievedObj.get("obj");
        
        String json = JSON.serialize(obj);
        
        System.out.println(json);
        System.out.println(className);
        
        B originalObj = (B)gson.fromJson(json, Class.forName(className));
        
        System.out.println("Got B: " + originalObj);
        
        assertEquals(999, originalObj.xyz.intValue());
        assertEquals(3, originalObj.strList.size());
        assertEquals("2", originalObj.strList.get(1));
        assertEquals(testObj.b, originalObj);
    }
    
    public static class A {
        String str1, str2;
        List<Integer> intList;
        B b;

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            A other = (A) obj;
            if (b == null) {
                if (other.b != null)
                    return false;
            } else if (!b.equals(other.b))
                return false;
            if (intList == null) {
                if (other.intList != null)
                    return false;
            } else if (!intList.equals(other.intList))
                return false;
            if (str1 == null) {
                if (other.str1 != null)
                    return false;
            } else if (!str1.equals(other.str1))
                return false;
            if (str2 == null) {
                if (other.str2 != null)
                    return false;
            } else if (!str2.equals(other.str2))
                return false;
            return true;
        }

        @Override
        public String toString() {
            return "A [str1=" + str1 + ", str2=" + str2 + ", intList="
                    + intList + ", b=" + b + "]";
        }
        
        
    }
    
    public static class B {
        Integer xyz;
        List<String> strList;
        

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            B other = (B) obj;
            if (strList == null) {
                if (other.strList != null)
                    return false;
            } else if (!strList.equals(other.strList))
                return false;
            if (xyz == null) {
                if (other.xyz != null)
                    return false;
            } else if (!xyz.equals(other.xyz))
                return false;
            return true;
        }


        @Override
        public String toString() {
            return "B [xyz=" + xyz + ", strList=" + strList + "]";
        }
        
        
    }

}
