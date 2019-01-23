package day11.StringDemo;

import com.google.gson.Gson;
import redis.clients.jedis.Jedis;

import java.io.*;

public class StringDemo2 {
    private static Jedis jedis = new Jedis("mini1",6379);
    public static void main(String[] args) throws IOException, ClassNotFoundException {
//        StringTest();
//        objectTest();
        objectToJson();
    }

    /**
     * 将字符串缓存到String数据结构中
     */
    public static void StringTest( ) {
        jedis.set("user:001:name","xiaofeng");
        jedis.mset("user:002:name", "xiaofen", "user:003:name", "yaoyao");

        String uname001 = jedis.get("user:001:name");
        String uname002 = jedis.get("user:002:name");
        String uname003 = jedis.get("user:003:name");

        System.out.println(uname001);
        System.out.println(uname002);
        System.out.println(uname003);
    }

    /**
     * 将对象缓存到String数据结构中
     *
     */

    public static void objectTest() throws IOException, ClassNotFoundException {
        ProduceInfo p = new ProduceInfo();
        p.setName("Iphone8plus");
        p.setPrice(7999.9);
        p.setProcuctDesc("看视频");

        // 将对象序列化到对象数组中
        ByteArrayOutputStream ba = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(ba);
        // 将对象序列化的方式将ProduceInfo的方式写入到流中
        oos.writeObject(p);
        // 将ba流转换为字节数组
        byte[] pBytes = ba.toByteArray();

        // 将序列化好的数据缓存到Redis中
        jedis.set("product:001".getBytes(),pBytes);

        // 读取刚才缓存的数据
        byte[] pBytesRes = jedis.get("product:001".getBytes());
        // 反序列化
        ByteArrayInputStream bi = new ByteArrayInputStream(pBytes);
        ObjectInputStream oi = new ObjectInputStream(bi);
        ProduceInfo pRes = (ProduceInfo)oi.readObject();

        System.out.println(pRes);

    }

    /**
     * 将对象转换为json字符串缓存到Redis中
     */
    public static void objectToJson(){
        ProduceInfo p = new ProduceInfo();
        p.setName("Iphone4");
        p.setPrice(4888.8);
        p.setProcuctDesc("用来起啤酒盖");

        // 将对象转为json格式
        Gson gson = new Gson();
        String jsonProductInfo = gson.toJson(p);

        // 缓存到Redis中
        jedis.set("product002",jsonProductInfo);

        // 获取数据
        String jsonRes = jedis.get("product002");

        // 将json字符串转换为对象
        ProduceInfo produceInfo = gson.fromJson(jsonRes,ProduceInfo.class);

        System.out.println(jsonRes);
        System.out.println(produceInfo);
    }

}

