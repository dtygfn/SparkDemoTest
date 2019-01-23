package day11.StringDemo;

import redis.clients.jedis.Jedis;

public class StringDemo1 {
    public static void main(String[] args) {
        // 创建一个Jeis连接
        Jedis jedis = new Jedis("mini1",6379);

        String setAck = jedis.set("s1","111");
        System.out.println(setAck);

        String getAck = jedis.get("s1");
        System.out.println(getAck);

        jedis.close();
    }
}
