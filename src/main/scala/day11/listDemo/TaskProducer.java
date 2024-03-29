package day11.listDemo;

import redis.clients.jedis.Jedis;

import java.util.Random;
import java.util.UUID;

public class TaskProducer {
    private static Jedis jedis = new Jedis("mini1",6379);

    public static void main(String[] args) throws InterruptedException {
        Random r = new Random();
        while (true){
            int nextInt = r.nextInt(1000);
            Thread.sleep(2000+nextInt);
            String taskId = UUID.randomUUID().toString();

            jedis.lpush("task-queue1",taskId);
            System.out.println("生成一条任务信息：" + taskId);
        }
    }
}
