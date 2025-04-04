package org.example.service.impl;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.List;

@Service
public class RedisService {

    private final JedisPool jedisPool;

    public RedisService() {
        // Replace with your Redis host and port
        this.jedisPool = new JedisPool("10.21.4.52", 6379);
    }

    public List<RedisKeyDetails> getAllKeyDetails() {
        List<RedisKeyDetails> details = new ArrayList<>();
        try (Jedis jedis = jedisPool.getResource()) {
            for (String key : jedis.keys("*")) {
                String value = jedis.get(key);
                int size = value != null ? value.length() : 0;
                details.add(new RedisKeyDetails(key, value, size));
            }
        }
        return details;
    }

    public static class RedisKeyDetails {
        private final String key;
        private final String value;
        private final int size;

        public RedisKeyDetails(String key, String value, int size) {
            this.key = key;
            this.value = value;
            this.size = size;
        }

        public String getKey() {
            return key;
        }

        public String getValue() {
            return value;
        }

        public int getSize() {
            return size;
        }
    }
}
