package com.ai.paas.ipaas.dss.impl;

import java.util.HashSet;
import java.util.Set;

import com.ai.paas.ipaas.dss.impl.DSSHelper;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

public class DSSSrvHelper extends DSSHelper {
    public static JedisCluster getRedis(String redisHost) {
        Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
        for (String address : redisHost.split(";")) {
            String[] ipAndPort = address.split(":");
            jedisClusterNodes.add(new HostAndPort(ipAndPort[0], Integer.parseInt(ipAndPort[1])));
        }
        JedisCluster jc = new JedisCluster(jedisClusterNodes);
        return jc;
    }
}
