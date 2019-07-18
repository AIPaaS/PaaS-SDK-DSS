package com.ai.paas.ipaas.dss;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ai.paas.ipaas.ccs.inner.CCSComponentFactory;
import com.ai.paas.ipaas.dss.impl.DSSSrvClient;
import com.ai.paas.ipaas.uac.service.UserClientFactory;
import com.ai.paas.ipaas.uac.vo.AuthDescriptor;
import com.ai.paas.ipaas.uac.vo.AuthResult;
import com.ai.paas.util.Assert;
import com.ai.paas.util.CiperUtil;
import com.google.gson.Gson;

public class DSSFactory {

    private static final String DSS_CONFIG_COMMON_PATH = "/DSS/COMMON";
    private static final String DSS_CONFIG_PATH = "/DSS/";
    private static final String MONGO_USER = "username";
    private static final String MONGO_PASSWORD = "password";
    private static final String MONGO_HOST = "hosts";
    private static final String MONGO_REDIS_HOST = "redisHosts";

    private static final String PWD_KEY = "BaryTukyTukyBary";

    private static Map<String, IDSSClient> clients = new ConcurrentHashMap<>();
    private static final Logger log = LogManager.getLogger(DSSFactory.class);

    private DSSFactory() {
        // 禁止私有化
    }

    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static IDSSClient getClient(AuthDescriptor ad) throws Exception {
        log.info("Check Formal Parameter AuthDescriptor ...");
        Assert.notNull(ad, "com.ai.paas.ipaas.common.auth_info_null");
        Assert.notNull(ad.getServiceId(), "com.ai.paas.ipaas.common.srvid_null");
        Assert.notNull(ad.getPid(), "com.ai.paas.ipaas.common.auth_pid_null");
        Assert.notNull(ad.getPassword(), "com.ai.paas.ipaas.common.auth_passwd_null");
        Assert.notNull(ad.getAuthAdress(), "com.ai.paas.ipaas.common.auth_addr_null");
        IDSSClient client = null;
        String srvId = ad.getServiceId();
        srvId = srvId.trim();
        // 服务号要检验
        // 传入用户描述对象，用户认证地址，服务申请号
        // 进行用户认证
        log.info("Check AuthResult ...");
        // 认证通过后，判断是否存在已有实例，有，直接返回
        // 单例标签
        String instanceKey = ad.getPid().trim() + "_" + ad.getServiceId().trim();
        if (null != clients.get(instanceKey)) {
            client = clients.get(instanceKey);
            return client;
        }
        AuthResult authResult = UserClientFactory.getUserClient().auth(ad);
        // 开始初始化
        Assert.notNull(authResult.getConfigAddr(), "com.ai.paas.ipaas.common.zk_addr_null");
        Assert.notNull(authResult.getConfigUser(), "com.ai.paas.ipaas.common.zk_user_null");
        Assert.notNull(authResult.getConfigPasswd(), "com.ai.paas.ipaas.common.zk_passwd_null");
        Assert.notNull(authResult.getUserId(), "com.ai.paas.ipaas.common.user_id_null");
        // 获取内部zk地址后取得该用户的cache配置信息，返回JSON String
        // 获取该用户申请的cache服务配置信息
        log.info("Get DSSConf ...");
        String conf = CCSComponentFactory
                .getConfigClient(authResult.getConfigAddr(), authResult.getConfigUser(), authResult.getConfigPasswd())
                .get(DSS_CONFIG_COMMON_PATH);
        String redisConf = CCSComponentFactory
                .getConfigClient(authResult.getConfigAddr(), authResult.getConfigUser(), authResult.getConfigPasswd())
                .get(DSS_CONFIG_PATH + srvId);
        Gson gson = new Gson();
        Map confMap = gson.fromJson(conf, Map.class);
        String hosts = (String) confMap.get(MONGO_HOST);
        String username = (String) confMap.get(MONGO_USER);
        String password = CiperUtil.decrypt(PWD_KEY, (String) confMap.get(MONGO_PASSWORD));
        String redisHosts = (String) confMap.get(MONGO_REDIS_HOST);
        Map redisConfMap = gson.fromJson(redisConf, Map.class);
        String userId = authResult.getUserId();
        client = new DSSSrvClient(hosts, userId, username, password, redisHosts, redisConfMap);
        clients.put(instanceKey, client);
        return client;
    }

}
