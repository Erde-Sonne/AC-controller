package apps.smartfwd.src.main.java.org.chandler.constants;

import org.onosproject.core.ApplicationId;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class App {
    //todo
    public static ApplicationId appId;
    ExecutorService pool= Executors.newCachedThreadPool();
    ScheduledExecutorService scheduledPool=Executors.newScheduledThreadPool(10);
    static App instance;
    private App(){}
    public static App getInstance(){
        return instance;
    }
    public ExecutorService getPool(){
        return pool;
    }
    public ScheduledExecutorService getScheduledPool(){
        return scheduledPool;
    }
    public static final String DEFAULT_ROUTING_IP="";
    public static final int DEFAULT_ROUTING_PORT=0;
    public static final String OPT_ROUTING_IP="";
    public static final int OPT_ROUTING_PORT=0;

    public static final String LISTENING_IP="0.0.0.0";
    public static final int CLASSIFIER_LISTENING_PORT=0;

    public static final String ALG_CLASSIFIER_IP="";
    public static final int ALG_CLASSIFIER_PORT=0;
}

