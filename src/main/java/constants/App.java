package apps.smartfwd.src.main.java.constants;

import org.onosproject.core.ApplicationId;

import javax.xml.crypto.dsig.spec.XSLTTransformParameterSpec;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class App {
    public static ApplicationId appId;
    ExecutorService pool= Executors.newFixedThreadPool(50);
    ScheduledExecutorService scheduledPool=Executors.newScheduledThreadPool(10);
    static App instance=new App();
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

    public static final String WEB_LISTENING_IP = "0.0.0.0";
    public static final int WEB_LISTENING_PORT = 1060;

    public static final String Server_IP = "192.168.1.49";
    public static final int Server_PORT = 1061;

    public static final String DNS_IP = "114.114.114.114";

    public static final String VUE_FRONT_IP = "192.168.1.49";

    public static final int FLOW_TIMEOUT = 60;
}
