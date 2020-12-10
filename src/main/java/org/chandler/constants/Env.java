package apps.smartfwd.src.main.java.org.chandler.constants;

import org.onlab.packet.IpPrefix;
import org.onlab.packet.MacAddress;
import org.onosproject.net.DeviceId;
import org.onosproject.net.PortNumber;

import java.util.HashMap;

public class Env {
    //todo decide switch number at runtime
    public static int N_SWITCH=0;
    public static final int N_FLOWS=3;
    private Env(){}
    static Env instance;
    static Env getInstance(){
        return instance;
    }

}
