package apps.smartfwd.src.main.java;

import org.onlab.packet.IpPrefix;
import org.onlab.packet.MacAddress;
import org.onosproject.net.DeviceId;
import org.onosproject.net.PortNumber;

import java.util.HashMap;

public final class HandleConfigFile {
    // topo中交换机的个数
    private static int swithNum;
    // 对switch的映射
    private static HashMap<String, DeviceId> swMap = new HashMap<>();
    // switch 到id 的反映射
    private static HashMap<DeviceId, String> reverswMap = new HashMap<>();
    // 对host mac的映射
    private static HashMap<String, MacAddress> hostMacMap = new HashMap<>();
    // 对host ip的映射
    private static HashMap<String, IpPrefix> hostIpMap = new HashMap<>();
    // 对host接入对应swithc的端口的映射
    private static HashMap<String, PortNumber> portMap = new HashMap<>();

    // IP对switch的映射
    private static HashMap<String, DeviceId> ip2swMap = new HashMap<>();
    static {
        swithNum = 100;
        for (int i = 0; i < swithNum; i++) {
            String rawStr = base16(i + 1);
            int zeroLen = 16 - rawStr.length();
            StringBuilder stringBuilder = new StringBuilder();
            for (int i1 = 0; i1 < zeroLen; i1++) {
                stringBuilder.append(0);
            }
            String res = stringBuilder.toString() + rawStr;
            swMap.put("" + i, DeviceId.deviceId("of:" + res));
            reverswMap.put(DeviceId.deviceId("of:" + res), "" + i);
        }
        for (int i = 0; i < swithNum; i++) {
            String rawStr = base16(i + 1);
            int len = rawStr.length();
            String res;
            if (len == 1) {
                res = "0" + rawStr;
            } else {
                res = rawStr;
            }
            hostMacMap.put("" + i, MacAddress.valueOf("00:00:00:00:00:" + res));
        }
        for (int i = 0; i < swithNum; i++) {
            hostIpMap.put("" + i, IpPrefix.valueOf("10.0.0." + (i + 1) + "/32"));
        }
        for (int i = 0; i < swithNum; i++) {
            portMap.put("" + i, PortNumber.portNumber(1));
        }
        for (int i = 0; i < swithNum; i++) {
            String rawStr = base16(i + 1);
            int zeroLen = 16 - rawStr.length();
            StringBuilder stringBuilder = new StringBuilder();
            for (int i1 = 0; i1 < zeroLen; i1++) {
                stringBuilder.append(0);
            }
            String res = stringBuilder.toString() + rawStr;
            ip2swMap.put("10.0.0." + (i + 1) + "/32", DeviceId.deviceId("of:" + res));
        }
    }
    private HandleConfigFile() {

    }
    public static int getSwithNum() {
        return swithNum;
    }

    public static String base16(int num) {
        if (num == 0) {
            return "0";
        }
        StringBuilder stringBuilder = new StringBuilder();
        while (num > 0) {
            int left = num % 16;
            if (left < 10) {
                stringBuilder.append(left);
            } else {
               char c = (char) ('a' + (left - 10));
               stringBuilder.append(c);
            }
            num = num / 16;
        }
        String res = stringBuilder.reverse().toString();
        return res;
    }

    public static HashMap<String, DeviceId> getSwMap() {
        return swMap;
    }

    public static HashMap<DeviceId, String> getReverswMap() {
        return reverswMap;
    }

    public static HashMap<String, MacAddress> getHostMacMap() {
        return hostMacMap;
    }

    public static HashMap<String, IpPrefix> getHostIpMap() {
        return hostIpMap;
    }

    public static HashMap<String, PortNumber> getPortMap() {
        return portMap;
    }
    public static HashMap<String, DeviceId> getIp2swMap() {
        return ip2swMap;
    }

}
