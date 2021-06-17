package apps.smartfwd.src.main.java.utils;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class Simulation {
    public static Set<String> blackMacSet = new HashSet<>();
    public static Set<String> localIpSet = new HashSet<>();
    static {
        blackMacSet.add("00:50:56:C0:00:02");
        for (int i = 0; i < 24; i++) {
            localIpSet.add("10.0.0." + (i+1));
        }
    }
}
