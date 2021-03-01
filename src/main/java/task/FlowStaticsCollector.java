package apps.smartfwd.src.main.java.task;

import apps.smartfwd.src.main.java.TopologyDesc;
import apps.smartfwd.src.main.java.constants.FlowEntryPriority;
import apps.smartfwd.src.main.java.task.base.PeriodicalTask;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.onosproject.net.DeviceId;
import org.onosproject.net.flow.FlowEntry;
import org.onosproject.net.flow.FlowRuleService;
import org.onosproject.net.flow.IndexTableId;
import org.onosproject.net.flow.TrafficSelector;
import org.onosproject.net.flow.criteria.Criterion;
import org.onosproject.net.flow.criteria.EthCriterion;
import org.onosproject.net.flow.criteria.IPCriterion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FlowStaticsCollector extends PeriodicalTask {
    public interface Handler{
        void handle(Map<String, List<Map<String, String>>> stats);
    }

    TopologyDesc desc;
    Handler handler;
    Map<String, List<Map<String, String>>> currStats=new HashMap<>();
    FlowRuleService flowRuleService;
    private final Logger logger= LoggerFactory.getLogger(getClass().getName());
    public FlowStaticsCollector(FlowRuleService flowRuleService, TopologyDesc topo, Handler handler){
        this.desc=topo;
        this.handler=handler;
        this.flowRuleService=flowRuleService;

        this.worker=()->{

            for(DeviceId srcId:topo.getDeviceIds()){
                //init
                List<Map<String, String>> res=new ArrayList<>();
                for(FlowEntry entry:flowRuleService.getFlowEntries(srcId)){
                    if(!entry.table().equals(IndexTableId.of(0))) continue;
                    if(entry.priority()!= FlowEntryPriority.RESOURCE_DEFAULT_ROUTING) continue;

                    TrafficSelector selector=entry.selector();
                    IPCriterion dstCriterion=(IPCriterion) selector.getCriterion(Criterion.Type.IPV4_DST);
                    IPCriterion srcCriterion=(IPCriterion) selector.getCriterion(Criterion.Type.IPV4_SRC);
                    EthCriterion srcMACcriterion = (EthCriterion) selector.getCriterion(Criterion.Type.ETH_SRC);
                    EthCriterion dstMACcriterion = (EthCriterion) selector.getCriterion(Criterion.Type.ETH_DST);
                    long time = new Date().getTime();
                    long packets = entry.packets();
                    long bytes = entry.bytes();
                    long life = entry.life();

                    String srcIP = "";
                    String dstIP = "";
                    String srcMAC = "";
                    String dstMAC = "";
                    if (srcCriterion != null) {
                        srcIP = srcCriterion.ip().toString();
                    }
                    if (dstCriterion != null) {
                        dstIP = dstCriterion.ip().toString();
                    }
                    if (srcMACcriterion != null) {
                        srcMAC = srcMACcriterion.mac().toString();
                    }
                    if (dstMACcriterion != null) {
                        dstMAC = dstMACcriterion.mac().toString();
                    }
                    Map<String, String> map = new HashMap<>();
                    map.put("time", String.valueOf(time));
                    map.put("packets", String.valueOf(packets));
                    map.put("bytes", String.valueOf(bytes));
                    map.put("life", String.valueOf(life));
                    map.put("srcIP", srcIP);
                    map.put("srcMAC", srcMAC);
                    map.put("dstIP", dstIP);
                    map.put("dstMAC", dstMAC);
//                    String statics = String.format("{\"time\":%s,\"srcIP\":\"%s\", \"srcMAC\":\"%s\", " +
//                            "\"dstIP\":\"%s\", \"dstMAC\":\"%s\", \"packets\":%s, \"bytes\":%s, \"life\":%s}",
//                            time, srcIP, srcMAC, dstIP, dstMAC, packets, bytes, life);
                    res.add(map);
                }
                currStats.put(String.valueOf(topo.getDeviceIdx(srcId)), res);
            }
            handler.handle(currStats);
        };
    }
}
