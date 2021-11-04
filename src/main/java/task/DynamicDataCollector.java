package apps.smartfwd.src.main.java.task;

import apps.smartfwd.src.main.java.TopologyDesc;
import apps.smartfwd.src.main.java.constants.FlowEntryPriority;
import apps.smartfwd.src.main.java.task.base.PeriodicalTask;
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

import java.util.HashMap;
import java.util.Map;

public class DynamicDataCollector extends PeriodicalTask {
    public interface Handler{
        void handle(Map<String,Map<String, long[]>> stats);
    }

    TopologyDesc desc;
    Handler handler;
    Map<String,Map<String, long[]>> preStats=new HashMap<>();
    Map<String,Map<String, long[]>> currStats=new HashMap<>();
    FlowRuleService flowRuleService;
    private final Logger logger= LoggerFactory.getLogger(getClass().getName());
    public DynamicDataCollector(FlowRuleService flowRuleService, TopologyDesc topo, Handler handler){
        this.desc=topo;
        this.handler=handler;
        this.flowRuleService=flowRuleService;

        this.worker=()->{
            //init
            Map<String,Map<String, long[]>> res=new HashMap<>();

            //reset
            currStats.clear();

            for(DeviceId deviceId:topo.getDeviceIds()){
                Map<String, long[]> tmp = new HashMap<>();
                for(FlowEntry entry:flowRuleService.getFlowEntries(deviceId)){
                    if(!entry.table().equals(IndexTableId.of(0))) continue;
                    if(entry.priority()!= FlowEntryPriority.RESOURCE_DEFAULT_ROUTING
                            && entry.priority()!= FlowEntryPriority.DEFAULT_ROUTING_FWD) continue;

                    TrafficSelector selector=entry.selector();
                    IPCriterion dstCriterion=(IPCriterion) selector.getCriterion(Criterion.Type.IPV4_DST);
                    IPCriterion srcCriterion=(IPCriterion) selector.getCriterion(Criterion.Type.IPV4_SRC);
                    EthCriterion srcMACcriterion = (EthCriterion) selector.getCriterion(Criterion.Type.ETH_SRC);
                    EthCriterion dstMACcriterion = (EthCriterion) selector.getCriterion(Criterion.Type.ETH_DST);

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

                    String key = "";
                    if (srcIP.equals("")) {
                        key = srcMAC + ">" + dstIP;
                    } else {
                        key = dstMAC + ">" + srcIP;
                    }
                    long[] tmpOrDefault = tmp.getOrDefault(key, new long[]{0L, 0L});
                    long packets = tmpOrDefault[0] + entry.packets();
                    long bytes = tmpOrDefault[1] + entry.bytes();
                    tmp.put(key, new long[]{packets, bytes});

                }
                currStats.put(String.valueOf(topo.getDeviceIdx(deviceId)), tmp);
            }


            //diff and replace
            for(DeviceId deviceId:topo.getDeviceIds()){
                String device = String.valueOf(topo.getDeviceIdx(deviceId));
                Map<String, long[]> curr=currStats.get(device);
                Map<String, long[]> pre=preStats.getOrDefault(device, new HashMap<>());
                Map<String, long[]> r=new HashMap<>();
                //diff and replace
                for(String key:curr.keySet()){
                    long[] curlongs = curr.get(key);
                    long[] tmp = pre.getOrDefault(key, new long[]{0L, 0L});
                    long packets = curlongs[0] - tmp[0];
                    long bytes = curlongs[1] - tmp[1];
                    r.put(key, new long[] {packets, bytes});
                }
                res.put(device, r);
                //将curr更新到pre
                pre.clear();
                pre.putAll(curr);
            }
            handler.handle(res);
        };
    }


}
