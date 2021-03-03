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
import org.onosproject.net.flow.criteria.IPProtocolCriterion;
import org.onosproject.net.flow.criteria.TcpPortCriterion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FlowStaticsCollector extends PeriodicalTask {
    public interface Handler{
        void handle(Map<String, List<Map<String, String>>> stats);
    }

    TopologyDesc desc;
    Handler handler;
    int[] deviceIds;
    Map<String, List<Map<String, String>>> currStats=new HashMap<>();
    FlowRuleService flowRuleService;
    private final Logger logger= LoggerFactory.getLogger(getClass().getName());
    public FlowStaticsCollector(FlowRuleService flowRuleService, TopologyDesc topo,
                                int[] deviceIds,  Handler handler){
        this.desc=topo;
        this.handler=handler;
        this.flowRuleService=flowRuleService;
        this.deviceIds = deviceIds;

        this.worker=()->{

            for(int srcId:this.deviceIds){
                //init
                List<Map<String, String>> res=new ArrayList<>();
                for(FlowEntry entry:flowRuleService.getFlowEntries(topo.getDeviceId(srcId))){
                    if(!entry.table().equals(IndexTableId.of(0))) continue;
                    if(entry.priority()!= FlowEntryPriority.RESOURCE_DEFAULT_ROUTING) continue;

                    TrafficSelector selector=entry.selector();
                    IPCriterion dstCriterion=(IPCriterion) selector.getCriterion(Criterion.Type.IPV4_DST);
                    IPCriterion srcCriterion=(IPCriterion) selector.getCriterion(Criterion.Type.IPV4_SRC);
                    EthCriterion srcMACcriterion = (EthCriterion) selector.getCriterion(Criterion.Type.ETH_SRC);
                    EthCriterion dstMACcriterion = (EthCriterion) selector.getCriterion(Criterion.Type.ETH_DST);
                    IPProtocolCriterion protocolCriterion = (IPProtocolCriterion) selector.getCriterion(Criterion.Type.IP_PROTO);
                    TcpPortCriterion srcPortCriterion = (TcpPortCriterion) selector.getCriterion(Criterion.Type.TCP_SRC);
                    TcpPortCriterion dstPortCriterion = (TcpPortCriterion) selector.getCriterion(Criterion.Type.TCP_DST);

                    long time = new Date().getTime();
                    long packets = entry.packets();
                    long bytes = entry.bytes();
                    long life = entry.life();

                    String srcIP = "";
                    String dstIP = "";
                    String srcMAC = "";
                    String dstMAC = "";
                    String srcPort = "";
                    String dstPort = "";
                    String protocol = "";
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
                    if (srcPortCriterion != null) {
                        srcPort = srcPortCriterion.tcpPort().toString();
                    }
                    if (dstPortCriterion != null) {
                        dstPort = dstPortCriterion.tcpPort().toString();
                    }
                    if (protocolCriterion != null) {
                        protocol = String.valueOf(protocolCriterion.protocol());
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
                    map.put("srcPort", srcPort);
                    map.put("dstPort", dstPort);
                    map.put("protocol", protocol);

                    res.add(map);
                }
                currStats.put(String.valueOf(srcId), res);
            }
            handler.handle(currStats);
        };
    }
}
