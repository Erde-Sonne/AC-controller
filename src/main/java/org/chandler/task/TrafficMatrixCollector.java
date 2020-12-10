package apps.smartfwd.src.main.java.org.chandler.task;

import apps.smartfwd.src.main.java.org.chandler.TopologyDesc;
import apps.smartfwd.src.main.java.org.chandler.constants.Env;
import apps.smartfwd.src.main.java.org.chandler.models.SwitchPair;
import org.onosproject.net.DeviceId;
import org.onosproject.net.flow.*;
import org.onosproject.net.flow.criteria.Criterion;
import org.onosproject.net.flow.criteria.IPCriterion;
import org.onosproject.net.flow.criteria.VlanIdCriterion;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TrafficMatrixCollector extends PeriodicalTask{
    public interface Handler{
        void handle(List<Map<SwitchPair,Long>> stats);
    }

    TopologyDesc desc;
    Handler handler;
    List<Map<SwitchPair,Long>> preStats=new ArrayList<>();
    List<Map<SwitchPair,Long>> currStats=new ArrayList<>();
    FlowRuleService flowRuleService;
    public TrafficMatrixCollector(FlowRuleService flowRuleService, TopologyDesc topo, Handler handler){
        this.desc=topo;
        this.handler=handler;
        this.flowRuleService=flowRuleService;
        //init
        //todo duplicate
        for(int i = 0; i< Env.N_FLOWS; i++){
            currStats.set(i, new HashMap<>());
            preStats.set(i,new HashMap<>());
        }

        this.worker=()->{
            //init
            List<Map<SwitchPair,Long>> res=new ArrayList<>(Env.N_FLOWS);
            //reset
            currStats.forEach(stats-> stats.replaceAll((k, v) -> 0L));

            for(DeviceId srcId:topo.getDeviceIds()){
                for(FlowEntry entry:flowRuleService.getFlowEntries(srcId)){
                    if(!entry.table().equals(IndexTableId.of(1))) continue;

                    TrafficSelector selector=entry.selector();
                    VlanIdCriterion vlanIdCriterion=(VlanIdCriterion)selector.getCriterion(Criterion.Type.VLAN_VID);
                    IPCriterion dstCriterion=(IPCriterion) selector.getCriterion(Criterion.Type.IPV4_DST);
                    DeviceId dstId=topo.getConnectedDeviceFromIp(dstCriterion.ip());

                    int vlanId=vlanIdCriterion.vlanId().toShort();
                    Map<SwitchPair,Long> stats=currStats.get(vlanId);
                    SwitchPair key=SwitchPair.switchPair(srcId,dstId);
                    stats.put(key,stats.getOrDefault(key,0L)+entry.bytes());
                }
            }
            //diff and replace
            for(int i = 0; i< Env.N_FLOWS; i++){
                Map<SwitchPair,Long> curr=currStats.get(i);
                Map<SwitchPair,Long> pre=preStats.get(i);
                Map<SwitchPair,Long> r=new HashMap<>();
                //diff and replace
                for(SwitchPair key:curr.keySet()){
                    r.put(key,curr.get(key)-pre.getOrDefault(key,0L));
                    pre.put(key,curr.get(key));
                }
                res.add(i,r);
            }
            handler.handle(res);
        };
    }

}
