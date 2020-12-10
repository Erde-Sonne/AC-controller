package apps.smartfwd.src.main.java.org.chandler.task;

import apps.smartfwd.src.main.java.org.chandler.models.SwitchPair;
import org.onosproject.net.ConnectPoint;
import org.onosproject.net.statistic.PortStatisticsService;
import org.onosproject.net.topology.Topology;
import org.onosproject.net.topology.TopologyEdge;
import org.onosproject.net.topology.TopologyGraph;
import org.onosproject.net.topology.TopologyService;

import java.util.HashMap;
import java.util.Map;

public class PortStatsCollectTask extends PeriodicalTask {
    PortStatisticsService portStatisticsService;
    TopologyService topologyService;
    public interface Consumer{
        void consume(Map<SwitchPair,Long> stats);
    }
    Consumer consumer;
    public PortStatsCollectTask(PortStatisticsService portStatisticsService, TopologyService topologyService,Consumer consumer){
        this.portStatisticsService=portStatisticsService;
        this.topologyService=topologyService;
        this.consumer=consumer;
        this.worker=()->{
            Map<SwitchPair,Long> res=new HashMap<>();
            Topology topo=topologyService.currentTopology();
            TopologyGraph graph=topologyService.getGraph(topo);
            for(TopologyEdge edge:graph.getEdges()){
                ConnectPoint src=edge.link().src();
                ConnectPoint dst=edge.link().dst();
                long rate1=this.portStatisticsService.load(src).rate();
                long rate2=this.portStatisticsService.load(dst).rate();
                res.put(SwitchPair.switchPair(src.deviceId(),dst.deviceId()),rate1);
                res.put(SwitchPair.switchPair(dst.deviceId(),src.deviceId()),rate2);
            }
            consumer.consume(res);
        };
    }

}
