/*
 * Copyright 2020 Open Networking Laboratory
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apps.smartfwd.src.main.java;

import apps.smartfwd.src.main.java.constants.App;
import apps.smartfwd.src.main.java.constants.FlowEntryPriority;
import apps.smartfwd.src.main.java.constants.FlowEntryTimeout;
import apps.smartfwd.src.main.java.constants.Env;
import apps.smartfwd.src.main.java.models.SwitchPair;
import apps.smartfwd.src.main.java.task.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.onlab.packet.*;
import org.onlab.util.KryoNamespace;
import org.onosproject.core.CoreService;
import org.onosproject.net.*;
import org.onosproject.net.device.DeviceService;
import org.onosproject.net.edge.EdgePortService;
import org.onosproject.net.flow.*;
import org.onosproject.net.flowobjective.FlowObjectiveService;
import org.onosproject.net.group.GroupService;
import org.onosproject.net.host.HostService;
import org.onosproject.net.packet.PacketService;
import org.onosproject.net.statistic.PortStatisticsService;
import org.onosproject.net.topology.Topology;
import org.onosproject.net.topology.TopologyEdge;
import org.onosproject.net.topology.TopologyGraph;
import org.onosproject.net.topology.TopologyService;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static apps.smartfwd.src.main.java.TopologyDesc.INVALID_PORT;


/**
 * Skeletal ONOS application component.
 */
@Component(immediate = true)
public class AppComponent {

    private final  Logger logger = LoggerFactory.getLogger(getClass());

    protected static KryoNamespace appKryo = new KryoNamespace.Builder()
            .register(Integer.class)
            .register(DeviceId.class)
            .build("smartfwd");

    @Reference(cardinality = org.osgi.service.component.annotations.ReferenceCardinality.MANDATORY)
    protected CoreService coreService;
    @Reference(cardinality = org.osgi.service.component.annotations.ReferenceCardinality.MANDATORY)
    protected TopologyService topologyService;
    @Reference(cardinality = org.osgi.service.component.annotations.ReferenceCardinality.MANDATORY)
    protected PacketService packetService;
    @Reference(cardinality = org.osgi.service.component.annotations.ReferenceCardinality.MANDATORY)
    protected HostService hostService;
    @Reference(cardinality = org.osgi.service.component.annotations.ReferenceCardinality.MANDATORY)
    protected FlowObjectiveService flowObjectiveService;
    @Reference(cardinality = org.osgi.service.component.annotations.ReferenceCardinality.MANDATORY)
    protected FlowRuleService flowRuleService;
    @Reference(cardinality = org.osgi.service.component.annotations.ReferenceCardinality.MANDATORY)
    protected DeviceService deviceService;
    @Reference(cardinality = org.osgi.service.component.annotations.ReferenceCardinality.MANDATORY)
    protected EdgePortService edgePortService;
    @Reference(cardinality = org.osgi.service.component.annotations.ReferenceCardinality.MANDATORY)
    protected PortStatisticsService portStatisticsService;
    @Reference(cardinality = org.osgi.service.component.annotations.ReferenceCardinality.MANDATORY)
    protected GroupService groupService;

    BlockingQueue<FlowTableEntry> flowEntries=new LinkedBlockingQueue<>();



    FlowEntryTask flowEntryTask;
    SocketServerTask classifierServer;

    SocketServerTask.Handler classifierHandler= payload -> {
        //start new socket client
        logger.info("classifier payload {}",payload);
        TopologyDesc topo=TopologyDesc.getInstance();
        JsonNode specifierNode;
        JsonNode statsNode;
        try {
            ObjectMapper mapper=new ObjectMapper();
            JsonNode root=mapper.readTree(payload);
            specifierNode=root.get("specifier");
            statsNode=root.get("stats");
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return;
        }
        ObjectMapper mapper=new ObjectMapper();
        String newPayload;
        try {
            ObjectNode node=mapper.createObjectNode();
            node.put("stats",statsNode);
            newPayload=mapper.writeValueAsString(node)+"*";
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return;
        }
        SocketClientTask task=new SocketClientTask(newPayload, response -> {
            //parse response and install flow entry
            logger.info("classifier response {}",response);
            ObjectMapper m=new ObjectMapper();
            try {
                JsonNode resNode=m.readTree(response);
                int vlanId=resNode.get("res").asInt();
                int sport=specifierNode.get(0).asInt();
                int dport=specifierNode.get(1).asInt();
                String srcIp=specifierNode.get(2).asText();
                IpPrefix sip=IpAddress.valueOf(srcIp).toIpPrefix();
                String dstIp=specifierNode.get(3).asText();
                IpPrefix dip=IpAddress.valueOf(dstIp).toIpPrefix();

                String proto=specifierNode.get(4).asText();//TCP or UDP
                FlowTableEntry entry=new FlowTableEntry();
                DeviceId deviceId=topo.getConnectedDeviceFromIp(sip);
                if(null==deviceId) return;
                entry.setDeviceId(deviceId)
                        .setPriority(FlowEntryPriority.TABLE0_TAG_FLOW)
                        .setTable(0)
                        .setTimeout(FlowEntryTimeout.TAG_FLOW);
                entry.filter()
                        .setVlanId(5)
                        .setSrcIP(sip)
                        .setDstIP(dip)
                        .setDstPort(dport)
                        .setSport(sport)
                        .setProtocol((proto.equalsIgnoreCase("TCP")?IPv4.PROTOCOL_TCP: IPv4.PROTOCOL_UDP));
                entry.action()
                        .setVlanId(vlanId)
                        .setTransition(1);

                flowEntries.put(entry);
            } catch (JsonProcessingException | InterruptedException e) {
                e.printStackTrace();
            }
        },App.ALG_CLASSIFIER_IP,App.ALG_CLASSIFIER_PORT);
        task.start();
    };
    AtomicReference<List<Map<SwitchPair,Long>>>  collectedStats=new AtomicReference<>();
    AtomicReference<Map<SwitchPair,Long>> portRate=new AtomicReference<>();

    TrafficMatrixCollector.Handler trafficMatrixCollectorHandler=new TrafficMatrixCollector.Handler() {
        @Override
        public void handle(List<Map<SwitchPair, Long>> stats) {
            collectedStats.set(stats);
        }
    };
    TrafficMatrixCollector trafficMatrixCollector;
    PeriodicalSocketClientTask optRoutingRequestTask;
    PeriodicalSocketClientTask.ResponseHandler optRoutingRespHandler = resp -> {
        TopologyDesc topo=TopologyDesc.getInstance();
        ObjectMapper mapper=new ObjectMapper();
        try {
            JsonNode root=mapper.readTree(resp);
            for(int i=0;i<Env.N_FLOWS;i++){
                JsonNode node=root.get(String.valueOf(i));
                for(int k=0;k<node.size();k++){
                    JsonNode routing=node.get(k);
                    int start=routing.get(0).asInt();
                    int end=routing.get(routing.size()-1).asInt();
                    for(IpPrefix srcAddr:topo.getConnectedIps(topo.getDeviceId(start))){
                        for(IpPrefix dstAddr:topo.getConnectedIps(topo.getDeviceId(end))){
                            for(int j=0;j<routing.size()-1;j++){
                                int curr=routing.get(j).asInt();
                                int next=routing.get(j+1).asInt();
                                DeviceId currDeviceId=topo.getDeviceId(curr);
                                DeviceId nextHopDeviceId=topo.getDeviceId(next);
                                PortNumber output=topo.getConnectionPort(currDeviceId,nextHopDeviceId);
                                if(output.equals(INVALID_PORT)){
                                    continue;
                                    //todo log
                                }
                                FlowTableEntry entry=new FlowTableEntry();
                                entry.setDeviceId(currDeviceId)
                                        .setPriority(FlowEntryPriority.TABLE2_OPT_ROUTING)
                                        .setTable(2);
                                entry.filter()
                                        .setSrcIP(srcAddr)
                                        .setDstIP(dstAddr)
                                        .setVlanId(i);
                                entry.action()
                                        .setOutput(output);
                                flowEntries.put(entry);
                            }
                        }
                    }

                }
            }
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        catch (InterruptedException e) {
            logger.info("Interrupted producer");
            e.printStackTrace();
        }
    };
    PeriodicalSocketClientTask.RequestGenerator optRoutingReqGenerator=new PeriodicalSocketClientTask.RequestGenerator() {
        @Override
        public String payload() {
            List<Map<SwitchPair,Long>> traffic=collectedStats.get();
            if(null==traffic){
                return null;
            }
            TopologyDesc topo=TopologyDesc.getInstance();
            Map<String,ArrayList<Long>> content=new HashMap<>();
            for(int f=0;f<Env.N_FLOWS;f++){
                content.put(String.valueOf(f),new ArrayList<>());
                ArrayList<Long> stats=content.get(String.valueOf(f));
                for(int i=0;i<Env.N_SWITCH;i++){
                    for(int j=0;j<Env.N_SWITCH;j++){
                        if(i==j) continue;
                        stats.add(traffic.get(f).get(SwitchPair.switchPair(topo.getDeviceId(i),topo.getDeviceId(j))));
                    }
                }
            }
            ObjectMapper mapper=new ObjectMapper();
            String payload=null;
            try {
                    payload= mapper.writeValueAsString(content);
            } catch (JsonProcessingException e) {
                logger.info(e.getOriginalMessage());
                    e.printStackTrace();
            }
            return payload+"*";
        }
    };

    PortStatsCollectTask portRateCollector;
    PortStatsCollectTask.Consumer portRateConsumer=new PortStatsCollectTask.Consumer() {
        @Override
        public void consume(Map<SwitchPair, Long> stats) {
            portRate.set(stats);
        }
    };

    @Activate
    protected void activate() {
        logger.info("Activate started-------------");
        App.appId = coreService.registerApplication("org.chandler.smartfwd");
        Env.N_SWITCH=deviceService.getAvailableDeviceCount();

        logger.info("Populate cache");
        TopologyDesc.getInstance(deviceService,hostService,topologyService);
        logger.info("Populate done");
        logger.info("Init static flow table");
        init();
        //start flow entry install worker
        logger.info("Start Flow entry installation worker");
        flowEntryTask=new FlowEntryTask(flowEntries,flowRuleService);
        flowEntryTask.start();
        logger.info("Flow entry installation worker started");
//        //start flow classifier server;
        logger.info("Start socket server for flow classifier");
        classifierServer=new SocketServerTask(App.CLASSIFIER_LISTENING_PORT,classifierHandler);
        classifierServer.start();
        logger.info("Socket server for flow classifier started");
//
//        //start traffic collector
        logger.info("Start traffic matrix collector");
        trafficMatrixCollector=new TrafficMatrixCollector(flowRuleService,TopologyDesc.getInstance(),trafficMatrixCollectorHandler);
        trafficMatrixCollector.start();
        logger.info("Traffic matrix collector started");

        logger.info("Start Port Rate Collector");
        portRateCollector=new PortStatsCollectTask(portStatisticsService,topologyService,portRateConsumer);
        portRateCollector.start();
        logger.info("Port Rate collector start");
//        //start opt routing request;
        optRoutingRequestTask=new PeriodicalSocketClientTask(App.OPT_ROUTING_IP,App.OPT_ROUTING_PORT,optRoutingReqGenerator, optRoutingRespHandler);
        optRoutingRequestTask.start();
    }

    @Deactivate
    protected void deactivate() {
        App.getInstance().getPool().shutdownNow();
        App.getInstance().getPool().shutdownNow();
        App.getInstance().getScheduledPool().shutdownNow();
        flowEntryTask.stop();
        classifierServer.stop();
        trafficMatrixCollector.stop();
        optRoutingRequestTask.stop();
        flowRuleService.removeFlowRulesById(App.appId);
        logger.info("--------------------System Stopped-------------------------");
    }



    /**
     * 初始化系统启动所需的相关方法.
     */
    void init() {

        //在table0 安装与host直连switch的默认路由
        installFlowEntryToEndSwitch();
        logger.info("Flow entry for end host installed");
//        //在table0 安装已经打标签的流的流表
        installFlowEntryForLabelledTraffic();
        logger.info("Flow entry for labelled traffic installed");
//        //在table0 安装默认流表
        installDefaultFlowEntryToTable0();
        logger.info("Flow entry for default traffic installed");
//        //安装table1 用于统计流量矩阵
        installFlowEntryForMatrixCollection();
        logger.info("Flow entry for traffic matrix collection installed");
//
//        //安装IP到table2的流表项
        installDefaultRoutingToTable2();
        installDropActionToTable2();
        logger.info("Flow entry for drop packet installed");


        //每隔1s统计一次接入端口流数据
//        storeFlowRateMission();
//        getMatrixMission();
    }

    void installFlowEntryToEndSwitch() {
        for(Device device:deviceService.getAvailableDevices()){
            for(Host host:hostService.getConnectedHosts(device.id())) {
                for(IpAddress ipAddr:host.ipAddresses()){
                    PortNumber output=host.location().port();
                    FlowTableEntry entry=new FlowTableEntry();
                    entry.setTable(0)
                            .setPriority(FlowEntryPriority.TABLE0_HANDLE_LAST_HOP)
                            .setDeviceId(device.id());
                    entry.filter()
                            .setDstIP(ipAddr.toIpPrefix());
                    entry.action()
                            .setOutput(output);
                    entry.install(flowRuleService);
                }
            }
        }
        logger.debug("Host connection flow installed");
    }

    void installFlowEntryForLabelledTraffic(){
        for(Device device:deviceService.getAvailableDevices()){
            Set<Host> hosts=hostService.getConnectedHosts(device.id());
            for(int vlanId = 0; vlanId< Env.N_FLOWS; vlanId++){
                FlowTableEntry entry=new FlowTableEntry();
                entry.setPriority(FlowEntryPriority.TABLE0_HANDLE_TAGGED_FLOW)
                        .setTable(0)
                        .setDeviceId(device.id());
                entry.filter()
                        .setVlanId(vlanId);
                entry.action()
                        .setTransition(1);
                for(Host host:hosts){
                    for(IpAddress addr:host.ipAddresses()){
                        entry.filter()
                                .setSrcIP(addr.toIpPrefix());
                        entry.install(flowRuleService);
                    }
                }

            }
        }
    }
    void installDefaultFlowEntryToTable0(){
        for(Device device:deviceService.getAvailableDevices()){
            FlowTableEntry entry=new FlowTableEntry();
            entry.setTable(0)
                    .setPriority(FlowEntryPriority.TABLE0_DEFAULT)
                    .setDeviceId(device.id());
            entry.action()
                    .setTransition(2);
            entry.install(flowRuleService);
        }
    }

    void installDefaultRoutingToTable2(){
        TopologyDesc topo=TopologyDesc.getInstance();
        String req="default*";
        SocketClientTask.ResponseHandler responseHandler = payload -> {
            logger.info(payload);
            try{
                ObjectMapper mapper=new ObjectMapper();
                JsonNode root=mapper.readTree(payload);
                JsonNode routings=root.get("default");
                for(int i=0;i<routings.size();i++){
                    JsonNode routing=routings.get(i);
                    int start=routing.get(0).asInt();
                    int end=routing.get(routing.size()-1).asInt();
                    for(IpPrefix srcAddr:topo.getConnectedIps(topo.getDeviceId(start))){
                        for(IpPrefix dstAddr:topo.getConnectedIps(topo.getDeviceId(end))){
                            for(int j=0;j<routing.size()-1;j++){
                                int curr=routing.get(j).asInt();
                                int next=routing.get(j+1).asInt();
                                DeviceId currDeviceId=topo.getDeviceId(curr);
                                DeviceId nextHopDeviceId=topo.getDeviceId(next);
                                PortNumber output=topo.getConnectionPort(currDeviceId,nextHopDeviceId);
                                if(output.equals(INVALID_PORT)){
                                    continue;
                                    //todo log
                                }
                                FlowTableEntry entry=new FlowTableEntry();
                                entry.setDeviceId(currDeviceId)
                                        .setPriority(FlowEntryPriority.TABLE2_DEFAULT_ROUTING)
                                        .setTable(2);
                                entry.filter()
                                        .setSrcIP(srcAddr)
                                        .setDstIP(dstAddr);
                                entry.action()
                                        .setOutput(output);
                                entry.install(flowRuleService);
                            }
                        }
                    }


                }


            }
            catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        };
        SocketClientTask task=new SocketClientTask(req, responseHandler,App.DEFAULT_ROUTING_IP,App.DEFAULT_ROUTING_PORT);
        task.start();
    }
    void installDropActionToTable2(){
        for(Device device:deviceService.getAvailableDevices()){
            FlowTableEntry entry=new FlowTableEntry();
            entry.setDeviceId(device.id())
                    .setPriority(FlowEntryPriority.TABLE2_DROP)
                    .setTable(2);
            entry.action()
                    .setDrop(true);
            entry.install(flowRuleService);
        }
    }

    private void installFlowEntryForMatrixCollection() {
        for(Host srcHost:TopologyDesc.getInstance().getHosts()){
            DeviceId switchID=srcHost.location().deviceId();
            for(Host dstHost:TopologyDesc.getInstance().getHosts()){
                if(!srcHost.equals(dstHost)){
                    for(IpAddress srcIp:srcHost.ipAddresses()){
                        for(IpAddress dstIp:dstHost.ipAddresses()){
                            for(int vlanId=0;vlanId<Env.N_FLOWS;vlanId++){
                                FlowTableEntry entry=new FlowTableEntry();
                                entry.setDeviceId(switchID)
                                        .setTable(1)
                                        .setPriority(FlowEntryPriority.TABLE1_MATRIX_COLLECTION);
                                entry.filter()
                                        .setSrcIP(srcIp.toIpPrefix())
                                        .setDstIP(dstIp.toIpPrefix())
                                        .setVlanId(vlanId);
                                entry.action()
                                        .setTransition(2);
                                entry.install(flowRuleService);
                            }
                        }
                    }
                }
            }
        }
    }


    /**
     * 通过五元组信息以及分类信息安装流表项到table0.此流表项优先级最高.
     * @param srcPort
     * @param dstPort
     * @param srcIp
     * @param dstIP
     * @param protocol
     * @param vlanId
     * @param deviceId
     */
    private void installBy5Tuple(String srcPort, String dstPort, String srcIp, String dstIP, String protocol,
                                 short vlanId, DeviceId deviceId) {
        DefaultFlowRule.Builder ruleBuilder = DefaultFlowRule.builder();
        TrafficSelector.Builder selectBuilder = DefaultTrafficSelector.builder();
        selectBuilder.matchEthType(Ethernet.TYPE_IPV4)
                .matchVlanId(VlanId.ANY)
                .matchIPSrc(IpPrefix.valueOf(srcIp))
                .matchIPDst(IpPrefix.valueOf(dstIP));
        if (protocol.equals("UDP")) {
            selectBuilder.matchUdpSrc(TpPort.tpPort(Integer.parseInt(srcPort)))
                    .matchUdpDst(TpPort.tpPort(Integer.parseInt(dstPort)))
                    .matchIPProtocol(IPv4.PROTOCOL_UDP);
        }else {
            selectBuilder.matchTcpSrc(TpPort.tpPort(Integer.parseInt(srcPort)))
                    .matchTcpDst(TpPort.tpPort(Integer.parseInt(dstPort)))
                    .matchIPProtocol(IPv4.PROTOCOL_TCP);
        }
        TrafficTreatment.Builder trafficBuilder = DefaultTrafficTreatment.builder();
        trafficBuilder.setVlanId(VlanId.vlanId(vlanId))
                .transition(1);
        ruleBuilder.withSelector(selectBuilder.build())
                .withTreatment(trafficBuilder.build())
                .withPriority(55000)
                .forTable(0)
                .fromApp(App.appId)
                .withIdleTimeout(300)
                .forDevice(deviceId);
        FlowRuleOperations.Builder flowRuleBuilder = FlowRuleOperations.builder();
        flowRuleBuilder.add(ruleBuilder.build());
        flowRuleService.apply(flowRuleBuilder.build());
    }



//    public String getFlowRate() {
//      /*  Set<String> keySet = testSwMap.keySet();
//        JsonObject matrixRes = new JsonObject();
//        for(String key : keySet) {
//            DeviceId deviceId = testSwMap.get(key);
//            PortStatistics deltaStatisticsForPort = deviceService.getDeltaStatisticsForPort(deviceId, PortNumber.portNumber("1"));
//            long l = deltaStatisticsForPort.bytesReceived();
//            matrixRes.set(key, l);
//        }
//        return matrixRes.toString();*/
////        DeviceId deviceId = testSwMap.get("0");
////        PortStatistics deltaStatisticsForPort = deviceService.getDeltaStatisticsForPort(deviceId, PortNumber.portNumber("1"));
////        long l = deltaStatisticsForPort.bytesReceived();
////        return String.valueOf(l);
//    }


    private String getMaxEdgeRate() {
        try {
            Topology topology = topologyService.currentTopology();
            TopologyGraph graph = topologyService.getGraph(topology);
            Set<TopologyEdge> edges = graph.getEdges();
            ArrayList<Long> longs = new ArrayList<>();
            for (TopologyEdge edge : edges) {
                ConnectPoint src = edge.link().src();
                long rate = portStatisticsService.load(src).rate();
                longs.add(rate);
            }
            Collections.sort(longs);
            int size = longs.size();
            Long long1 = longs.get(size - 1);
            Double out1 = Double.parseDouble(String.valueOf(long1 * 8 / 10000)) * 0.01;
            return "Max:" + out1.toString() + "***";
        } catch (Exception e) {
            logger.info(e.toString());
            return "-------->>>>>>>" + e.toString();
        }
    }
}

