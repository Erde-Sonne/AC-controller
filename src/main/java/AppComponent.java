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
import apps.smartfwd.src.main.java.utils.Simulation;
import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
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
import org.onosproject.net.packet.InboundPacket;
import org.onosproject.net.packet.PacketContext;
import org.onosproject.net.packet.PacketPriority;
import org.onosproject.net.packet.PacketProcessor;
import org.onosproject.net.packet.PacketRequest;
import org.onosproject.net.packet.PacketService;
import org.onosproject.net.statistic.PortStatisticsService;
import org.onosproject.net.topology.Topology;
import org.onosproject.net.topology.TopologyEdge;
import org.onosproject.net.topology.TopologyGraph;
import org.onosproject.net.topology.TopologyService;
import org.onosproject.store.service.EventuallyConsistentMap;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static apps.smartfwd.src.main.java.TopologyDesc.INVALID_PORT;
import static apps.smartfwd.src.main.java.task.SocketClientTask.sendPost;


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
    HashSet<MacAddress> macAddrSet = new HashSet<>();

    FlowEntryTask flowEntryTask;
    SocketServerTask classifierServer;
    private ArrayList<FlowRule> defaultFlowRulesCache = new ArrayList<>();
    private ArrayList<FlowRule> optiFlowRulesCache = new ArrayList<>();
    String topoIdxJson = "{ \"topo_idx\" : 0}";
    String rateFilePath = "";

    private boolean recordMetrics = false;
    /** Enable IPv6 forwarding; default is false. */
    private boolean ipv6Forwarding = false;
    /** Ignore (do not forward) IPv4 multicast packets; default is false. */
    private boolean ignoreIPv4Multicast = false;
    /** Configure Flow Priority for installed flow rules; default is 10. */
    private int flowPriority = 10;
    /** Configure Flow Timeout for installed flow rules; default is 10 sec. */
    private int flowTimeout = 10;
    /** Enable first packet forwarding using OFPP_TABLE port instead of PacketOut with actual port; default is false. */
    private boolean packetOutOfppTable = false;
    private EventuallyConsistentMap<MacAddress, ReactiveForwardMetrics> metrics;


    PeriodicalSocketClientTask.RequestGenerator defaultIdxRouteReq = new PeriodicalSocketClientTask.RequestGenerator() {
        @Override
        public String payload() {
            return topoIdxJson + "*";
        }
    };
    PeriodicalSocketClientTask.ResponseHandler defaultIdxRouteHandler = new PeriodicalSocketClientTask.ResponseHandler() {
        @Override
        public void handle(String payload) {
            //clear default route
            logger.info("request default routing");
            emptyDefaultFlow();
            TopologyDesc topo=TopologyDesc.getInstance();
            try{
                ObjectMapper mapper=new ObjectMapper();
                JsonNode root=mapper.readTree(payload);
                JsonNode routings=root.get("res1");
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
                                FlowRule rule = entry.install(flowRuleService);
                                defaultFlowRulesCache.add(rule);
                            }
                        }
                    }


                }
                logger.info("---------default routing have been installed----------");
            }
            catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        }
    };

    SocketServerTask.Handler classifierHandler= payload -> {
        //start new socket client
//        logger.info("classifier payload {}",payload);
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
//            logger.info("classifier response {}",response);
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
    AtomicReference<Map<String,List<Map<String, String>>>> flowStatics = new AtomicReference<>();
    FlowStaticsCollector.Handler flowStaticsCollectorHandler = new FlowStaticsCollector.Handler() {
        @Override
        public void handle(Map<String,List<Map<String, String>>> stats) {
            flowStatics.set(stats);
            logger.info(stats.toString());
            ObjectMapper mapper = new ObjectMapper();
            try {
                String valueAsString = mapper.writeValueAsString(stats);
                String param = "data=" + valueAsString;
                logger.info(param);
                String sr= sendPost("http://192.168.1.136:8888/data/postData", param);
                logger.info(sr);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        }
    };
    FlowStaticsCollector flowStaticsCollector;


    TrafficMatrixCollector.Handler trafficMatrixCollectorHandler=new TrafficMatrixCollector.Handler() {
        @Override
        public void handle(List<Map<SwitchPair, Long>> stats) {
            collectedStats.set(stats);
        }
    };
    TrafficMatrixCollector trafficMatrixCollector;



    PeriodicalSocketClientTask.ResponseHandler optRoutingRespHandler = resp -> {
        writeTimeHeaderToFile("/data/theMaxRate.txt");
        int topo_idx = 0;
        try {
            ObjectMapper mapper=new ObjectMapper();
            JsonNode topoIdx = mapper.readTree(topoIdxJson);
            topo_idx = topoIdx.get("topo_idx").asInt();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        writeToFile("当前TOPO:" + topo_idx, "/data/theMaxRate.txt");
        writeToFile("优化前:" + getCurrentMaxRate().toString() + " Mbit/s", "/data/theMaxRate.txt");
        writeToFile("优化前:" + getAllRate(), "/data/matchRate.txt");
        emptyOptiFlow();
        TopologyDesc topo=TopologyDesc.getInstance();
        ObjectMapper mapper=new ObjectMapper();
        try {
            JsonNode root=mapper.readTree(resp);
            for(int i=0;i<Env.N_FLOWS;i++){
                JsonNode node=root.get("res" + String.valueOf(i+1));
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
                                FlowRule rule = entry.install(flowRuleService);
                                optiFlowRulesCache.add(rule);
                            }
                        }
                    }

                }
            }
            logger.info("---------opt routing have been installed----------");
            try {
                Thread.sleep(12000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            writeTimeHeaderToFile("/data/theMaxRate.txt");
            writeToFile("优化后：" + getCurrentMaxRate().toString() + " Mbit/s", "/data/theMaxRate.txt");
            writeToFile("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%", "/data/theMaxRate.txt");
            writeToFile("优化后：" + getAllRate(), "/data/matchRate.txt");
//            writeToFile("after", rateFilePath);
//            rateToFile(portRate, rateFilePath);
            try {
                Thread.sleep(150000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            emptyOptiFlow();

        } catch (JsonProcessingException e) {
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
                        SwitchPair switchPair = SwitchPair.switchPair(topo.getDeviceId(i),topo.getDeviceId(j));
                        Long res=traffic.get(f).get(switchPair);
                        if(res == null) {
                            logger.info("i:" + String.valueOf(i) + "j:" + j);
                            res = 0L;
                        }
                        stats.add(res);
                    }
                }
            }

//            logger.info(content.toString());
            JsonObject matrixRes = new JsonObject();
            JsonArray jsonArray = new JsonArray();
            for(int f = 0; f < Env.N_FLOWS; f++) {
                for(Long value : content.get(String.valueOf(f))) {
                    jsonArray.add(value);
                }
            }
            matrixRes.set("volumes", jsonArray);
            ObjectMapper mapper=new ObjectMapper();
            try {
                JsonNode topoIdx = mapper.readTree(topoIdxJson);
                int topo_idx = topoIdx.get("topo_idx").asInt();
                matrixRes.set("topo_idx", topo_idx);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            String payload=matrixRes.toString();
            return payload+"*";
        }
    };

    PortStatsCollectTask portRateCollector;
    PortStatsCollectTask.Consumer portRateConsumer=new PortStatsCollectTask.Consumer() {
        @Override
        public void consume(Map<SwitchPair, Long> stats) {
            portRate.set(stats);
            long time = new Date().getTime();
            rateFilePath = "/data/" + String.valueOf(time) + ".rate";
            rateToFile(portRate, rateFilePath);
            writeToFile(getAllRate(), "/data/allRate.txt");
        }
    };


    SocketClientTask.ResponseHandler connectionDownHandler = response -> {
        //清空优化路由流表项
        emptyOptiFlow();
        TopologyDesc topo=TopologyDesc.getInstance();
        ObjectMapper mapper=new ObjectMapper();
        try {
            JsonNode root=mapper.readTree(response);
            for(int i=0;i<Env.N_FLOWS;i++){
                JsonNode node=root.get("res" + String.valueOf(i+1));
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
                                FlowRule rule = entry.install(flowRuleService);
                                optiFlowRulesCache.add(rule);
                            }
                        }
                    }

                }
            }
            logger.info("---------connection down routing have been installed----------");
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    };

    SocketServerTask listenWebServerTask;
    SocketServerTask.Handler listenWebHandler = payload -> {
        logger.info("!!!!!!!!!!!!!!!!!!!!!!!!");
        logger.info("{}", payload);
        ObjectMapper mapper=new ObjectMapper();
        try {
            JsonNode dataJson=mapper.readTree(payload);
            if(dataJson == null) {
                logger.info("the json format error");
            }
            assert dataJson != null;
            String mac = dataJson.get("mac").asText();
            int status = dataJson.get("status").asInt();
//            int toInternet = dataJson.get("toInternet").asInt();
            int hostId = dataJson.get("hostId").asInt();
            logger.info("the mac address is -> " + mac);
            if(status == 1) {
                JsonNode node = dataJson.get("dstIds");
                if(node.isArray()) {
                    int size = node.size();
                    for(int i = 0; i < size; i++) {
                        int dst = node.get(i).asInt();
                        //安装到host的流表
                        installFlowEntryToEndSwitch(dst);
                        //计算出到资源服务器的路径
                        Deque<Integer> path = Dijkstra(Env.graph, hostId, dst);
                        logger.info(path.toString());
                        hostRouteToDst(new LinkedList<>(path), mac);
                        dstRouteToHost(new LinkedList<>(path), mac);
                        logger.info("you can visit the source server" + dst);
                    }

                }

            } else if (status == 2) {
                logger.info("----------reading set internet------------");
                Deque<Integer> dijkstraPath = Dijkstra(Env.graph, hostId, 0);
                logger.info(dijkstraPath.toString());
                natRouteToHost(new LinkedList<>(dijkstraPath),
                        mac, FlowEntryPriority.RESOURCE_DEFAULT_ROUTING - 1, 2);
                hostRouteToNat(new LinkedList<>(dijkstraPath), null,
                        FlowEntryPriority.RESOURCE_DEFAULT_ROUTING - 1, 2, mac);
                logger.info("can surf the Internet");

            }
//            if(toInternet == 1) {
//                //允许host连接外网
//                logger.info("hostId:" + hostId + "can be connected to Internet");
//                Deque<Integer> path = Dijkstra(Env.graph, hostId, 0);
//                hostRouteToNat(path, FlowEntryPriority.INTERNET_ROUTING);
//            }

        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

    };

    private ReactivePacketProcessor processor = new ReactivePacketProcessor();

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
        packetService.addProcessor(processor, PacketProcessor.director(1));

        requestIntercepts();

        listenWebServerTask = new SocketServerTask(App.WEB_LISTENING_IP, App.WEB_LISTENING_PORT, listenWebHandler);
        listenWebServerTask.start();
        logger.info("listen web socket started");

        flowStaticsCollector = new FlowStaticsCollector(flowRuleService, TopologyDesc.getInstance(), flowStaticsCollectorHandler);
        flowStaticsCollector.setInterval(20);
        flowStaticsCollector.start();
        logger.info("flowStatics started");

        //start flow entry install worker
//        logger.info("Start Flow entry installation worker");
//        flowEntryTask=new FlowEntryTask(flowEntries,flowRuleService);
//        flowEntryTask.start();
//        logger.info("Flow entry installation worker started");
////        //start flow classifier server;
//        logger.info("Start socket server for flow classifier");
//        classifierServer=new SocketServerTask(App.CLASSIFIER_LISTENING_IP, App.CLASSIFIER_LISTENING_PORT,classifierHandler);
//        classifierServer.start();
//        logger.info("Socket server for flow classifier started");

//        logger.info("Topo changed idx server start");
//        topoIdxServerTask = new SocketServerTask(App.TOPO_IDX_IP, App.TOPO_IDX_PORT, topoIdxHandler);
//        topoIdxServerTask.start();

////        //start traffic collector
//        logger.info("Start traffic matrix collector");
//        trafficMatrixCollector=new TrafficMatrixCollector(flowRuleService,TopologyDesc.getInstance(),trafficMatrixCollectorHandler);
//        trafficMatrixCollector.start();
//        logger.info("Traffic matrix collector started");
//
//        logger.info("Start Port Rate Collector");
//        portRateCollector=new PortStatsCollectTask(portStatisticsService,topologyService,portRateConsumer);
//        portRateCollector.start();
//        logger.info("Port Rate collector start");

//        optiCondition();

    }

    @Deactivate
    protected void deactivate() {
        App.getInstance().getPool().shutdownNow();
        App.getInstance().getScheduledPool().shutdownNow();
        packetService.removeProcessor(processor);
        flowRuleService.removeFlowRulesById(App.appId);
        listenWebServerTask.stop();
        processor = null;
        logger.info("--------------------System Stopped-------------------------");
    }


    /**
     * 初始化系统启动所需的相关方法.
     */
    void init() {

        //在table0 安装与host直连switch的默认路由
//        installFlowEntryToEndSwitch(0);
        logger.info("Flow entry for end host installed");
        installFlowEntryToNatSwitch();
//        //在table0 安装已经打标签的流的流表
//        installFlowEntryForLabelledTraffic();
//        logger.info("Flow entry for labelled traffic installed");
//        //在table0 安装默认流表
//        installDefaultFlowEntryToTable0();
//        logger.info("Flow entry for default traffic installed");
//        //安装table1 用于统计流量矩阵
//        installFlowEntryForMatrixCollection();
//        logger.info("Flow entry for traffic matrix collection installed");

        //安装IP到table2的流表项
//        installDefaultRoutingToTable2();
//        installDropActionToTable2();
//        logger.info("Flow entry for drop packet installed");


        //每隔1s统计一次接入端口流数据
//        storeFlowRateMission();
//        getMatrixMission();
    }

    void installFlowEntryToEndSwitch(int endSwitchId) {
//        for(Device device:deviceService.getAvailableDevices()){
//            for(Host host:hostService.getConnectedHosts(device.id())) {
//                for(IpAddress ipAddr:host.ipAddresses()){
//                    PortNumber output=host.location().port();

                    FlowTableEntry entry=new FlowTableEntry();
                    entry.setTable(0)
                            .setPriority(FlowEntryPriority.TABLE0_HANDLE_LAST_HOP)
                            .setDeviceId(TopologyDesc.getInstance().getDeviceId(endSwitchId));
                    entry.filter()
                            .setDstIP(IpPrefix.valueOf("10.0.0." + (endSwitchId + 1) + "/32"));
                    entry.action()
                            .setOutput(PortNumber.portNumber("1"));
                    entry.install(flowRuleService);


//        FlowTableEntry entry2=new FlowTableEntry();
//        entry2.setTable(0)
//                .setPriority(FlowEntryPriority.TABLE0_HANDLE_LAST_HOP)
//                .setDeviceId(TopologyDesc.getInstance().getDeviceId(9));
//        entry2.filter()
//                .setDstIP(IpPrefix.valueOf("10.0.0.20/32"));
//        entry2.action()
//                .setOutput(PortNumber.portNumber("2"));
//        entry2.install(flowRuleService);
//                }
//            }
//        }
        logger.debug("Host connection flow installed");
    }


    void installFlowEntryToNatSwitch() {
        DeviceId deviceId = TopologyDesc.getInstance().getDeviceId(0);
        FlowTableEntry entry=new FlowTableEntry();
        entry.setPriority(FlowEntryPriority.TABLE0_NAT_FLOW)
                .setTable(0)
                .setDeviceId(deviceId);
//        entry.filter()
//                .setDstIP(IpPrefix.valueOf("192.168.5.1/16"));
        entry.action()
                .setOutput(PortNumber.portNumber("2"));
        entry.install(flowRuleService);
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
        String req="{ \"topo_idx\" : 0}*";
        SocketClientTask.ResponseHandler responseHandler = payload -> {
//            logger.info(payload);
            try{
                ObjectMapper mapper=new ObjectMapper();
                JsonNode root=mapper.readTree(payload);
                JsonNode routings=root.get("res1");
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
                                FlowRule rule = entry.install(flowRuleService);
                                defaultFlowRulesCache.add(rule);
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

    /**
     * 清空默认流表项.
     */
    void emptyDefaultFlow() {
        if (defaultFlowRulesCache.size() != 0) {
            for (FlowRule flowRule : defaultFlowRulesCache) {
                flowRuleService.removeFlowRules(flowRule);
            }
            // 清空数据
            defaultFlowRulesCache.clear();
        }
        logger.info("---------have emptyed the default flowEntries----------------");
    }

    /**
     * 清空优化流表项.
     */
    void emptyOptiFlow() {
        if (optiFlowRulesCache.size() != 0) {
            for (FlowRule flowRule : optiFlowRulesCache) {
                flowRuleService.removeFlowRules(flowRule);
            }
            // 清空数据
            optiFlowRulesCache.clear();
        }
        logger.info("---------have emptyed the optimize flowEntries----------------");
    }

    public Double getCurrentMaxRate() {
        long max = 0L;
        Map<SwitchPair, Long> map = portRate.get();
        if(null == map) {
            return 0.0;
        }
        for(long rate : map.values()) {
            max = Math.max(max, rate);
        }
        return max * 8 / 1000000.0;
    }

    public String getAllRate() {
        ArrayList<Double> res = new ArrayList<>();
        Map<SwitchPair, Long> map = portRate.get();
        if(null == map) {
            return "";
        }
        for(long rate : map.values()) {
            res.add(rate * 8/ 1000000.0);
        }
        Collections.sort(res);
        return res.toString();
    }

    public void optiCondition() {
       /* PeriodicalSocketClientTask optRoutingRequestTask = new PeriodicalSocketClientTask(App.OPT_ROUTING_IP,
                App.OPT_ROUTING_PORT, optRoutingReqGenerator, optRoutingRespHandler);
        optRoutingRequestTask.setDelay(120).setInterval(200);
        optRoutingRequestTask.start();*/
    }

    /**
     * 模拟链路断掉，将断掉链路信息传输给算法模块，并下发新的路由.
     * @param downList
     */
    public void connectionDownReq(List<Integer> downList) {
        JsonObject sendInfo = new JsonObject();
        JsonArray jsonArray = new JsonArray();
        for(int i : downList) {
            jsonArray.add(i);
        }
        sendInfo.set("disconnectEdge", jsonArray);
        ObjectMapper mapper=new ObjectMapper();
        try {
            JsonNode topoIdx = mapper.readTree(topoIdxJson);
            int topo_idx = topoIdx.get("topo_idx").asInt();
            sendInfo.set("topo_idx", topo_idx);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        String payload = sendInfo.toString() + "*";
        SocketClientTask connectionDownTask = new SocketClientTask(payload, connectionDownHandler,
                App.C_DOWN_ROUTING_IP, App.C_DOWN_ROUTING_PORT);
        connectionDownTask.start();
    }

    /**
     * 一直等待topo发现完全.
     */
    void waitTopoDiscover() {
        int topoId = 0;
        int linksCount = 0;
        logger.info("---------------discover topo waiting.....-----------------------");
        while (true) {
            linksCount = topologyService.currentTopology().linkCount();
            try {
                JsonNode jsonNode = new ObjectMapper().readTree(topoIdxJson);
                topoId = jsonNode.get("topo_idx").intValue();
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (topoId % 2 == 0) {
                if (linksCount == 202) {
                    break;
                }
            } else {
                if (linksCount == 212) {
                    break;
                }
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        logger.info("--------topo discover complete------------");
    }

    public void rateToFile(AtomicReference<Map<SwitchPair,Long>> portRate, String path) {
        Map<SwitchPair, Long> map = portRate.get();
        if(null == map) {
            return;
        }
        Set<SwitchPair> switchPairs = map.keySet();
        for(SwitchPair switchPair : switchPairs) {
            DeviceId src = switchPair.src;
            DeviceId dst = switchPair.dst;
            TopologyDesc topologyDesc = TopologyDesc.getInstance();
            Integer srcId = topologyDesc.deviceIDToSwitchID.get(src);
            Integer dstId = topologyDesc.deviceIDToSwitchID.get(dst);
            Long aLong = map.get(switchPair);
            String out = srcId + " " + dstId + " " + aLong;
            writeToFile(out, path);
        }
    }

    /**
     * 把信息输出到文件.
     * @param content
     */
    public void writeToFile(String content, String filePath) {
        try {
            File file = new File(filePath);
            if(!file.exists()){
                file.createNewFile();
            }
            FileWriter fileWriter = new FileWriter(file.getAbsoluteFile(), true);
            BufferedWriter bw = new BufferedWriter(fileWriter);
            /*SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String format = df.format(new Date());
            String out = format + "-->>" + content + "\n";
            bw.write(out);*/
            bw.write(content + "\n");
            bw.close();
//            log.info("finished write to file");
        } catch (IOException e) {
            logger.error(e.toString());
        }
    }

    public void writeTimeHeaderToFile(String filePath) {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String format = df.format(new Date());
        writeToFile(format, filePath);
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


    public static Deque<Integer> Dijkstra(int[][] graph, int src, int dst) {
        int n = graph.length;
        int[] dist = new int[n];
        int[] prev = new int[n];
        dist[src] = 0;
        prev[src] = -1;
        PriorityQueue<int[]> Q = new PriorityQueue<>(new Comparator<int[]>() {
            @Override
            public int compare(int[] o1, int[] o2) {
                return o1[1] - o2[1];
            }
        });
        for(int v = 0; v < n; v++) {
            if(v != src) {
                dist[v] = Integer.MAX_VALUE;
                prev[v] = -1;
            }
            Q.offer(new int[]{v, dist[v]});
        }

        while (!Q.isEmpty()) {
            int[] poll = Q.poll();
            int u = poll[0];
            if(u == dst) {
                break;
            }

            for(int v = 0; v < n; v++) {
                if(graph[u][v] == 1) {
                    int alt = dist[u] + 1;
                    if(alt < dist[v]) {
                        dist[v] = alt;
                        prev[v] = u;
                        Q.offer(new int[] {v, alt});
                    }
                }
            }
        }
        Deque<Integer> deque = new LinkedList<>();
        if(prev[dst] != -1) {
            while (dst != -1) {
                deque.addFirst(dst);
                dst = prev[dst];
            }
        }
//        System.out.println(deque.toString());
        return deque;
    }


    // Sends a packet out the specified port.
    private void packetOut(PacketContext context, PortNumber portNumber) {
        context.treatmentBuilder().setOutput(portNumber);
        context.send();
    }


    // Indicated whether this is an IPv6 multicast packet.
    private boolean isIpv6Multicast(Ethernet eth) {
        return eth.getEtherType() == Ethernet.TYPE_IPV6 && eth.isMulticast();
    }

    // Indicates whether this is a control packet, e.g. LLDP, BDDP
    private boolean isControlPacket(Ethernet eth) {
        short type = eth.getEtherType();
        return type == Ethernet.TYPE_LLDP || type == Ethernet.TYPE_BSN;
    }


    public void sendMsgToServer(String req) {
        SocketClientTask.ResponseHandler responseHandler = payload -> {
            logger.info(payload);
        };
        SocketClientTask task=new SocketClientTask(req, responseHandler,App.Server_IP,App.Server_PORT);
        task.start();
    }

    public void hostRouteToNat(Deque<Integer> path, int flowPriority) {
        hostRouteToNat(path, null, flowPriority, 1, null);
    }

    public void hostRouteToNat(Deque<Integer> path, IpPrefix ipPrefix, int flowPriority, int mode, String mac) {
        TopologyDesc topo = TopologyDesc.getInstance();
        List<Integer> routing = new ArrayList<>();
        int size = path.size();
        for(int i = 0; i < size; i++) {
            routing.add(path.pollFirst());
        }
        for(int j=0;j<routing.size()-1;j++){
            int curr=routing.get(j);
            int next=routing.get(j+1);
            DeviceId currDeviceId=topo.getDeviceId(curr);
            DeviceId nextHopDeviceId=topo.getDeviceId(next);
            PortNumber output=topo.getConnectionPort(currDeviceId,nextHopDeviceId);
            if(output.equals(INVALID_PORT)){
                logger.info("the adjacent switch port error");
                continue;
                //todo log
            }
            FlowTableEntry entry=new FlowTableEntry();
            entry.setDeviceId(currDeviceId)
                    .setPriority(flowPriority)
                    .setTable(0);
            if(ipPrefix != null) {
                entry.filter()
                        .setDstIP(ipPrefix);
            }
            entry.action()
                    .setOutput(output);
            if(mode == 2) {
                entry.setTimeout(60);
                entry.filter()
                        .setSrcMac(MacAddress.valueOf(mac));
            }
            FlowRule rule = entry.install(flowRuleService);
            defaultFlowRulesCache.add(rule);
        }

    }


    public void hostRouteToDst(Deque<Integer> path, String mac) {
        TopologyDesc topo = TopologyDesc.getInstance();
        List<Integer> routing = new ArrayList<>();
        int size = path.size();
        for(int i = 0; i < size; i++) {
            routing.add(path.pollFirst());
        }
        Integer dst = routing.get(size - 1);
        for(int j=0;j<routing.size()-1;j++){
            int curr=routing.get(j);
            int next=routing.get(j+1);
            DeviceId currDeviceId=topo.getDeviceId(curr);
            DeviceId nextHopDeviceId=topo.getDeviceId(next);
            PortNumber output=topo.getConnectionPort(currDeviceId,nextHopDeviceId);
            if(output.equals(INVALID_PORT)){
                logger.info("the adjacent switch port error");
                continue;
                //todo log
            }
            FlowTableEntry entry=new FlowTableEntry();
            entry.setDeviceId(currDeviceId)
                    .setPriority(FlowEntryPriority.RESOURCE_DEFAULT_ROUTING)
                    .setTable(0);

            entry.filter()
                    .setSrcMac(MacAddress.valueOf(mac))
                    .setDstIP(IpPrefix.valueOf("10.0.0." + (dst + 1) + "/32"));

            entry.action()
                    .setOutput(output);
            entry.setTimeout(60);
            FlowRule rule = entry.install(flowRuleService);
            defaultFlowRulesCache.add(rule);
        }

    }

    public void dstRouteToHost(Deque<Integer> path, String mac) {
        TopologyDesc topo = TopologyDesc.getInstance();
        List<Integer> routing = new ArrayList<>();
        int size = path.size();
        for(int i = 0; i < size; i++) {
            routing.add(path.pollLast());
        }
        Integer dst = routing.get(0);

        for(int j=0;j<routing.size()-1;j++){
            int curr=routing.get(j);
            int next=routing.get(j+1);
            DeviceId currDeviceId=topo.getDeviceId(curr);
            DeviceId nextHopDeviceId=topo.getDeviceId(next);
            PortNumber output=topo.getConnectionPort(currDeviceId,nextHopDeviceId);
            if(output.equals(INVALID_PORT)){
                logger.info("the adjacent switch port error");
                continue;
                //todo log
            }
            FlowTableEntry entry=new FlowTableEntry();
            entry.setDeviceId(currDeviceId)
                    .setPriority(FlowEntryPriority.RESOURCE_DEFAULT_ROUTING)
                    .setTable(0);
            entry.filter()
                    .setDstMac(MacAddress.valueOf(mac))
                    .setSrcIP(IpPrefix.valueOf("10.0.0." + (dst + 1) + "/32"));
            entry.action()
                    .setOutput(output);
            entry.setTimeout(60);
            FlowRule rule = entry.install(flowRuleService);
            defaultFlowRulesCache.add(rule);
        }

    }



    public void natRouteToHost(Deque<Integer> path, String dstMac, int flowPriority, int mode) {
        TopologyDesc topo = TopologyDesc.getInstance();
        List<Integer> routing = new ArrayList<>();
        int size = path.size();
        for(int i = 0; i < size; i++) {
            routing.add(path.pollLast());
        }
        for(int j=0;j<routing.size()-1;j++){
            int curr=routing.get(j);
            int next=routing.get(j+1);
            DeviceId currDeviceId=topo.getDeviceId(curr);
            DeviceId nextHopDeviceId=topo.getDeviceId(next);
            PortNumber output=topo.getConnectionPort(currDeviceId,nextHopDeviceId);
            if(output.equals(INVALID_PORT)){
                logger.info("the adjacent switch port error");
                continue;
                //todo log
            }
            FlowTableEntry entry=new FlowTableEntry();
            entry.setDeviceId(currDeviceId)
                    .setPriority(flowPriority)
                    .setTable(0);
            entry.filter()
                    .setDstMac(MacAddress.valueOf(dstMac));
            entry.action()
                    .setOutput(output);
            if(mode == 2) {
                entry.setTimeout(60);
            }
            FlowRule rule = entry.install(flowRuleService);
            defaultFlowRulesCache.add(rule);
        }

    }

    public void installFlowEndSwitchToHost(MacAddress hostMac, DeviceId deviceId, PortNumber port) {
        FlowTableEntry entry=new FlowTableEntry();
        entry.setTable(0)
                .setPriority(FlowEntryPriority.TABLE0_HANDLE_LAST_HOP)
                .setDeviceId(deviceId);
        entry.filter()
                .setDstMac(hostMac);
        entry.action()
                .setOutput(port);
        entry.install(flowRuleService);
    }


    /**
     * Request packet in via packet service.
     */
    private void requestIntercepts() {
        TrafficSelector.Builder selector = DefaultTrafficSelector.builder();
        selector.matchEthType(Ethernet.TYPE_IPV4);
        packetService.requestPackets(selector.build(), PacketPriority.REACTIVE, App.appId);
    }


    /**
     * Packet processor responsible for forwarding packets along their paths.
     */
    private class ReactivePacketProcessor implements PacketProcessor {

        @Override
        public void process(PacketContext context) {
            // Stop processing if the packet has been handled, since we
            // can't do any more to it.
            if (context.isHandled()) {
                return;
            }

            InboundPacket pkt = context.inPacket();

            Ethernet ethPkt = pkt.parsed();

            if (ethPkt == null) {
                return;
            }
            //get source mac
            MacAddress macAddress = ethPkt.getSourceMAC();

            //if mac black set return
            if (Simulation.blackMacSet.contains(macAddress.toString())) {
                return;
            }
            // Bail if this is deemed to be a control packet.
            if (isControlPacket(ethPkt)) {
                return;
            }

            // Skip IPv6 multicast packet when IPv6 forward is disabled.
            if (!ipv6Forwarding && isIpv6Multicast(ethPkt)) {
                return;
            }

            HostId id = HostId.hostId(ethPkt.getDestinationMAC());

            // Do not process LLDP MAC address in any way.
            // do not process arp packet
            if (id.mac().isLldp()) {
                logger.info(pkt.receivedFrom().toString());
                logger.info("drop lldp packet");
                return;
            }

            if(ethPkt.getEtherType() == Ethernet.TYPE_ARP) {
//                logger.info(pkt.receivedFrom().toString());
//                logger.info("drop arp packet");
                return;
            }

            // Do not process IPv4 multicast packets, let mfwd handle them
            if (ignoreIPv4Multicast && ethPkt.getEtherType() == Ethernet.TYPE_IPV4) {
                if (id.mac().isMulticast()) {
                    return;
                }
            }
            if(ethPkt.getEtherType() == Ethernet.TYPE_IPV4) {
                logger.info("*********************************************");
                logger.info(pkt.receivedFrom().toString());
                //第一次packetIn会默认配置到网关的路由
                if(!macAddrSet.contains(macAddress)) {
                    logger.info("source mac addr:" + macAddress);
                    logger.info("install the routing to gate");
                    DeviceId deviceId = pkt.receivedFrom().deviceId();
                    int srcId = TopologyDesc.getInstance().getDeviceIdx(deviceId);
                    logger.info("srcid:" + srcId);
                    PortNumber port = pkt.receivedFrom().port();

                    installFlowEndSwitchToHost(macAddress, deviceId, port);
                    Deque<Integer> dijkstraPath = Dijkstra(Env.graph, srcId, 0);
                    logger.info(dijkstraPath.toString());
                    natRouteToHost(new LinkedList<>(dijkstraPath), macAddress.toString(),FlowEntryPriority.NAT_DEFAULT_ROUTING, 1);
//                hostRouteToNat(new LinkedList<>(dijkstraPath), FlowEntryPriority.NAT_DEFAULT_ROUTING);
                    hostRouteToNat(new LinkedList<>(dijkstraPath), IpPrefix.valueOf("192.168.1.136/24"), FlowEntryPriority.NAT_DEFAULT_ROUTING, 1, null);
                    macAddrSet.add(macAddress);
                }

                MacAddress destinationMAC = ethPkt.getDestinationMAC();
                DeviceId deviceId = pkt.receivedFrom().deviceId();
                int deviceIdx = TopologyDesc.getInstance().getDeviceIdx(deviceId);
                String param = "src=" + macAddress + "&dst=" + destinationMAC + "&switcher=" + deviceIdx;
                logger.info(param);
                String sr= sendPost("http://192.168.1.136:8888/user/verifyByMac", param);
                logger.info(sr);
            }
        }

    }

}


