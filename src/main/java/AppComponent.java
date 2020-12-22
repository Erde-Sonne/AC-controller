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
import apps.smartfwd.src.main.java.task.base.PeriodicalTask;
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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.text.SimpleDateFormat;
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
    private ArrayList<FlowRule> defaultFlowRulesCache = new ArrayList<>();
    private ArrayList<FlowRule> optiFlowRulesCache = new ArrayList<>();
    String topoIdxJson = "{ \"topo_idx\" : 0}";


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

    TrafficMatrixCollector.Handler trafficMatrixCollectorHandler=new TrafficMatrixCollector.Handler() {
        @Override
        public void handle(List<Map<SwitchPair, Long>> stats) {
            collectedStats.set(stats);
        }
    };
    TrafficMatrixCollector trafficMatrixCollector;



    PeriodicalSocketClientTask.ResponseHandler optRoutingRespHandler = resp -> {
//        writeToFile(getCurrentMaxRate().toString(), "/home/theMaxRate.txt");
//        writeToFile(getAllRate(), "/home/matchRate.txt");
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
            writeToFile(getCurrentMaxRate().toString(), "/home/theMaxRate.txt");
            writeToFile("---------------------------->>>", "/home/theMaxRate.txt");
            writeToFile(getAllRate(), "/home/matchRate.txt");
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
            writeToFile(getAllRate(), "/home/allRate.txt");
        }
    };


    SocketServerTask topoIdxServerTask;
    SocketServerTask.Handler topoIdxHandler = payload -> {
        logger.error("topo_idx info{}", payload);
        topoIdxJson = payload;
        waitTopoDiscover();
        TopologyDesc topo=TopologyDesc.getInstance();
        //update the port info
        topo.updateConnectionPort();

        SocketClientTask.ResponseHandler responseH = res -> {

        };
        SocketClientTask task=new SocketClientTask(topoIdxJson + "*", responseH,App.DEFAULT_ROUTING_IP,
                App.DEFAULT_ROUTING_PORT);
        task.start();


        PeriodicalSocketClientTask defaultIdxRouteClientTask = new PeriodicalSocketClientTask(App.DEFAULT_ROUTING_IP,
                App.DEFAULT_ROUTING_PORT, defaultIdxRouteReq, defaultIdxRouteHandler);
        defaultIdxRouteClientTask.setOneTime(true).setDelay(30);
        defaultIdxRouteClientTask.start();
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
        classifierServer=new SocketServerTask(App.CLASSIFIER_LISTENING_IP, App.CLASSIFIER_LISTENING_PORT,classifierHandler);
        classifierServer.start();
        logger.info("Socket server for flow classifier started");

        logger.info("Topo changed idx server start");
        topoIdxServerTask = new SocketServerTask(App.TOPO_IDX_IP, App.TOPO_IDX_PORT, topoIdxHandler);
        topoIdxServerTask.start();

//        //start traffic collector
        logger.info("Start traffic matrix collector");
        trafficMatrixCollector=new TrafficMatrixCollector(flowRuleService,TopologyDesc.getInstance(),trafficMatrixCollectorHandler);
        trafficMatrixCollector.start();
        logger.info("Traffic matrix collector started");

        logger.info("Start Port Rate Collector");
        portRateCollector=new PortStatsCollectTask(portStatisticsService,topologyService,portRateConsumer);
        portRateCollector.start();
        logger.info("Port Rate collector start");

        optiCondition();

    }

    @Deactivate
    protected void deactivate() {
        App.getInstance().getPool().shutdownNow();
        App.getInstance().getScheduledPool().shutdownNow();
        flowEntryTask.stop();
        classifierServer.stop();
        trafficMatrixCollector.stop();
        portRateCollector.stop();
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
        OptiConditionTask conditionTask = new OptiConditionTask(portRate, optRoutingReqGenerator, optRoutingRespHandler, 180);
        conditionTask.setInterval(10).start();
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
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String format = df.format(new Date());
            String out = format + "-->>" + content + "\n";
            bw.write(out);
            bw.close();
//            log.info("finished write to file");
        } catch (IOException e) {
            logger.error(e.toString());
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

    class OptiConditionTask extends PeriodicalTask {
        AtomicReference<Map<SwitchPair,Long>> portRate;
        long preReqTime = 0L;
        int interval;
//    PeriodicalSocketClientTask.RequestGenerator optRoutingReqGenerator;
//    PeriodicalSocketClientTask.ResponseHandler optRoutingRespHandler;

        public OptiConditionTask(AtomicReference<Map<SwitchPair,Long>>  rates,
                                 PeriodicalSocketClientTask.RequestGenerator optRoutingReqGenerator,
                                 PeriodicalSocketClientTask.ResponseHandler optRoutingRespHandler,
                                 int interval) {
            this.portRate = rates;
            this.interval = interval;
//        this.optRoutingReqGenerator = optRoutingReqGenerator;
//        this.optRoutingRespHandler = optRoutingRespHandler;
            this.worker = () -> {
                ArrayList<Double> res = new ArrayList<>();
                Map<SwitchPair, Long> map = this.portRate.get();
                if(null == map) {
                    return;
                }
                for(long rate : map.values()) {
                    res.add(rate * 8/ 1000000.0);
                }
                int cntBits = 0;
                for(Double rate : res) {
                    if(rate > 75.0) {
                        cntBits++;
                    }
                }
                long nowReqTime = new Date().getTime();
                long intervalTime = nowReqTime - preReqTime;
                if(cntBits >= 2) {
                    logger.info("cntBits:--->" + cntBits);
                    if(intervalTime >= this.interval * 1000) {
                        logger.info("<<<<<<----------------request the opt routing----------------->>>>>>>>>>");
                        writeToFile(getCurrentMaxRate().toString(), "/home/theMaxRate.txt");
                        writeToFile(getAllRate(), "/home/matchRate.txt");
                        PeriodicalSocketClientTask optRoutingRequestTask = new PeriodicalSocketClientTask(App.OPT_ROUTING_IP,
                                App.OPT_ROUTING_PORT, optRoutingReqGenerator, optRoutingRespHandler);
                        optRoutingRequestTask.setOneTime(true).setDelay(0);
                        optRoutingRequestTask.start();
                        preReqTime = nowReqTime;
                    }

                }

            };
        }
    }
}




