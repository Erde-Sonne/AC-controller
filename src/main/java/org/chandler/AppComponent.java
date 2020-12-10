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
package apps.smartfwd.src.main.java.org.chandler;

import apps.smartfwd.src.main.java.org.chandler.constants.App;
import apps.smartfwd.src.main.java.org.chandler.constants.FlowEntryPriority;
import apps.smartfwd.src.main.java.org.chandler.constants.FlowEntryTimeout;
import apps.smartfwd.src.main.java.org.chandler.constants.Env;
import apps.smartfwd.src.main.java.org.chandler.models.SwitchPair;
import apps.smartfwd.src.main.java.org.chandler.task.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.onlab.packet.*;
import org.onlab.util.KryoNamespace;
import org.onosproject.core.ApplicationId;
import org.onosproject.core.CoreService;
import org.onosproject.net.*;
import org.onosproject.net.device.DeviceService;
import org.onosproject.net.device.PortStatistics;
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

import static apps.smartfwd.src.main.java.org.chandler.TopologyDesc.INVALID_PORT;


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
        private ApplicationId appId;
    private HashMap<String, DeviceId> testSwMap = HandleConfigFile.getSwMap();
    private HashMap<DeviceId, String> reverSwMap = HandleConfigFile.getReverswMap();
    private HashMap<String, IpPrefix> testHostIpMap = HandleConfigFile.getHostIpMap();
    private HashMap<String, MacAddress> testHostMacMap = HandleConfigFile.getHostMacMap();
    private HashMap<String, DeviceId> testip2swMap = HandleConfigFile.getIp2swMap();
    private int switchNum = HandleConfigFile.getSwithNum();
    private HashMap<String, PortNumber> stringPortNumberHashMap = new HashMap<>();
    private int flowTableCnt = 0;
    private int timesCnt = 0;

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

    private ArrayList<TopologyEdge> edgeArrayList = new ArrayList<>();


    FlowEntryTask flowEntryTask;
    SocketServerTask classifierServer;

    SocketServerTask.Handler classifierHandler= payload -> {
        //start new socket client and start;
        SocketClientTask task=new SocketClientTask(payload, response -> {
            //parse response and install flow entry

        },App.ALG_CLASSIFIER_IP,App.ALG_CLASSIFIER_PORT);
        task.start();
    };
    AtomicReference<List<Map<SwitchPair,Long>>>  collectedStats;

    TrafficMatrixCollector.Handler trafficMatrixCollectorHandler=new TrafficMatrixCollector.Handler() {
        @Override
        public void handle(List<Map<SwitchPair, Long>> stats) {
            collectedStats.set(stats);
        }
    };
    TrafficMatrixCollector trafficMatrixCollector;

    PeriodicalSocketClientTask optRoutingRequestTask;
    PeriodicalSocketClientTask.ResponseHandler optRoutingRespHandler = resp -> {

    };
    PeriodicalSocketClientTask.RequestGenerator optRoutingReqGenerator=new PeriodicalSocketClientTask.RequestGenerator() {
        @Override
        public String payload() {
            List<Map<SwitchPair,Long>> stats=collectedStats.get();
            if(null==stats){
                return null;
            }

            return null;
        }
    };

    @Activate
    protected void activate() {
        logger.info("Activate started-------------");
        appId = coreService.registerApplication("org.chandler.smartfwd");
        Env.N_SWITCH=deviceService.getAvailableDeviceCount();

        logger.info("Populate cache");
        TopologyDesc.getInstance(deviceService,hostService,topologyService);
        logger.info("Populate done");
//        init();
//        //start flow entry install worker
//        flowEntryTask=new FlowEntryTask(flowEntries,flowRuleService);
//        flowEntryTask.start();
//        //start flow classifier server;
//        classifierServer=new SocketServerTask(App.CLASSIFIER_LISTENING_PORT,classifierHandler);
//        classifierServer.start();
//
//        //start traffic collector
//        trafficMatrixCollector=new TrafficMatrixCollector(flowRuleService,TopologyDesc.getInstance(),trafficMatrixCollectorHandler);
//        trafficMatrixCollector.start();
//        //start opt routing request;
//        optRoutingRequestTask=new PeriodicalSocketClientTask(App.OPT_ROUTING_IP,App.OPT_ROUTING_PORT,optRoutingReqGenerator, optRoutingRespHandler);
//        optRoutingRequestTask.start();




//        init();


    }

    @Deactivate
    protected void deactivate() {
        flowRuleService.removeFlowRulesById(App.appId);

        logger.info("--------------------System Stopped-------------------------");
    }



    /**
     * 初始化系统启动所需的相关方法.
     */
    void init() {

        //在table0 安装与host直连switch的默认路由
        installFlowEntryToEndSwitch();
        //在table0 安装已经打标签的流的流表
        installFlowEntryForLabelledTraffic();
        //在table0 安装默认流表
        installDefaultFlowEntryToTable0();
        //安装table1 用于统计流量矩阵
        installFlowEntryForMatrixCollection();

        //安装IP到table2的流表项
//        installIP2Table2();
        installDefaultRoutingToTable2();
        installDropActionToTable2();

        //每隔5s上传一次流量矩阵
//        storeMatrixMission();
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


    void installSingleOptRoutingToTable2(List<Integer> routing,int vlanId){
        //[1,2,3,4]
        TopologyDesc desc=TopologyDesc.getInstance();
        DeviceId start=desc.getDeviceId(routing.get(0));
        DeviceId end=desc.getDeviceId(routing.get(routing.size()-1));

        for(Host srcHost:hostService.getConnectedHosts(start)){
            for(Host dstHost:hostService.getConnectedHosts(end)){
                for(IpAddress srcAddr:srcHost.ipAddresses()){
                    for(IpAddress dstAddr:dstHost.ipAddresses()){
                        for(int i=0;i<routing.size()-1;i++){
                            DeviceId curr=desc.getDeviceId(routing.get(i));
                            DeviceId nextHop=desc.getDeviceId(routing.get(i+1));
                            FlowTableEntry entry=new FlowTableEntry();
                            PortNumber output=desc.getConnectionPort(curr,nextHop);
                            if(output.equals(INVALID_PORT)){
                                //todo log
                                continue;
                            }
                            //check output
                            entry.setTable(2)
                                    .setPriority(FlowEntryPriority.TABLE2_OPT_ROUTING)
                                    .setDeviceId(curr)
                                    .setTimeout(FlowEntryTimeout.OPT_ROUTING);
                            entry.filter()
                                    .setVlanId(vlanId)
                                    .setSrcIP(srcAddr.toIpPrefix())
                                    .setDstIP(dstAddr.toIpPrefix());
                            entry.action()
                                    .setOutput(output);
                            try{
                                flowEntries.put(entry);
                            }catch (InterruptedException e){
                                //todo log
                                e.printStackTrace();
                            }
                        }
                    }
                }
            }

        }

    }
    void installOptRoutingToTable2(ArrayList<ArrayList<Integer>> routings,int vlanId){
        //[1,2,3,4]
        for(List<Integer> routing:routings){
            installSingleOptRoutingToTable2(routing,vlanId);
        }
        //todo log
    }


    private void installFlowEntryForMatrixCollection() {
        for(Host srcHost:TopologyDesc.getInstance().getHosts()){
            DeviceId switchID=srcHost.location().deviceId();
            for(Host dstHost:TopologyDesc.getInstance().getHosts()){
                if(!srcHost.equals(dstHost)){
                    for(IpAddress srcIp:srcHost.ipAddresses()){
                        for(IpAddress dstIp:dstHost.ipAddresses()){
                            for(int vlanId=0;vlanId<4;vlanId++){
                                FlowTableEntry entry=new FlowTableEntry();
                                entry.setDeviceId(switchID)
                                        .setTable(1)
                                        .setPriority(60000);
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



    public String getFlowRate() {
      /*  Set<String> keySet = testSwMap.keySet();
        JsonObject matrixRes = new JsonObject();
        for(String key : keySet) {
            DeviceId deviceId = testSwMap.get(key);
            PortStatistics deltaStatisticsForPort = deviceService.getDeltaStatisticsForPort(deviceId, PortNumber.portNumber("1"));
            long l = deltaStatisticsForPort.bytesReceived();
            matrixRes.set(key, l);
        }
        return matrixRes.toString();*/
        DeviceId deviceId = testSwMap.get("0");
        PortStatistics deltaStatisticsForPort = deviceService.getDeltaStatisticsForPort(deviceId, PortNumber.portNumber("1"));
        long l = deltaStatisticsForPort.bytesReceived();
        return String.valueOf(l);
    }


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

    /**
     * 获取当前topo中链路速率前3的链路.
     * @return
     */
    private String getTop3EdgeRate() {
        try {
            Topology topology = topologyService.currentTopology();
            TopologyGraph graph = topologyService.getGraph(topology);
            Set<TopologyEdge> edges = graph.getEdges();
            ArrayList<Long> longs = new ArrayList<>();
            HashMap<Long, TopologyEdge> hashMap = new HashMap<>();
            for (TopologyEdge edge : edges) {
                ConnectPoint src = edge.link().src();
                long rate = portStatisticsService.load(src).rate();
                longs.add(rate);
                hashMap.put(rate, edge);
            }
            Collections.sort(longs);
            int size = longs.size();
            Long long1 = longs.get(size - 1);
            Long long2 = longs.get(size - 2);
            Long long3 = longs.get(size - 3);
            edgeArrayList.add(hashMap.get(long1));
            edgeArrayList.add(hashMap.get(long2));
            edgeArrayList.add(hashMap.get(long3));
            Double out1 = Double.parseDouble(String.valueOf(long1 * 8 / 10000)) * 0.01;
            Double out2 = Double.parseDouble(String.valueOf(long2 * 8 / 10000)) * 0.01;
            Double out3 = Double.parseDouble(String.valueOf(long3 * 8 / 10000)) * 0.01;
            return "rate1:" + out1.toString() + "  rate2:" +
                    out2.toString() + "  rate3:" + out3.toString() + "***";
        } catch (Exception e) {
            logger.info(e.toString());
            return "-------->>>>>>>" + e.toString();
        }
    }

    /**
     * 通过edge获取当前链路所连接的端口速率.
     * @param edge
     * @return
     */
    private String getRateByEdge(TopologyEdge edge){
        try {
            ConnectPoint src = edge.link().src();
            ConnectPoint dst = edge.link().dst();
            long srcRate = portStatisticsService.load(src).rate();
            long dstRate = portStatisticsService.load(dst).rate();
            Double srcOut = Double.parseDouble(String.valueOf(srcRate * 8 / 10000)) * 0.01;
            Double dstOut = Double.parseDouble(String.valueOf(dstRate * 8 / 10000)) * 0.01;

            return "srcRate:" + srcOut.toString() +
                    "   dstRate:" + dstOut.toString() + "*************";
        } catch (Exception e) {
            logger.info(e.toString());
            return "-------->>>>>>>" + e.toString();
        }
    }

    /**
     * 获取链路的速率.
     */
    private String getEdgeSpeed() {
        logger.info("??????????????????????????");
        try {
            Topology topology = topologyService.currentTopology();
            TopologyGraph graph = topologyService.getGraph(topology);
            Set<TopologyEdge> edges = graph.getEdges();
//        JsonObject jsonObject = new JsonObject();
            ArrayList<Double> doubles = new ArrayList<>();
            for (TopologyEdge edge : edges) {
                ConnectPoint src = edge.link().src();
//                ConnectPoint dst = edge.link().dst();
//                String srcId = reverSwMap.get(src.deviceId());
//                String dstId = reverSwMap.get(dst.deviceId());
                long rate = portStatisticsService.load(src).rate();
//                String key = srcId + "->" + dstId;
//            jsonObject.set(key, rate/1000000);
                Double out = Double.parseDouble(String.valueOf(rate * 8 / 10000)) * 0.01;
                doubles.add(out);
            }
            int size = doubles.size();
            Double sum = 0d;
            for (Double aDouble : doubles) {
                sum += aDouble;
            }
            Double avgSpeed = sum / size;
            Collections.sort(doubles);
            Double minSpeed = doubles.get(0);
            Double maxSpeed = doubles.get(size - 1);

//            List<Double> doubles1 = doubles.subList(doubles.size() - 61, doubles.size() - 1);
//            longs.clear();
//            return doubles1.toString();
            return doubles.toString() + "\n" +
                    "   average:" + avgSpeed +
                    "    min:" + minSpeed +
                    "    max:" + maxSpeed + "*************";
        } catch (Exception e) {
            logger.info(e.toString());
            return "-------->>>>>>>" + e.toString();
        }
    }

    /**
     * 定时显示链路速度.
     */
//    private void showEdgeSpeed(int interval) {
//        timer.schedule(new TimerTask() {
//            @Override
//            public void run() {
//                String edgeSpeed = getEdgeSpeed();
//                logger.info("==============================================");
//                logger.info(edgeSpeed);
//            }
//        }, 1000 * interval, 1000 * interval);
//    }

    /**
     * 获取两个优化路由时间段内经过默认路由的数据量.
     */
    private HashMap<String, ArrayList<Long>> getDefaultStatics() {
        Set<String> keySet = testSwMap.keySet();
        HashMap<String, ArrayList<Long>> hashMap = new HashMap<>();
        for (String key : keySet) {
            ArrayList<Long> longArrayList = new ArrayList<>();
            DeviceId deviceId = testSwMap.get(key);
            Iterable<FlowEntry> flowEntries = flowRuleService.getFlowEntries(deviceId);
            Iterator<FlowEntry> iterator = flowEntries.iterator();
            while (iterator.hasNext()) {
                FlowEntry flowEntry = iterator.next();
                if (flowEntry.tableId() == 0 && flowEntry.priority() == 50000) {
                    long bytes = flowEntry.bytes();
                    long packets = flowEntry.packets();
                    longArrayList.add(bytes);
                    longArrayList.add(packets);
                }
            }
            hashMap.put(key, longArrayList);
        }
        return hashMap;
    }

//    /**
//     * 将新得到的json数据与原json数据作差值，得到数据的增量.
//     * @return
//     */
//    private String subObjStatics(HashMap<String, ArrayList<Long>> map, HashMap<String, ArrayList<Long>> oldMap) {
//        Set<String> keySet = map.keySet();
//        JsonObject jsonObject = new JsonObject();
//        for (String key : keySet) {
//            JsonArray jsonArray = new JsonArray();
//            ArrayList<Long> longs = map.get(key);
//            ArrayList<Long> oldLongs = oldMap.get(key);
//            long bytes = longs.get(0) - oldLongs.get(0);
//            long packets = longs.get(1) - oldLongs.get(1);
//            jsonArray.add(bytes);
//            jsonArray.add(packets);
//            jsonObject.set(key, jsonArray);
//        }
//        //更新oldStatics
////        oldStatics.clear();
//        oldStatics.putAll(map);
//        return jsonObject.toString();
//    }

    /**
     * 上传流量矩阵线程，并且获取返回的优化路由信息.
     */
    public class FlowMarixThread implements Runnable {
        private String matrixMap;
        public FlowMarixThread(String matrixMap) {
            this.matrixMap = matrixMap;
        }
        @Override
        public void run() {
            try {
                SocketChannel socketChannel = SocketChannel.open();
                socketChannel.connect(new InetSocketAddress("192.168.1.132", 1030));
//            socketChannel.connect(new InetSocketAddress("172.16.181.1", 1027));
                ByteBuffer byteBuffer = ByteBuffer.allocate(512 * 1024);
                byteBuffer.put(matrixMap.getBytes());
                byteBuffer.flip();
                socketChannel.write(byteBuffer);
                while (byteBuffer.hasRemaining()) {
                    socketChannel.write(byteBuffer);
                }
                byteBuffer.clear();
                //接收数据
                int len = 0;
                StringBuilder stringBuilder = new StringBuilder();
                while ((len = socketChannel.read(byteBuffer)) >= 0) {
                    byteBuffer.flip();
                    String res = new String(byteBuffer.array(), 0, len);
                    byteBuffer.clear();
                    stringBuilder.append(res);
                }
                String out = stringBuilder.toString();
                logger.info(out);
//                flowEntries.offer(out);
                socketChannel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

//    /**
//     * 通过获取优化路由信息下发流表项.
//     */
//    public  class RoutingFlowThread implements Runnable {
//        @Override
//        public void run() {
//            // 线程会一直运行,除非被中断掉.
//            while (!Thread.currentThread().isInterrupted()) {
//                while (!flowEntries.isEmpty()) {
//                    logger.info("-----------install routing flow entries---------------");
////                    String routingInfo = flowEntries.poll();
////                    throughStrInstallFlow(routingInfo);
//                    logger.info("-------------------optimizing routing flow installed---------------------------");
//                    try {
//                        Thread.sleep(6000);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                    if (edgeArrayList.size() != 0) {
//                        for (TopologyEdge edge : edgeArrayList) {
//                            String rateByEdge = getRateByEdge(edge);
//                            writeToFile(rateByEdge, "/home/top3rate.txt");
//                        }
//                    }
//                    writeToFile("\n\n", "/home/top3rate.txt");
//                    edgeArrayList.clear();
//                    String maxEdgeRate = getMaxEdgeRate();
//                    writeToFile(maxEdgeRate, "/home/something.txt");
//                    writeToFile("\n\n", "/home/something.txt");
////            String edgeSpeed = getEdgeSpeed();
////            writeToFile(edgeSpeed, "/home/something.txt");
////            log.info(edgeSpeed);
//           /* HashMap<String, ArrayList<Long>> statics = getDefaultStatics();
//            String staticsOut = subObjStatics(statics, oldStatics);
//            log.info(staticsOut);*/
//                }
//            }
//        }
//    }





//    /**
//     * 链路断掉处理的线程.
//     */
//    public class ConnectionDownThread implements Runnable {
//        @Override
//        public void run() {
//            try {
//                logger.info("--------------connection down and request--------------");
//                SocketChannel socketChannel = SocketChannel.open();
//                socketChannel.connect(new InetSocketAddress("192.168.1.196", 1028));
//                ByteBuffer byteBuffer = ByteBuffer.allocate(2048);
//                //发送断掉的链路以及topo信息
//                JsonObject sendInfo = new JsonObject();
//                JsonArray jsonArray = new JsonArray();
//                jsonArray.add(0);
//                jsonArray.add(1);
//                jsonArray.add(1);
//                jsonArray.add(0);
//                sendInfo.set("disconnectEdge", jsonArray);
//                try {
//                    JsonNode jsonNode = new ObjectMapper().readTree(topoIdx.substring(0, topoIdx.length() - 1));
//                    JsonNode topoid = jsonNode.get("topo_idx");
//                    sendInfo.set("topo_idx", topoid.intValue());
//                } catch (JsonProcessingException e) {
//                    e.printStackTrace();
//                }
//                String s = sendInfo.toString() + "*";
//                byteBuffer.put(s.getBytes());
//                byteBuffer.flip();
//                socketChannel.write(byteBuffer);
//                while (byteBuffer.hasRemaining()) {
//                    socketChannel.write(byteBuffer);
//                }
//                byteBuffer.clear();
//
//                //接收数据
//                int len = 0;
//                StringBuilder stringBuilder = new StringBuilder();
//                while ((len = socketChannel.read(byteBuffer)) >= 0)  {
//                    byteBuffer.flip();
//                    String res = new String(byteBuffer.array(), 0, len);
//                    byteBuffer.clear();
//                    stringBuilder.append(res);
//                }
////                flowEntries.offer(stringBuilder.toString());
//                socketChannel.close();
//
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//
//        }
//    }
//    /**
//     * 接收topo的id信息.
//     */
//    public class TopoIdxThread implements Runnable {
//        @Override
//        public void run() {
//            ServerSocketChannel serverSocketChannel = null;
//            Selector selector = null;
//            ByteBuffer buffer = ByteBuffer.allocate(1024);
//            logger.info("-------topoidxthread   running----------");
//            try {
//                serverSocketChannel = ServerSocketChannel.open();
//                serverSocketChannel.bind(new InetSocketAddress("0.0.0.0", 1029));
//                serverSocketChannel.configureBlocking(false);
//                selector = Selector.open();
//                serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
//                while (selector.select() > 0) {
//                    Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
//                    while (iterator.hasNext()) {
//                        SelectionKey next = iterator.next();
//                        iterator.remove();
//                        if (next.isAcceptable()) {
//                            SocketChannel accept = serverSocketChannel.accept();
//                            accept.configureBlocking(false);
//                            accept.register(selector, SelectionKey.OP_READ);
//                        } else if (next.isReadable()) {
//                            SocketChannel channel = (SocketChannel) next.channel();
//                            int len = 0;
//                            StringBuilder stringBuilder = new StringBuilder();
//                            while ((len = channel.read(buffer)) >= 0) {
//                                buffer.flip();
//                                String res = new String(buffer.array(), 0, len);
//                                buffer.clear();
//                                stringBuilder.append(res);
//                            }
//                            //清空优化路由
////                        emptyOptiFlow();
//                            // 接收topo信息
//                            topoIdx = stringBuilder.toString() + "*";
//                            logger.info("=========" + topoIdx + "==========");
//                            //发送请求默认路由信息，但不对接收数据进行处理
//                            sendDefaultReqMessage();
//                            // topo变化35s后请求默认路由
//                            DefaultRouterMission(45);
//                            // 等待topo发现完全
//                            waitTopoDiscover();
//                            //重新设置switch间的端口信息
////                            setPortInfo();
//                     /*   try {
//                            Thread.sleep(10000);
//                        } catch (InterruptedException e) {
//                            e.printStackTrace();
//                        }*/
////                        DisConnectionMission(65);
//                            //请求优化路由
//                            optiRouterMission(15, 60, 0);
//                            channel.close();
//                        }
//                    }
//
//                }
//            } catch (IOException e) {
//                e.printStackTrace();
//            } finally {
//                // 释放相关的资源
//                if (selector != null) {
//                    try {
//                        selector.close();
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                }
//                if (serverSocketChannel != null) {
//                    try {
//                        serverSocketChannel.close();
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                }
//            }
//        }
//    }
}

