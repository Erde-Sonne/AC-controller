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
import apps.smartfwd.src.main.java.constants.Env;
import apps.smartfwd.src.main.java.models.SwitchPair;
import apps.smartfwd.src.main.java.task.*;
import apps.smartfwd.src.main.java.task.base.AbstractStoppableTask;
import apps.smartfwd.src.main.java.utils.Simulation;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.onlab.packet.*;
import org.onlab.util.KryoNamespace;
import org.onosproject.core.CoreService;
import org.onosproject.net.*;
import org.onosproject.net.device.DeviceService;
import org.onosproject.net.edge.EdgePortService;
import org.onosproject.net.flow.*;
import org.onosproject.net.flow.criteria.Criterion;
import org.onosproject.net.flow.criteria.EthCriterion;
import org.onosproject.net.flow.criteria.IPCriterion;
import org.onosproject.net.flowobjective.FlowObjectiveService;
import org.onosproject.net.group.GroupService;
import org.onosproject.net.host.HostService;
import org.onosproject.net.packet.InboundPacket;;
import org.onosproject.net.packet.PacketContext;
import org.onosproject.net.packet.PacketPriority;
import org.onosproject.net.packet.PacketProcessor;
import org.onosproject.net.packet.PacketService;
import org.onosproject.net.statistic.PortStatisticsService;
import org.onosproject.net.topology.TopologyService;
import org.onosproject.store.service.EventuallyConsistentMap;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
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

    BlockingQueue<String> kafkablockingQueue = new LinkedBlockingDeque<>();
    HashSet<MacAddress> macAddrSet = new HashSet<>();
    Set<MacAddress> isLoginMac = new CopyOnWriteArraySet<>();
    Set<String> noRightMacToIPs = new CopyOnWriteArraySet<>();
    Map<String, Map<String, String>> redirectRootMap = new ConcurrentHashMap<>();
    Set<String> localIpSet = Simulation.localIpSet;

    private ArrayList<FlowRule> defaultFlowRulesCache = new ArrayList<>();
    private ArrayList<FlowRule> optiFlowRulesCache = new ArrayList<>();
    private HashMap<Integer, List<AflowEntry>> flowMap = new HashMap<>();
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



    AtomicReference<List<Map<SwitchPair,Long>>>  collectedStats=new AtomicReference<>();
    AtomicReference<Map<SwitchPair,Long>> portRate=new AtomicReference<>();
    AtomicReference<Map<String,List<Map<String, String>>>> flowStatics = new AtomicReference<>();

    KafkaListenerTask kafkaListenerTask;

    FlowStaticsCollector.Handler flowStaticsCollectorHandler = new FlowStaticsCollector.Handler() {
        @Override
        public void handle(Map<String,List<Map<String, String>>> stats) {
            flowStatics.set(stats);
//            logger.info(stats.toString());
            ObjectMapper mapper = new ObjectMapper();
            try {
                String valueAsString = mapper.writeValueAsString(stats);
                String param = "data=" + valueAsString;
//                logger.info(param);
                String sr= sendPost("http://" + App.Server_IP + ":8888/data/postData", param);
//                logger.info(sr);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        }
    };
    FlowStaticsCollector flowStaticsCollector;

    /**
     * TODO LIST
     动态指标（单个交换机上）
     1. 流的平均数据包数目
     2. 流的最大数据包数目
     3. 流的平均数据byte数目
     4. 流的最大byte数目
     5. 总流量大小
     6. 流经过的流数目
     7. 总流量的变化率
     */
    Map<String, Long> preSumBytesMap = new HashMap<>();
    DynamicDataCollector.Handler dynamicDataCollectorHandler = new DynamicDataCollector.Handler() {
        @Override
        public void handle(Map<String, Map<String, long[]>> stats) {
            Map<String, long[]> dynamicData = new HashMap<>();
            for (String device : stats.keySet()) {
                Map<String, long[]> deviceMap = stats.get(device);
                Set<String> keySet = deviceMap.keySet();
                long maxPackets = 0L;
                long maxBytes = 0L;
                long sumBytes = 0L;
                long sumPackets = 0L;
                int flowSize = keySet.size();
                long preSumBytes = preSumBytesMap.getOrDefault(device, 0L);
                for (String key : keySet) {
                    long[] longs = deviceMap.get(key);
                    sumPackets += longs[0];
                    sumBytes += longs[1];
                    maxPackets = Math.max(maxPackets, longs[0]);
                    maxBytes = Math.max(maxBytes, longs[1]);
                }
                long averagePackets = flowSize == 0 ? 0 : sumPackets / flowSize;
                long averageBytes = flowSize == 0 ? 0 : sumBytes / flowSize;
                long changeRate = Math.abs(sumBytes - preSumBytes);
                preSumBytesMap.put(device, sumBytes);
                dynamicData.put(device, new long[]{averagePackets, maxPackets, averageBytes,
                        maxBytes, sumBytes, flowSize, changeRate});
            }
            ObjectMapper mapper = new ObjectMapper();
            try {
                String valueAsString = mapper.writeValueAsString(dynamicData);
                String param = "data=" + valueAsString;
                logger.info(param);
                String sr= sendPost("http://" + App.Server_IP + ":8888/data/postDynamicData", param);
                logger.info(sr);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        }
    };
    DynamicDataCollector dynamicDataCollector;


    SocketServerTask listenWebServerTask;
    SocketServerTask.Handler listenWebHandler = payload -> {
        logger.info("!!!!!!!!!!!!!!!!!!!!!!!!");
        logger.info("{}", payload);
        handleReceiveDataAndInstallRoute(payload);
    };

    ResourceRouteTask resourceRouteTask;

    public void handleReceiveDataAndInstallRoute(String data) {
        ObjectMapper mapper=new ObjectMapper();
        try {
            JsonNode dataJson=mapper.readTree(data);
            if(dataJson == null) {
                logger.info("the json format error");
            }
            assert dataJson != null;
            int info = dataJson.get("info").asInt();

            if(info == 2){
                //用户成功登陆
                try {
                    TimeUnit.SECONDS.sleep(3);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                String loginMac = dataJson.get("loginMac").asText();
                String switcher = dataJson.get("switcher").asText();
                isLoginMac.add(MacAddress.valueOf(loginMac));
                logger.info("mac: " + loginMac + " have been login ----yes----");
                //移除强转的映射
                redirectRootMap.remove(loginMac);
//                Deque<Integer> dijkstraPath = Dijkstra(Env.graph, Integer.parseInt(switcher), 0);
//                natRouteToHost(new LinkedList<>(dijkstraPath), loginMac, IpPrefix.valueOf(App.VUE_FRONT_IP + "/32"),
//                        FlowEntryPriority.NAT_DEFAULT_ROUTING, 1, PortNumber.portNumber(2));

            } else if(info == 3) {
                try {
                    TimeUnit.SECONDS.sleep(3);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                String loginMac = dataJson.get("loginMac").asText();
                logger.info(isLoginMac.toString());
                isLoginMac.remove(MacAddress.valueOf(loginMac));
                logger.info(isLoginMac.toString());
                try {
                    for (FlowEntry entry : flowRuleService.getFlowEntries(TopologyDesc.getInstance().getDeviceId(App.ACCESSID))) {
                        if(!entry.table().equals(IndexTableId.of(0))) continue;
                        if(entry.priority()!= FlowEntryPriority.RESOURCE_DEFAULT_ROUTING) continue;
                        TrafficSelector selector = entry.selector();
                        EthCriterion srcCriterion = (EthCriterion) selector.getCriterion(Criterion.Type.ETH_SRC);
                        if(srcCriterion != null) {
                            //移除流表
                            flowRuleService.removeFlowRules(entry);
                        }
                    }
                } catch (Exception e) {
                    logger.error(e.toString());
                }
                logger.info("-----mac:" + loginMac + "  have logout success********************");

            } else if(info == 1) {
                String srcMac = dataJson.get("srcMac").asText();
                String dstIP = dataJson.get("dstIP").asText();
                String switcher = dataJson.get("switcher").asText();
                String srcPort = dataJson.get("srcPort").asText();
                String dstPort = dataJson.get("dstPort").asText();
                String protocol = dataJson.get("protocol").asText();
                long beginTime = dataJson.get("beginTime").asLong();
                logger.info("the mac address is -> " + srcMac  + "the dst IP is ->" + dstIP);
                Deque<Integer> path = null;
                if (localIpSet.contains(dstIP)) {
                    String[] split = dstIP.split("10.0.0.");
                    logger.info("dst source id is: " + split[1]);
                    //安装到host的流表
                    installFlowEntryToEndSwitch(Integer.parseInt(split[1]) - 1, dstIP);
                    //计算出到资源服务器的路径
                    path = Dijkstra(Env.graph, Integer.parseInt(switcher), Integer.parseInt(split[1]) - 1);
                } else {
                    path = Dijkstra(Env.graph, Integer.parseInt(switcher), 0);
                }

                logger.info(path.toString());
                hostRouteToDst(new LinkedList<>(path), srcMac, dstIP,
                        srcPort, dstPort, protocol);
                dstRouteToHost(new LinkedList<>(path), srcMac, dstIP,
                        srcPort, dstPort, protocol);
                logger.info("you can visit the source server" + dstIP);
                long endTime = new Date().getTime();
                logger.info("cost of all the time:" + (endTime-beginTime) + "  ms");

            } else if(info == 4) {
                logger.info("******************************************************");
                String msg = dataJson.get("msg").asText();
                noRightMacToIPs.add(msg);
                String[] split = msg.split("-");
                String mac = split[0];
                String ip = split[1] + "/32";
                logger.info("mac:" + mac + "     ip:" + ip + " have been down");
                try {
                    for (FlowEntry entry : flowRuleService.getFlowEntries(TopologyDesc.getInstance().getDeviceId(App.ACCESSID))) {
                        if(!entry.table().equals(IndexTableId.of(0))) continue;
                        if(entry.priority()!= FlowEntryPriority.RESOURCE_DEFAULT_ROUTING) continue;
                        TrafficSelector selector = entry.selector();
                        EthCriterion srcCriterion = (EthCriterion) selector.getCriterion(Criterion.Type.ETH_SRC);
                        IPCriterion dstCriterion=(IPCriterion) selector.getCriterion(Criterion.Type.IPV4_DST);
                        if(srcCriterion != null && dstCriterion != null) {
                            logger.info(srcCriterion.mac().toString()  + " -> " + dstCriterion.ip().toString());
                            if(srcCriterion.mac().toString().equals(mac) && dstCriterion.ip().toString().equals(ip)) {
                                //移除流表
                                logger.info("flow rule" + entry.toString() + "have been removed");
                                flowRuleService.removeFlowRules(entry);
                            }
                        }
                    }
                } catch (Exception e) {
                    logger.error(e.toString());
                }

            }

        } catch (JsonProcessingException e) {
            e.printStackTrace();
            logger.info("!!attention!!" + e.toString());
        }

    }
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

        /*kafkaListenerTask = new KafkaListenerTask(kafkablockingQueue);
        kafkaListenerTask.start();
        logger.info("kafka listener start");

        resourceRouteTask = new ResourceRouteTask(kafkablockingQueue);
        resourceRouteTask.start();
        logger.info("resource route task start!!!");*/
        listenWebServerTask = new SocketServerTask(App.WEB_LISTENING_IP, App.WEB_LISTENING_PORT, listenWebHandler);
        listenWebServerTask.start();
        logger.info("listen web socket started");
        //只收集入口交换机上的流表
        flowStaticsCollector = new FlowStaticsCollector(flowRuleService, TopologyDesc.getInstance(),
                new int[]{24}, flowStaticsCollectorHandler);
        flowStaticsCollector.setInterval(20);
        flowStaticsCollector.start();
        logger.info("flowStatics started");

        dynamicDataCollector = new DynamicDataCollector(flowRuleService, TopologyDesc.getInstance(), dynamicDataCollectorHandler);
        dynamicDataCollector.setInterval(25);
        dynamicDataCollector.start();
        logger.info("dynamic data collect");

        //链路qos多播测试
        String jsonfile = readFile("/data/telemetry.flow.json");
//        logger.info(jsonfile);
        handleTestData(jsonfile);
        installLinkTestFlow();
    }
    @Deactivate
    protected void deactivate() {
        App.getInstance().getPool().shutdownNow();
        App.getInstance().getScheduledPool().shutdownNow();
        packetService.removeProcessor(processor);
        flowRuleService.removeFlowRulesById(App.appId);
        listenWebServerTask.stop();
//        kafkaListenerTask.stop();
//        resourceRouteTask.stop();
        processor = null;
        logger.info("--------------------System Stopped-------------------------");
    }


    /**
     * 初始化系统启动所需的相关方法.
     */
    void init() {
        logger.info("init function");
        installFlowEntryToEndSwitch();
//        initTable1();
        installDefaultRouting(Env.defaultRoutings);
//        controllPica();
    }

    void controllPica() {
        DeviceId deviceId = DeviceId.deviceId("of:4641486e73020380");
        PortNumber output = PortNumber.portNumber(1);
        for(int i = 2; i <=10; i++) {
            FlowTableEntry entryTo=new FlowTableEntry();
            entryTo.setDeviceId(deviceId)
                    .setPriority(FlowEntryPriority.PICA)
                    .setTable(0);
            entryTo.filter()
                    .setInport(i);
            entryTo.action()
                    .setOutput(output);
            entryTo.install(flowRuleService);
        }

        DefaultFlowRule.Builder ruleBuilder=DefaultFlowRule.builder();
        TrafficSelector.Builder selectorBuilder= DefaultTrafficSelector.builder();
        selectorBuilder.matchEthType(Ethernet.TYPE_IPV4)
                .matchInPort(PortNumber.portNumber(1));

        TrafficTreatment.Builder trafficBuilder=DefaultTrafficTreatment.builder();
        for(int i = 2; i <= 10; i++) {
            trafficBuilder.setOutput(PortNumber.portNumber(i));
        }
        ruleBuilder.withSelector(selectorBuilder.build())
                .withTreatment(trafficBuilder.build())
                .makePermanent()
                .withPriority(FlowEntryPriority.PICA)
                .forTable(0)
                .fromApp(App.appId);
        ruleBuilder.forDevice(deviceId);
        FlowRuleOperations.Builder flowRuleOpBuilder=FlowRuleOperations.builder();
        FlowRule build = ruleBuilder.build();
        flowRuleOpBuilder.add(build);
        flowRuleService.apply(flowRuleOpBuilder.build());
   /*     output = PortNumber.portNumber(1);
        FlowTableEntry entryBack=new FlowTableEntry();
        entryBack.setDeviceId(deviceId)
                .setPriority(FlowEntryPriority.PICA)
                .setTable(0);
        entryBack.filter()
                .setInport(2);
        entryBack.action()
                .setOutput(output);
        entryBack.install(flowRuleService);*/
    }

    void testPica() {
        DeviceId deviceId = DeviceId.deviceId("of:4641486e73020380");
        PortNumber output = PortNumber.portNumber(2);
        FlowTableEntry entry=new FlowTableEntry();
        entry.setDeviceId(deviceId)
                .setPriority(FlowEntryPriority.RESOURCE_DEFAULT_ROUTING)
                .setTable(0);
        entry.filter()
                .setDstIP(IpPrefix.valueOf("10.0.0.1" + "/32"))
                .setProtocol(Byte.parseByte("6"));

        entry.action()
                .setOutput(output);
        entry.setTimeout(App.FLOW_TIMEOUT);
        FlowRule rule = entry.install(flowRuleService);
    }

    void installFlowEntryToEndSwitch(int endSwitchId, String dstIP) {
        FlowTableEntry entry=new FlowTableEntry();
        entry.setTable(0)
                .setPriority(FlowEntryPriority.TABLE0_HANDLE_LAST_HOP)
                .setDeviceId(TopologyDesc.getInstance().getDeviceId(endSwitchId));
        entry.filter()
                .setDstIP(IpPrefix.valueOf(dstIP + "/32"));
        entry.action()
                .setOutput(PortNumber.portNumber("1"));
        entry.install(flowRuleService);
        logger.debug("Host connection flow installed");
    }


    void installFlowEntryToEndSwitch() {
        for(Device device:deviceService.getAvailableDevices()){
            if(device.id().toString().equals(TopologyDesc.getInstance().getDeviceId(App.ACCESSID).toString())) {
                continue;
            }
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
    private void packetOut(PacketContext context, byte[] outdata, PortNumber portNumber) {
        context.treatmentBuilder()
                .setOutput(portNumber);
        ByteBuffer data = context.outPacket().data();
        data.clear();
        data.put(outdata);
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

    public void hostRouteToDst(Deque<Integer> path, String srcMac, String dstIP,
                               String srcPort, String dstPort, String protocol) {
        TopologyDesc topo = TopologyDesc.getInstance();
        List<Integer> routing = new ArrayList<>();
        int size = path.size();
        for(int i = 0; i < size; i++) {
            routing.add(path.pollFirst());
        }
        Integer dst = routing.get(size - 1);
        for(int j=0;j<routing.size();j++){
            int curr=routing.get(j);
            DeviceId currDeviceId=topo.getDeviceId(curr);
            PortNumber output = PortNumber.portNumber(App.NATPORT);
            if(j < routing.size() - 1) {
                int next=routing.get(j+1);
                DeviceId nextHopDeviceId=topo.getDeviceId(next);
                output = topo.getConnectionPort(currDeviceId,nextHopDeviceId);
            }
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
                    .setSrcMac(MacAddress.valueOf(srcMac))
                    .setDstIP(IpPrefix.valueOf(dstIP + "/32"))
                    .setProtocol(Byte.parseByte(protocol));

            if(Integer.parseInt(srcPort) != 0 && Byte.parseByte(protocol) == IPv4.PROTOCOL_TCP) {
                entry.filter()
//                    .setSport(Integer.parseInt(srcPort))
                        .setDstPort(Integer.parseInt(dstPort));
            }

            entry.action()
                    .setOutput(output);
            entry.setTimeout(App.FLOW_TIMEOUT);
            FlowRule rule = entry.install(flowRuleService);
//            defaultFlowRulesCache.add(rule);
        }

    }

    public void dstRouteToHost(Deque<Integer> path, String srcMac, String dstIP,
                               String srcPort, String dstPort, String protocol) {
        TopologyDesc topo = TopologyDesc.getInstance();
        List<Integer> routing = new ArrayList<>();
        int size = path.size();
        for(int i = 0; i < size; i++) {
            routing.add(path.pollLast());
        }
        Integer dst = routing.get(0);

        for(int j=0;j<routing.size();j++){
            int curr=routing.get(j);
            int nextIndex = (j + 1) < routing.size() ? (j + 1) : j;
            int next=routing.get(nextIndex);
            DeviceId currDeviceId=topo.getDeviceId(curr);
            DeviceId nextHopDeviceId=topo.getDeviceId(next);
            PortNumber output = PortNumber.portNumber(App.ACCESSPORT);
            if (j != routing.size() - 1) {
                output=topo.getConnectionPort(currDeviceId,nextHopDeviceId);
            }
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
                    .setDstMac(MacAddress.valueOf(srcMac))
                    .setSrcIP(IpPrefix.valueOf(dstIP + "/32"))
                    .setProtocol(Byte.parseByte(protocol));
            if(Integer.parseInt(dstPort) != 0 && Byte.parseByte(protocol) == IPv4.PROTOCOL_TCP) {
                entry.filter()
                        .setSport(Integer.parseInt(dstPort));
//                        .setDstPort(Integer.parseInt(srcPort));
            }

            entry.action()
                    .setOutput(output);
            entry.setTimeout(App.FLOW_TIMEOUT);
            FlowRule rule = entry.install(flowRuleService);
//            defaultFlowRulesCache.add(rule);
        }

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
        for(int j=0;j<routing.size();j++){
            int curr=routing.get(j);
            PortNumber output = PortNumber.portNumber(App.NATPORT);
            DeviceId currDeviceId=topo.getDeviceId(curr);
            if(j < routing.size() - 1) {
                int next=routing.get(j+1);
                DeviceId nextHopDeviceId=topo.getDeviceId(next);
                output = topo.getConnectionPort(currDeviceId,nextHopDeviceId);
            }
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
//            defaultFlowRulesCache.add(rule);
        }

    }


    public void natRouteToHost(Deque<Integer> path, String dstMac, IpPrefix ipPrefix, int flowPriority, int mode, PortNumber lastPort) {
        TopologyDesc topo = TopologyDesc.getInstance();
        List<Integer> routing = new ArrayList<>();
        int size = path.size();
        for(int i = 0; i < size; i++) {
            routing.add(path.pollLast());
        }
        for(int j=0;j<routing.size();j++){
            int curr=routing.get(j);
            int nextIndex = (j + 1) < routing.size() ? (j + 1) : j;
            int next=routing.get(nextIndex);
            DeviceId currDeviceId=topo.getDeviceId(curr);
            DeviceId nextHopDeviceId=topo.getDeviceId(next);
            PortNumber output = PortNumber.portNumber("0");
            if (j != routing.size() - 1) {
                output=topo.getConnectionPort(currDeviceId,nextHopDeviceId);
            } else {
                output = lastPort;
            }

            if(output.equals(INVALID_PORT)){
                logger.info("the adjacent switch port error");
                continue;
                //todo log
            }
            FlowTableEntry entry=new FlowTableEntry();
            entry.setDeviceId(currDeviceId)
                    .setPriority(flowPriority)
                    .setTable(0);
            if(null != ipPrefix) {
                entry.filter()
                        .setSrcIP(ipPrefix);
            }
            entry.filter()
                    .setDstMac(MacAddress.valueOf(dstMac));
            entry.action()
                    .setOutput(output);
            if(mode == 2) {
                entry.setTimeout(3);
            }
            FlowRule rule = entry.install(flowRuleService);
//            defaultFlowRulesCache.add(rule);
        }

    }

    /**
     *下发改IP的流表
     */
    public void mendPacket(Ethernet ethPkt) {
        MacAddress macAddress = ethPkt.getSourceMAC();
        IPv4 ipPayload = (IPv4) ethPkt.getPayload();
        IpAddress srcIP = IpAddress.valueOf(ipPayload.getSourceAddress());
        IpAddress dstIP = IpAddress.valueOf(ipPayload.getDestinationAddress());
        Map<String, String> redirectMap = redirectRootMap.getOrDefault(macAddress.toString(), new ConcurrentHashMap<>());
        TCP tcpPayload =  (TCP) ipPayload.getPayload();
        int srcPort = tcpPayload.getSourcePort();
        String key = srcIP + "->" + srcPort;
        String value = dstIP.toString();
        ipPayload.setDestinationAddress(App.VUE_FRONT_IP);
        redirectMap.put(key, value);
        redirectRootMap.put(macAddress.toString(), redirectMap);

        DeviceId deviceId = TopologyDesc.getInstance().getDeviceId(App.ACCESSID);
        FlowTableEntry entryTo = new FlowTableEntry();
        entryTo.setTable(0)
                .setPriority(FlowEntryPriority.REDIRECT_PACKET)
                .setTimeout(6)
                .setDeviceId(deviceId);
        entryTo.filter()
                .setProtocol(IPv4.PROTOCOL_TCP)
                .setSport(srcPort)
                .setSrcIP(IpPrefix.valueOf(srcIP + "/32"));
        entryTo.action()
                .setDstIP(IpAddress.valueOf(App.VUE_FRONT_IP))
                .setOutput(PortNumber.portNumber(2));
        entryTo.install(flowRuleService);

        FlowTableEntry entryBack = new FlowTableEntry();
        entryBack.setTable(0)
                .setPriority(FlowEntryPriority.REDIRECT_PACKET)
                .setTimeout(6)
                .setDeviceId(deviceId);
        entryBack.filter()
                .setProtocol(IPv4.PROTOCOL_TCP)
                .setDstPort(srcPort)
                .setDstIP(IpPrefix.valueOf(srcIP + "/32"));
        entryBack.action()
                .setSrcIP(dstIP)
                .setOutput(PortNumber.portNumber(App.ACCESSPORT));
        entryBack.install(flowRuleService);
    }


    public void initTable1() {
        DeviceId deviceId = TopologyDesc.getInstance().getDeviceId(App.ACCESSID);
        FlowTableEntry entry = new FlowTableEntry();
        entry.setPriority(5)
                .setTable(1)
                .setDeviceId(deviceId);
        entry.action()
                .setPunt(true);
        entry.install(flowRuleService);
    }


    public void packetToTable1(){
        FlowTableEntry entry = new FlowTableEntry();
        DeviceId deviceId = TopologyDesc.getInstance().getDeviceId(App.ACCESSID);
        entry.setDeviceId(deviceId)
                .setTable(0)
                .setPriority(FlowEntryPriority.TO_TABLE1);
        entry.filter()
                .setProtocol(IPv4.PROTOCOL_TCP);
        entry.action()
                .setTransition(1);
        entry.install(flowRuleService);
     /*
        DefaultFlowRule.Builder ruleBuilder=DefaultFlowRule.builder();
        TrafficSelector.Builder selectorBuilder= DefaultTrafficSelector.builder();
        selectorBuilder.matchEthType(Ethernet.TYPE_IPV4);
        selectorBuilder.matchIPProtocol(IPv4.PROTOCOL_TCP);

        TrafficTreatment.Builder trafficBuilder=DefaultTrafficTreatment.builder();
        trafficBuilder.transition(1);

        ruleBuilder.withSelector(selectorBuilder.build())
                .withTreatment(trafficBuilder.build())
                .makePermanent()
                .withPriority(FlowEntryPriority.TO_TABLE1)
                .forTable(0)
                .fromApp(App.appId);
        ruleBuilder.forDevice(TopologyDesc.getInstance().getDeviceId(App.ACCESSID));
        FlowRuleOperations.Builder flowRuleOpBuilder=FlowRuleOperations.builder();
        FlowRule build = ruleBuilder.build();
        flowRuleOpBuilder.add(build);
        flowRuleService.apply(flowRuleOpBuilder.build());*/
    }


    private void installDefaultRouting(String payload) {
        TopologyDesc topo=TopologyDesc.getInstance();
        try{
            ObjectMapper mapper=new ObjectMapper();
            JsonNode root=mapper.readTree(payload);
            JsonNode routings=root.get("res");
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
                                    .setTable(0);
                            entry.filter()
                                    .setSrcIP(srcAddr)
                                    .setDstIP(dstAddr);
                            entry.action()
                                    .setOutput(output);
                            entry.install(flowRuleService);
                        }
                    }
                }
                logger.info("default routing has been installed");
            }
        }
        catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    private void handleTestData(String jsonData) {
        TopologyDesc instance = TopologyDesc.getInstance();
//        jsonData = jsonData.substring(0, jsonData.length() - 1);
        try {
            JsonNode jsonNode = new ObjectMapper().readTree(jsonData);
            for(int i = 0; i < Env.N_SWITCH; i++) {
                JsonNode deviceData = jsonNode.get(String.valueOf(i+1));
                Iterator<String> iterator = deviceData.fieldNames();
                List<AflowEntry> aflowEntryList = new ArrayList<>();
                while(iterator.hasNext()) {
                    String fieldName = iterator.next();
                    String[] split = fieldName.split(",");
                    String srcIP = split[0] + "/32";
                    String swithcId = String.valueOf(i);
                    int preId = Integer.parseInt(split[1]) - 1;
                    String inport = "";
                    if(preId == -1) {
                        inport = "1";
                    } else {
                        DeviceId cur = instance.getDeviceId(swithcId);
                        DeviceId pre = instance.getDeviceId(preId);
                        inport = instance.getConnectionPort(cur, pre).toString();
//                        inport = getPortInfo(testSwMap.get(swithcId), testSwMap.get(String.valueOf(preId))).toString();
                    }

                    JsonNode actions = deviceData.get(fieldName);
                    JsonNode action1 = actions.get("action1");
                    JsonNode action2 = actions.get("action2");
                    AflowEntry aflowEntry = null;
                    if(action1 != null && action2 != null) {
                        String outportText = action1.get("outport").toString();
                        outportText = outportText.substring(1, outportText.length() - 1);
                        String[] split1 = outportText.split(",");
                        List<String> outports = new ArrayList<>();
                        for(String dstId : split1) {
                            int outId = Integer.parseInt(dstId) - 1;
                            if(outId == -1) {
                                outports.add("1");
                            } else {
                                outports.add(instance.getConnectionPort(
                                        instance.getDeviceId(swithcId), instance.getDeviceId(outId)).toString());
//                                outports.add(getPortInfo(testSwMap.get(swithcId), testSwMap.get(String.valueOf(outId))).toString());
                            }
                        }
                        String newSrcIP = action2.get("src").asText();
                        String newDstIP = action2.get("dst").asText();
                        String vlan = action2.get("vlan").asText();
                        aflowEntry = new AflowEntry(srcIP, inport, outports, newSrcIP, newDstIP, vlan);
                    } else if(action1 != null) {
                        String outportText = action1.get("outport").toString();
                        outportText = outportText.substring(1, outportText.length() - 1);
                        String[] split1 = outportText.split(",");
                        List<String> outports = new ArrayList<>();
                        for(String dstId : split1) {
                            int outId = Integer.parseInt(dstId) - 1;
                            if(outId == -1) {
                                outports.add("1");
                            } else {
                                outports.add(instance.getConnectionPort(
                                        instance.getDeviceId(swithcId), instance.getDeviceId(outId)).toString());
//                                outports.add(getPortInfo(testSwMap.get(swithcId), testSwMap.get(String.valueOf(outId))).toString());
                            }
                        }
                        aflowEntry = new AflowEntry(srcIP, inport, outports, "X", "X", "-1");
                    } else if(action2 != null) {
                        List<String> outports = new ArrayList<>();
                        String newSrcIP = action2.get("src").asText();
                        String newDstIP = action2.get("dst").asText();
                        String vlan = action2.get("vlan").asText();
                        aflowEntry = new AflowEntry(srcIP, inport, outports, newSrcIP, newDstIP, vlan);
                    }
                    aflowEntryList.add(aflowEntry);
                }
                flowMap.put(i, aflowEntryList);
            }

        } catch (JsonProcessingException e) {
            e.printStackTrace();
            logger.info(e.toString());
        }
    }

    public void installLinkTestFlow() {
        for(int i = 0; i < Env.N_SWITCH; i++)  {
            List<AflowEntry> aflowEntryList = flowMap.get(i);
            if (aflowEntryList == null) {
                return;
            }
            for(AflowEntry a : aflowEntryList) {
                installMultiCastFlow(a.srcIP, a.inport, a.outports, a.newSrcIP, a.newDstIP, a.vlan, String.valueOf(i));
            }
        }
    }


    private void installMultiCastFlow(String srcIP, String inport, List<String> outports, String newSrcIP, String newDstIP, String vlan, String sId) {
//        DeviceId deviceId = testSwMap.get(sId);
        DeviceId deviceId = TopologyDesc.getInstance().getDeviceId(sId);
        char c = srcIP.charAt(7);
        //table0 的流表下发
        if(c == '1') {
            DefaultFlowRule.Builder ruleBuilder = DefaultFlowRule.builder();
            TrafficSelector.Builder selectBuilder = DefaultTrafficSelector.builder();
            selectBuilder.matchEthType(Ethernet.TYPE_IPV4)
                    .matchInPort(PortNumber.portNumber(inport))
                    .matchIPSrc(IpPrefix.valueOf(srcIP))
                    .matchVlanId(VlanId.ANY);

            TrafficTreatment.Builder trafficBuilder = DefaultTrafficTreatment.builder();
            for(String output : outports) {
                if(output.equals("0")) {
                    logger.error("---port error--");
                }
                trafficBuilder.setOutput(PortNumber.portNumber(output));
            }
            ruleBuilder.withSelector(selectBuilder.build())
                    .withPriority(60010)
                    .withTreatment(trafficBuilder.build())
                    .forTable(0)
                    .fromApp(App.appId)
                    .makePermanent()
                    .forDevice(deviceId);
            FlowRuleOperations.Builder flowRulebuilder = FlowRuleOperations.builder();
            flowRulebuilder.add(ruleBuilder.build());
            flowRuleService.apply(flowRulebuilder.build());
        } else {

            DefaultFlowRule.Builder ruleBuilder0 = DefaultFlowRule.builder();
            TrafficSelector.Builder selectBuilder0 = DefaultTrafficSelector.builder();
            selectBuilder0.matchEthType(Ethernet.TYPE_IPV4)
                    .matchInPort(PortNumber.portNumber(inport))
                    .matchIPSrc(IpPrefix.valueOf(srcIP));

            TrafficTreatment.Builder trafficBuilder0 = DefaultTrafficTreatment.builder();
            for(String output : outports) {
                if(output.equals("0")) {
                    logger.error("---port error--");
                }
                trafficBuilder0.setOutput(PortNumber.portNumber(output));
            }
            if(!newSrcIP.equals("X")) {
                trafficBuilder0.transition(3);
                //处理返回包的流表项
                handleReturnPacket(srcIP, inport, newSrcIP, newDstIP, vlan, deviceId);
            }
            ruleBuilder0.withSelector(selectBuilder0.build())
                    .withPriority(60009)
                    .withTreatment(trafficBuilder0.build())
                    .forTable(0)
                    .fromApp(App.appId)
                    .makePermanent()
                    .forDevice(deviceId);
            FlowRuleOperations.Builder flowRulebuilder0 = FlowRuleOperations.builder();
            flowRulebuilder0.add(ruleBuilder0.build());
            flowRuleService.apply(flowRulebuilder0.build());
        }

    }

    private void handleReturnPacket(String srcIP, String inport, String newSrcIP, String newDstIP, String vlan,  DeviceId deviceId) {
        DefaultFlowRule.Builder ruleBuilder = DefaultFlowRule.builder();
        TrafficSelector.Builder selectBuilder = DefaultTrafficSelector.builder();
        selectBuilder.matchEthType(Ethernet.TYPE_IPV4)
                .matchIPSrc(IpPrefix.valueOf(srcIP))
                .matchInPort(PortNumber.portNumber(inport))
                .matchVlanId(VlanId.ANY);

        TrafficTreatment.Builder trafficBuilder = DefaultTrafficTreatment.builder();
        trafficBuilder.setIpSrc(IpAddress.valueOf(newSrcIP))
                .setIpDst(IpAddress.valueOf(newDstIP))
                .setVlanId(VlanId.vlanId(Short.parseShort(vlan)))
                .setOutput(PortNumber.IN_PORT);

        ruleBuilder.withSelector(selectBuilder.build())
                .withPriority(60009)
                .withTreatment(trafficBuilder.build())
                .forTable(3)
                .fromApp(App.appId)
                .makePermanent()
                .forDevice(deviceId);
        FlowRuleOperations.Builder flowRulebuilder = FlowRuleOperations.builder();
        flowRulebuilder.add(ruleBuilder.build());
        flowRuleService.apply(flowRulebuilder.build());
    }

    public String readFile(String path) {
        BufferedReader reader = null;
        String laststr = "";
        try {
            FileInputStream fileInputStream = new FileInputStream(path);
            InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream, "UTF-8");
            reader = new BufferedReader(inputStreamReader);
            String tempString = null;
            while ((tempString = reader.readLine()) != null) {
                laststr += tempString;
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return laststr;
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

            //mac在黑名单中就返回
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

            if(ethPkt.getEtherType() == Ethernet.TYPE_ARP) {
//                logger.info(pkt.receivedFrom().toString());
//                logger.info("drop arp packet");
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

            // Do not process IPv4 multicast packets, let mfwd handle them
            if (ignoreIPv4Multicast && ethPkt.getEtherType() == Ethernet.TYPE_IPV4) {
                if (id.mac().isMulticast()) {
                    return;
                }
            }
            //packetin 消息接入的交换机
            PortNumber port = pkt.receivedFrom().port();
            DeviceId deviceId = pkt.receivedFrom().deviceId();
            int srcId = TopologyDesc.getInstance().getDeviceIdx(deviceId);

            // 解析出包中的IP字段
            IPv4 ipPayload = (IPv4) ethPkt.getPayload();
            if(ipPayload != null && (srcId == App.NATID || srcId == App.ACCESSID)) {
                IpAddress srcIP = IpAddress.valueOf(ipPayload.getSourceAddress());
                IpAddress dstIP = IpAddress.valueOf(ipPayload.getDestinationAddress());
                byte protocol = ipPayload.getProtocol();
                int srcPort = 0;
                int dstPort = 0;

                //如果用户没有登陆,将修改ip包,强制跳转到登陆页面
                if(!isLoginMac.contains(macAddress)) {
                    //第一次packetIn会默认配置到网关的路由
                    if(!macAddrSet.contains(macAddress) && srcId == App.ACCESSID) {
                        logger.info("first in mac addr:" + macAddress + "   connected switcher:" + srcId);
//                        packetToTable1();
                        Deque<Integer> dijkstraPath = Dijkstra(Env.graph, srcId, App.NATID);
                        logger.info(dijkstraPath.toString());
                        //配置到dns服务器的连通
                        natRouteToHost(new LinkedList<>(dijkstraPath), macAddress.toString(), IpPrefix.valueOf(App.DNS_IP + "/32"), FlowEntryPriority.DNS_DEFAULT_ROUTING, 1, port);
                        hostRouteToNat(new LinkedList<>(dijkstraPath), IpPrefix.valueOf(App.DNS_IP + "/32"), FlowEntryPriority.DNS_DEFAULT_ROUTING, 1, null);

                        hostRouteToNat(new LinkedList<>(dijkstraPath), IpPrefix.valueOf(App.VUE_FRONT_IP + "/32"), FlowEntryPriority.NAT_DEFAULT_ROUTING, 1, null);
                        natRouteToHost(new LinkedList<>(dijkstraPath), macAddress.toString(), IpPrefix.valueOf(App.VUE_FRONT_IP + "/32"), FlowEntryPriority.NAT_DEFAULT_ROUTING - 5, 1, port);
                        macAddrSet.add(macAddress);
                    }
                    //修改包强转到认证页面
                    if(protocol == IPv4.PROTOCOL_TCP  && srcId == App.ACCESSID) {
                        mendPacket(ethPkt);
                    }
                    return;
                } else {
                    //用户登陆后的处理
                    if(srcId == App.ACCESSID) {
                        if (protocol == IPv4.PROTOCOL_ICMP) {
                            logger.info("icmp  ping");
                        } else if (protocol == IPv4.PROTOCOL_TCP) {
                            TCP tcpPayload = (TCP) ipPayload.getPayload();
                            srcPort = tcpPayload.getSourcePort();
                            dstPort = tcpPayload.getDestinationPort();
                        } else if (protocol == IPv4.PROTOCOL_UDP) {
                            UDP udpPayload = (UDP) ipPayload.getPayload();
                            srcPort = udpPayload.getSourcePort();
                            dstPort = udpPayload.getDestinationPort();
                        }
                        String key  = macAddress.toString() + "-" + dstIP.toString();
                        if (noRightMacToIPs.contains(key)) {
                            //如果用户没有信任度访问IP，则将其强制转换到一个页面提示
                            mendPacket(ethPkt);
                            return;
                        }
                        long time = new Date().getTime();
                        String param = "srcMac=" + macAddress.toString() + "&srcIP=" + srcIP + "&dstIP=" +
                                dstIP.toString() + "&switcher=" + srcId + "&srcPort=" + srcPort +
                                "&dstPort=" + dstPort + "&protocol=" + protocol + "&beginTime=" + time;
//                        logger.info(param);
                        String sr = sendPost("http://" + App.Server_IP + ":8888/user/verifyByMac", param);
//                        logger.info(sr);
                    }
                    return;
                }

            }
            logger.info(ethPkt.toString());
            //packet in 的包不是IP包
            logger.info("other  packet in message");
        }

    }

    private class ResourceRouteTask extends AbstractStoppableTask {
        BlockingQueue<String> blockingQueue;
        public ResourceRouteTask( BlockingQueue<String> blockingQueue) {
            this.blockingQueue = blockingQueue;
        }
        @Override
        public void run() {
            isRunning.set(true);
            while(!stopRequested){
                try{
                    String data = blockingQueue.take();
                    handleReceiveDataAndInstallRoute(data);
                }catch (InterruptedException exception){
                    stopRequested=true;
                }
            }
            isRunning.set(false);
        }
    }

    /**
     * 每一条流表项的类.
     */
    public class AflowEntry {
        String srcIP;
        String inport;
        List<String> outports;
        String newSrcIP;
        String newDstIP;
        String vlan;
        public AflowEntry(String srcIP, String inport, List<String> outports, String newSrcIP, String newDstIp, String vlan) {
            this.srcIP = srcIP;
            this.inport = inport;
            this.outports = outports;
            this.newSrcIP = newSrcIP;
            this.newDstIP = newDstIp;
            this.vlan = vlan;
        }

        @Override
        public String toString() {
            return "AflowEntry{" +
                    "srcIP='" + srcIP + '\'' +
                    ", inport='" + inport + '\'' +
                    ", outports=" + outports +
                    ", newSrcIP='" + newSrcIP + '\'' +
                    ", newDstIP='" + newDstIP + '\'' +
                    ", vlan=" + vlan +
                    '}';
        }
    }
}