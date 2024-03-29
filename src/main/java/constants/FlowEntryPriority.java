package apps.smartfwd.src.main.java.constants;

public class FlowEntryPriority {
    //作为最后一跳的交换机，将packet送给host
    public static final int TABLE0_HANDLE_LAST_HOP=60001;
    public static final int TABLE0_TAG_FLOW=60000;
    // table0 如果遇到已经打上标签的flow
    public static final int TABLE0_HANDLE_TAGGED_FLOW=59999;
    // to nat
    public static final int TABLE0_NAT_FLOW=49999;
    // table0 默认流表优先级,但是比fwd的优先级要高
    public static final int TABLE0_DEFAULT=10000;

    //table1 用于统计流量矩阵
    public static final int TABLE1_MATRIX_COLLECTION=60000;

    //table2 优化路由
    public static final int TABLE2_OPT_ROUTING=60000;
    //table2 默认路由
    public static final int TABLE2_DEFAULT_ROUTING=59999;
    //table2 drop
    public static final int TABLE2_DROP=40000;

    public static final int DEFAULT_ROUTING_FWD=51000;
    public static final int DEFAULT_ROUTING_VLAN=52000;
    public static final int RESOURCE_DEFAULT_ROUTING=55000;
    public static final int NAT_DEFAULT_ROUTING=50000;
    public static final int DNS_DEFAULT_ROUTING=60000;
    public static final int INTERNET_ROUTING=48000;
    public static final int REDIRECT_PACKET=60002;
    public static final int TO_TABLE1=50005;

    public static final int PICA=60000;

}


