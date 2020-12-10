package apps.smartfwd.src.main.java.org.chandler.task;

import apps.smartfwd.src.main.java.org.chandler.FlowTableEntry;
import org.onosproject.net.flow.FlowRuleService;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;

public class FlowEntryTask extends StoppableTask {

    BlockingQueue<FlowTableEntry> flowEntries;
    FlowRuleService southboundService;
    public FlowEntryTask(BlockingQueue<FlowTableEntry> entries, FlowRuleService flowRuleService){
        this.flowEntries=entries;
        this.southboundService=flowRuleService;

    }
    public FlowEntryTask(BlockingQueue<FlowTableEntry> entries, FlowRuleService flowRuleService, ExecutorService service){
        this.flowEntries=entries;
        this.southboundService=flowRuleService;
        this.service=service;
    }

    @Override
    public void run() {
        isRunning.set(true);
        while(!stopRequested){
            try{
                FlowTableEntry flowEntry=flowEntries.take();
                flowEntry.install(southboundService);
            }catch (InterruptedException exception){
                stopRequested=true;
            }
        }
        isRunning.set(false);
    }

}
