package apps.smartfwd.src.main.java.org.chandler.task;

import apps.smartfwd.src.main.java.org.chandler.FlowTableEntry;
import org.onosproject.net.flow.FlowRuleService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;

public class FlowEntryTask extends StoppableTask {
    private final Logger logger=LoggerFactory.getLogger(getClass().getName());
    BlockingQueue<FlowTableEntry> flowEntries;
    FlowRuleService southboundService;
    public FlowEntryTask(BlockingQueue<FlowTableEntry> entries, FlowRuleService flowRuleService){
        this.flowEntries=entries;
        this.southboundService=flowRuleService;
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
                return;
            }
        }
        isRunning.set(false);
    }

}
