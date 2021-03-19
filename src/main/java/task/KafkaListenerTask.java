package apps.smartfwd.src.main.java.task;

import apps.smartfwd.src.main.java.task.base.AbstractStoppableTask;
import apps.smartfwd.src.main.java.utils.Consumer;
import apps.smartfwd.src.main.java.utils.MQDict;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import java.util.concurrent.BlockingQueue;

public class KafkaListenerTask extends AbstractStoppableTask {
    BlockingQueue<String> blockingQueue;
    public KafkaListenerTask( BlockingQueue<String> blockingQueue) {
        this.blockingQueue = blockingQueue;

    }
    @Override
    public void run() {
        isRunning.set(true);
        while (!stopRequested) {
            try {
                ConsumerRecords<String, String> recordsA = Consumer.consumer.poll(MQDict.CONSUMER_POLL_TIME_OUT);
                recordsA.forEach((ConsumerRecord<String, String> record)->{
                    if (record.topic().equals(MQDict.CONSUMER_TOPICB)) {
                        try {
                            blockingQueue.put(record.value());
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
//                        System.out.println("revice: key ==="+record.key()+" value ===="+record.value()+" topic ==="+record.topic());
                    }
                });
            } catch (Exception e) {
                stopRequested = true;
            }
        }
        isRunning.set(false);
    }
}
