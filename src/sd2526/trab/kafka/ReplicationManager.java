package sd2526.trab.kafka;

import com.google.gson.Gson;
import sd2526.trab.api.Message;
import sd2526.trab.impl.java.servers.JavaMessagesKafka;
import sd2526.trab.kafka.Events.*;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class ReplicationManager {

    private KafkaPublisher publisher;
    private KafkaSubscriber subscriber;

    private final String topic;
    private final AtomicLong version;
    private final Gson gson;

    private final JavaMessagesKafka service;

    private boolean kafkaEnabled = false;

    private static final int RETRY_COUNT = 10;
    private static final int RETRY_SLEEP_MS = 1000;

    public ReplicationManager(String topic,
                              JavaMessagesKafka service,
                              AtomicLong version) {

        this.topic = topic;
        this.service = service;
        this.version = version;
        this.gson = new Gson();

        initKafka();
        start();
    }

    private void initKafka() {

        for (int i = 0; i < RETRY_COUNT; i++) {
            try {
                KafkaUtils.createTopic(topic);

                this.publisher = KafkaPublisher.createPublisher(
                        "localhost:9092,kafka:9092"
                );

                this.subscriber = KafkaSubscriber.createSubscriber(
                        "localhost:9092,kafka:9092",
                        List.of(topic)
                );

                kafkaEnabled = true;
                System.out.println("[Kafka] enabled after retry " + i);
                return;

            } catch (Exception e) {
                System.out.println("[Kafka] not ready (retry " + i + ")");
                sleep(RETRY_SLEEP_MS);
            }
        }

        System.out.println("[Kafka] DISABLED (running without replication)");
        kafkaEnabled = false;
        publisher = null;
        subscriber = null;
    }

    public void start() {

        if (!kafkaEnabled || subscriber == null)
            return;

        subscriber.start(record -> {

            try {
                Event base = gson.fromJson(record.value(), Event.class);

                long incoming = base.getSid();

                synchronized (this) {

                    if (incoming <= version.get())
                        return;

                    switch (base.getType()) {

                        case "POST" -> {
                            PostEvent e = gson.fromJson(record.value(), PostEvent.class);
                            service.applyPost(e);
                        }

                        case "REMOVE_INBOX" -> {
                            DeleteInboxEvent e = gson.fromJson(record.value(), DeleteInboxEvent.class);
                            service.applyRemoveInbox(e);
                        }

                        case "DELETE_MESSAGE" -> {
                            DeleteMessageEvent e = gson.fromJson(record.value(), DeleteMessageEvent.class);
                            service.applyDeleteMessageEvent(e);
                        }
                    }

                    version.set(incoming);
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    private boolean canPublish() {
        return kafkaEnabled && publisher != null;
    }

    public void publishPost(Message msg, Set<String> local) {

        if (!canPublish()) return;

        PostEvent event = new PostEvent();
        event.setSid(version.incrementAndGet());
        event.setMsg(msg);
        event.setLocalRecipients(local);

        publish(event);
    }

    public void publishRemoveInbox(String name, String mid, String pwd) {

        if (!canPublish()) return;

        DeleteInboxEvent event = new DeleteInboxEvent();
        event.setSid(version.incrementAndGet());
        event.setName(name);
        event.setMid(mid);
        event.setPwd(pwd);

        publish(event);
    }

    public void publishDeleteMessage(String mid) {

        if (!canPublish()) return;

        DeleteMessageEvent event = new DeleteMessageEvent();
        event.setSid(version.incrementAndGet());
        event.setMid(mid);

        publish(event);
    }

    private void publish(Event event) {
        try {
            publisher.publish(topic, gson.toJson(event));
        } catch (Exception e) {
            System.out.println("[Kafka] publish failed: " + e.getMessage());
        }
    }

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (Exception ignored) {}
    }
}