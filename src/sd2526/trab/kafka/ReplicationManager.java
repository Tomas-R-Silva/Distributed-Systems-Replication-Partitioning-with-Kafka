package sd2526.trab.kafka;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import com.google.gson.Gson;

import sd2526.trab.api.Message;
import sd2526.trab.impl.java.servers.JavaMessages;
import sd2526.trab.kafka.Events.DeleteInboxEvent;
import sd2526.trab.kafka.Events.DeleteMessageEvent;
import sd2526.trab.kafka.Events.Event;
import sd2526.trab.kafka.Events.PostEvent;

public class ReplicationManager {

    private KafkaPublisher publisher;
    private KafkaSubscriber subscriber;

    private final String topic;
    private final AtomicLong version;

    private final Gson gson;

    private final JavaMessages service;

    public ReplicationManager(String topic,
                              JavaMessages service,
                              AtomicLong version) {

        this.topic = topic;
        this.version = version;
        this.gson = new Gson();
        this.service = service;

        KafkaUtils.createTopic(topic);

        try {
            Thread.sleep(1000);
        } catch (Exception x) {}

        try {

            this.publisher =
                KafkaPublisher.createPublisher(
                    "localhost:9092,kafka:9092");

            this.subscriber =
                KafkaSubscriber.createSubscriber(
                    "localhost:9092,kafka:9092",
                    List.of(topic));

        } catch (Exception e) {

            System.out.println("Kafka disabled");

            this.publisher = null;
            this.subscriber = null;
        }
    }

    public void publishPost(Message msg, Set<String> local) {

        PostEvent event = new PostEvent();

        event.setSid(version.incrementAndGet());

        event.setMsg(msg);
        event.setLocalRecipients(local);

        System.out.println("PUBLISH POST SID=" + event.getSid());
        publisher.publish(topic, gson.toJson(event));
        System.out.println("PUBLISHED OK");
    }

    public void publishRemoveInbox(String name,
                                   String mid,
                                   String pwd) {

        DeleteInboxEvent event = new DeleteInboxEvent();

        // atomic version generation
        event.setSid(version.incrementAndGet());

        event.setMid(mid);
        event.setName(name);
        event.setPwd(pwd);

        publisher.publish(topic, gson.toJson(event));
    }

    public void publishDeleteMessage(String mid) {

        DeleteMessageEvent event = new DeleteMessageEvent();

        // atomic version generation
        event.setSid(version.incrementAndGet());

        event.setMid(mid);

        publisher.publish(topic, gson.toJson(event));
    }

    public void start() {

        if (subscriber == null)
            return;

        subscriber.start(record -> {

            try {

                Event base =
                    gson.fromJson(record.value(), Event.class);

                synchronized (version) {

                    long current = version.get();
                    long incoming = base.getSid();

                    if (incoming <= current)
                        return;

                    switch (base.getType()) {

                        case "POST" -> {

                            PostEvent e =
                                gson.fromJson(
                                    record.value(),
                                    PostEvent.class);

                            service.applyPost(e);
                        }

                        case "REMOVE_INBOX" -> {

                            DeleteInboxEvent e =
                                gson.fromJson(
                                    record.value(),
                                    DeleteInboxEvent.class);

                            service.applyRemoveInbox(e);
                        }

                        case "DELETE_MESSAGE" -> {

                            DeleteMessageEvent e =
                                gson.fromJson(
                                    record.value(),
                                    DeleteMessageEvent.class);

                            service.applyDeleteMessageEvent(e);
                        }
                    }

                    // set exact version processed
                    version.set(incoming);
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }
}