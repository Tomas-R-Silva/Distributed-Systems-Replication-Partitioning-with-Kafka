package sd2526.trab.kafka;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.hibernate.sql.Delete;

import sd2526.trab.api.Message;
import sd2526.trab.impl.java.servers.JavaMessages;
import sd2526.trab.kafka.Events.DeleteInboxEvent;
import sd2526.trab.kafka.Events.DeleteMessageEvent;
import sd2526.trab.kafka.Events.PostEvent;
import com.google.gson.Gson;


public class ReplicationManager {
    
    private  KafkaPublisher publisher;
    private  KafkaSubscriber subscriber;
    private String topic;
    private AtomicLong counter;
    private Gson gson;

    private JavaMessages service;

    public ReplicationManager(String topic, JavaMessages service, AtomicLong counter){
        this.publisher = KafkaPublisher.createPublisher("kafka:9092");
        this.subscriber = KafkaSubscriber.createSubscriber("kafka:9092", List.of(topic));
        this.topic = topic;
        this.counter = counter;
        this.gson = new Gson();
        this.service = service;
    }

    public void publishPost(Message msg, Set<String>local){

        PostEvent event = new PostEvent();
        event.setSid(counter.incrementAndGet());
        event.setMsg(msg);
        event.setLocalRecipients(local);
        String value = gson.toJson(event);

        publisher.publish(topic, value);
    }

    public void publishRemoveInbox(AtomicLong counter, String name, String mid, String pwd){

        DeleteInboxEvent event = new DeleteInboxEvent();
        event.setSid(counter.incrementAndGet());
        event.setMid(mid);
        event.setName(name);
        event.setPwd(pwd);
        String value = gson.toJson(event);

        publisher.publish(topic, value);
    }

    public void publishDeleteMessage(String mid) {

        DeleteMessageEvent event = new DeleteMessageEvent();

        event.setSid(counter.incrementAndGet());
        event.setMid(mid);

        publisher.publish(topic, gson.toJson(event));
    }

    public void start(){
        subscriber.start(record -> {
            PostEvent event =
                gson.fromJson(record.value(), PostEvent.class);
            
            service.applyPost(event);
        });

        subscriber.start(record -> {
            DeleteInboxEvent event =
                gson.fromJson(record.value(), DeleteInboxEvent.class);

            service.applyRemoveInbox(event);
        });

        subscriber.start(record -> {
            DeleteMessageEvent event =
                gson.fromJson(record.value(), DeleteMessageEvent.class);

            service.applyDeleteMessageEvent(event);
        });
    }


}
