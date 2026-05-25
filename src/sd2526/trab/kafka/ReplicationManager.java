package sd2526.trab.kafka;

import static sd2526.trab.api.java.Result.error;
import static sd2526.trab.api.java.Result.ErrorCode.BAD_REQUEST;
import static sd2526.trab.impl.java.servers.JavaMessages.REMOTE_COMM_DEADLINE;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.google.gson.Gson;

import sd2526.trab.api.Message;
import sd2526.trab.api.User;
import sd2526.trab.api.java.Result;
import sd2526.trab.impl.db.DB;
import sd2526.trab.impl.java.clients.Clients;
import sd2526.trab.impl.java.servers.InboxEntry;
import sd2526.trab.impl.java.servers.JavaMessages;
import sd2526.trab.impl.utils.IP;
import sd2526.trab.impl.utils.JSON;
import sd2526.trab.impl.utils.SyncPoint;
import sd2526.trab.kafka.Events.DeleteInboxEvent;
import sd2526.trab.kafka.Events.DeleteMessageEvent;
import sd2526.trab.kafka.Events.Event;
import sd2526.trab.kafka.Events.PostEvent;
import sd2526.trab.api.java.Result.ErrorCode;

public class ReplicationManager extends JavaMessages{

    private static Logger Log = Logger.getLogger(ReplicationManager.class.getName());

    private KafkaPublisher publisher;
    private KafkaSubscriber subscriber;

    private final Gson gson;

    SyncPoint syncPoint;

    private boolean kafkaEnabled = false;

    static final AtomicLong counter = new AtomicLong(0L);

    public ReplicationManager() {
        this.syncPoint = SyncPoint.getSyncPoint();
        this.gson = new Gson();
        initKafka();
        start();
    }

    private void initKafka() {
        KafkaUtils.createTopic(IP.domain());
        this.publisher = KafkaPublisher.createPublisher("localhost:9092,kafka:9092");
        this.subscriber = KafkaSubscriber.createSubscriber("localhost:9092,kafka:9092",List.of(IP.domain()));
    }

    public void start() {
        if (!kafkaEnabled || subscriber == null){
            return;
        }

        subscriber.start( new RecordProcessor() {
			@Override
			public void onReceive(ConsumerRecord<String, String> r) {
                long offset = r.offset();
                Event event = gson.fromJson(r.value(), Event.class);
                switch(event.getType()){
                    case "POST" -> {
                        PostEvent e = gson.fromJson(r.value(), PostEvent.class);
                        Message msg = e.getMsg();
                        List<String> localRecipients = e.getLocalRecipients();
                        //aqui vamos fazer as alterações à DB de modo a que seja em todas as réplicas
                        //como o deliverToKnownLocalRecipients, ...
                        var res = postToLocalInboxes(localRecipients, msg);
                        syncPoint.setResult(offset, res.isOK() ? msg.getId() : null); //says that have written in the DB
                    }

                    case "REMOVE_INBOX" -> {
                        DeleteInboxEvent e = gson.fromJson(r.value(), DeleteInboxEvent.class);
                        getUser(e.getName(), e.getPwd() )
                            .then( () -> DB.deleteOne( new InboxEntry(e.getMid(), e.getName()) ) ).mapToVoid()
                            .then( () -> {
                                gcDeletedMessageCache.put( e.getMid(), e.getMid() );
                            });
                        syncPoint.setResult(offset, "");
                    }

                    case "DELETE_MESSAGE" -> {
                        DeleteMessageEvent e = gson.fromJson(r.value(), DeleteMessageEvent.class);
                        //aqui vamos fazer as alterações à DB de modo a que seja em todas as réplicas
                        deleteFromLocalInbox(e.getMid());
                        syncPoint.setResult(offset, "");
                    }
                }
				
			}
		});

    }


    @Override
    public Result<String> doAsyncPost(User sender, Message msg) {

        if(msg.getId() == null){
            return error(BAD_REQUEST);
        }

		return getCachedMessage(msg.originId()).mapValue(Message::getId).orElse(() -> {
			
			msg.setId("%s+%04d".formatted(THIS_DOMAIN, counter.incrementAndGet()));
			
			messagesCache.put(msg.originId(), new Message( msg )); // For ensuring idempotency...
			
			msg.setSender("%s <%s@%s>".formatted(sender.getDisplayName(), sender.getName(), sender.getDomain()));

			messagesCache.put(msg.getId(), msg); // For enabling delete of messages...

			var localAdresses = getLocalRecipientAddresses(msg);
			var remoteAddresses = getRemoteRecipientAddresses(msg);

			System.out.println("Local Recipients:" + localAdresses);
			System.out.println("Remote Recipients:" + remoteAddresses);
			System.out.println("Local Adresses Size" + localAdresses.size());

			if (localAdresses.size() > 0) {
                PostEvent event = new PostEvent();
                event.setMsg(msg);
                event.setLocalRecipients(localAdresses);
                long offset = publisher.publish(IP.domain(), JSON.encode(event));
                syncPoint.waitForResult(offset); // wait until subscriber has written to DB
            }

			if (remoteAddresses.size() > 0) {

				var remoteTargets = remoteAddresses.stream().collect(
						Collectors.groupingBy( super::getDomain, Collectors.mapping( address -> address, Collectors.toSet())));

				for (var e : remoteTargets.entrySet()) {
					var domain = e.getKey();
					var domainRecipientAddressess = e.getValue();
					
					jobs.submit(domain, () -> {
						var res = super.reTry(() -> Clients.AdminMessagesClient.get(domain).remotePostMessage(msg), REMOTE_COMM_DEADLINE);
						if (res.error() == ErrorCode.TIMEOUT) {
							for (var address : domainRecipientAddressess)
								postToLocalInboxes(Set.of(msg.senderAddress()), msg.cloneWithTimeout(address));
						}
					});
					
				}
			}
			return Result.ok(msg.getId());
		});
	}

    @Override
    public Result<Void> doAsyncDelete( Message msg ) {
        DeleteMessageEvent event = new DeleteMessageEvent();
        event.setMid(msg.getId());

        var domains = msg.getDestination().stream()
                .map(r -> r.split("@")[1])
                .collect(Collectors.toSet());

        for (var domain : domains) {
            if (domain.equals(IP.domain())) {
                // delete in all replicas
                long offset = publisher.publish(IP.domain(), JSON.encode(event));
                syncPoint.waitForResult(offset);
            } else {
                // remote domain, faz diretamente
                jobs.submit(domain, () ->
                        super.reTry(() -> Clients.AdminMessagesClient.get(domain)
                                .remoteDeleteMessage(msg.getId()), REMOTE_COMM_DEADLINE));
            }
        }
        return Result.ok();
    }

    @Override
	public Result<Void> remoteDeleteMessage(String mid) {
        //if??
        DeleteMessageEvent event = new DeleteMessageEvent();
        event.setMid(mid);
		long offset = publisher.publish(IP.domain(), JSON.encode(event));
		syncPoint.waitForResult(offset);
        return Result.ok();
	}

    @Override
    public Result<Void> remotePostMessage(Message msg){
        var localAddresses = getLocalRecipientAddresses(msg);
        PostEvent event = new PostEvent();
        event.setMsg(msg);
        event.setLocalRecipients(localAddresses);
        long offset = publisher.publish(IP.domain(), JSON.encode(event));
        syncPoint.waitForResult(offset);
        return Result.ok();
    }

    @Override
    public Result<Void> removeInboxMessage(String name, String mid, String pwd){
        return getUser(name, pwd).thenWith(user -> {
            DeleteInboxEvent event = new DeleteInboxEvent();
            event.setName(name);
            event.setMid(mid);
            event.setPwd(pwd);
            long offset = publisher.publish(IP.domain(), JSON.encode(event));
            syncPoint.waitForResult(offset);
            return Result.ok();
        });
    }

    private boolean canPublish() {
        return kafkaEnabled && publisher != null;
    }

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (Exception ignored) {}
    }
}