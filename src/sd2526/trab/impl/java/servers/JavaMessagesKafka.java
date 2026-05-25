package sd2526.trab.impl.java.servers;

import static sd2526.trab.api.java.Result.error;
import static sd2526.trab.api.java.Result.ok;
import static sd2526.trab.api.java.Result.ErrorCode.BAD_REQUEST;
import static sd2526.trab.api.java.Result.ErrorCode.FORBIDDEN;
import static sd2526.trab.api.java.Result.ErrorCode.INTERNAL_ERROR;
import static sd2526.trab.api.java.Result.ErrorCode.NOT_FOUND;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import sd2526.trab.api.Message;
import sd2526.trab.api.User;
import sd2526.trab.api.java.Messages;
import sd2526.trab.api.java.Result;
import sd2526.trab.api.java.Result.ErrorCode;
import sd2526.trab.impl.api.java.AdminMessages;
import sd2526.trab.impl.db.DB;
import sd2526.trab.impl.java.clients.Clients;
import sd2526.trab.impl.utils.IP;
import sd2526.trab.impl.utils.JSON;
import sd2526.trab.impl.utils.Sleep;
import sd2526.trab.impl.utils.SyncPoint;
import sd2526.trab.kafka.ReplicationManager;
import sd2526.trab.kafka.Events.DeleteInboxEvent;
import sd2526.trab.kafka.Events.DeleteMessageEvent;
import sd2526.trab.kafka.Events.PostEvent;

public class JavaMessagesKafka extends JavaMessages{

	private static final int REMOTE_COMM_DEADLINE = 90000;
	protected static final String TOPIC = JavaMessages.THIS_DOMAIN;
    
    	
    final AtomicLong counter = new AtomicLong(0L);
    private final ReplicationManager manager;
	SyncPoint syncPoint;
	
    
    public JavaMessagesKafka(){
        super();
		syncPoint = SyncPoint.getSyncPoint();
        this.manager = new ReplicationManager(TOPIC, this);
		this.manager.start();
    }

    @Override
	public Result<Void> removeInboxMessage(String name, String mid, String pwd) {
		Log.info( () -> "removeInboxMessage : name = %s, mid = %s, pwd = %s\n".formatted(name, mid, pwd));
		

			return getUser(name, pwd)
			.then(() -> {

				manager.publishRemoveInbox(name,mid,pwd);

				return ok();
			})
			.mapToVoid();
	}

	@Override
	public Result<String> postMessage(String pwd, Message msg) {
		Log.info( () -> "postMessage : pwd = %s, msg = %s\n".formatted(pwd, msg));

		syncPoint.waitForVersion();

		return getUser(msg.getSender(), pwd)					
				.thenWith( (user) -> doAsyncPost( user, msg ));			
	}

    @Override
    public Result<String> doAsyncPost(User sender, Message msg) {

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

			if (localAdresses.size() > 0){
				manager.publishPost(msg, new HashSet<>(localAdresses));
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


    public Result<Void> doAsyncDelete( Message msg ) {
		var domains = msg.getDestination().stream().map( r -> r.split("@")[1]).collect( Collectors.toSet() );
		for( var domain : domains )
			if( domain.equals( IP.domain() ))
				deleteFromLocalInbox( msg.getId() );
			// coloco aqui a parte do Kafka?
			else
				jobs.submit(domain, () -> {
					super.reTry(()-> Clients.AdminMessagesClient.get(domain).remoteDeleteMessage(msg.getId()), REMOTE_COMM_DEADLINE);			
				});				
		return Result.ok();
	}
		


    public void applyPost(PostEvent e) {

			Message msg = e.getMsg();
			messagesCache.put(msg.getId(), msg);
			postToLocalInboxes(e.getLocalRecipients(), msg);
			//setResult
		}

		public void applyRemoveInbox(DeleteInboxEvent e){
			
			 DB.deleteOne( new InboxEntry(e.getMid(), e.getName() ) ).mapToVoid()
				.then( () -> {
					gcDeletedMessageCache.put( e.getMid(), e.getMid() );
				});
		}

		public void applyDeleteMessageEvent( DeleteMessageEvent event){
			deleteFromLocalInbox(event.getMid());
		}

}
