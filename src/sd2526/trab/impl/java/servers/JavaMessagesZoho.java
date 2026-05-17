package sd2526.trab.impl.java.servers;

import static sd2526.trab.api.java.Result.error;
import static sd2526.trab.api.java.Result.ok;
import static sd2526.trab.api.java.Result.ErrorCode.BAD_REQUEST;
import static sd2526.trab.api.java.Result.ErrorCode.FORBIDDEN;
import static sd2526.trab.api.java.Result.ErrorCode.INTERNAL_ERROR;

import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
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
import sd2526.trab.impl.zoho.Zoho;
import sd2526.trab.impl.zoho.zoho.msgs.*;;

public class JavaMessagesZoho extends JavaMessages{

    private static Logger Log = Logger.getLogger(JavaMessagesZoho.class.getName());
    
    public JavaMessagesZoho(){
        super();
    }

    //Done - to review
    @Override
    public Result<List<String>> searchInbox(String name, String pwd, String query) {
		Log.info( () -> "searchInbox : name = %s, pwd = %s, query=%s\n".formatted(name, pwd, query));
        
        try{
            return Result.ok(Zoho.getInstance().searchInbox(query));
            
        }catch(Exception e){
            e.printStackTrace();
        }

        return null;
	}

    @Override
	public Result<Void> removeInboxMessage(String name, String mid, String pwd) {
		Log.info( () -> "removeInboxMessage : name = %s, mid = %s, pwd = %s\n".formatted(name, mid, pwd));
		
		return getUser(name, pwd )
				.then( () -> DB.deleteOne( new InboxEntry(mid, name) ) ).mapToVoid()
				.then( () -> {
					gcDeletedMessageCache.put( mid, mid );
				});
	}

    //Done - to review
    @Override
    protected void deliverToKnownLocalRecipients(Collection<String> addresses, Message msg) {
		Log.info( () -> "deliverToKnownLocalRecipients : local known addresses = %s, msg = %s\n".formatted(addresses, msg));

        for( var address : addresses){
            try{
                Zoho.getInstance().postMessage(address, msg);
            }catch( Exception e){
                e.printStackTrace();
            }
        }

	}

    //Done - to review
    @Override
    protected void reportUnknownLocalRecipients(Collection<String> addresses, Message msg) {
		Log.info( () -> "reportUnknownLocalRecipients : unknown addresses = %s, msg = %s\n".formatted(addresses, msg));

		var senderDomain = super.getDomain( msg.senderAddress() );
		
		try {
			for( var recipientAddress : addresses ) {
				var errorMsg = msg.cloneWithUserNotFound( recipientAddress );
				if( super.isLocalDomain( senderDomain ) ) {

                    Zoho.getInstance().postMessage(recipientAddress, errorMsg);
				}
				else doAsyncRemotePost(senderDomain, errorMsg);
			}
		} catch( Exception x ) {
			x.printStackTrace();			
		}
	}	

}
