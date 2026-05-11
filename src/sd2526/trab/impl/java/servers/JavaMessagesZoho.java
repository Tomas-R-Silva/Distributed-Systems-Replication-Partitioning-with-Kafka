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
import sd2526.trab.impl.utils.Sleep;
import sd2526.trab.impl.zoho.Zoho;

public class JavaMessagesZoho extends JavaMessages{

    private static Logger Log = Logger.getLogger(JavaMessagesZoho.class.getName());
    
    public JavaMessagesZoho(){
        super();
    }

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

		DB.transaction((hibernate) -> {
			hibernate.persistOne( msg );
			for( var address : addresses )
				hibernate.persistOne( new InboxEntry( msg.getId(), getName(address) ));
			
			return ok();
		});
		
	}

}
