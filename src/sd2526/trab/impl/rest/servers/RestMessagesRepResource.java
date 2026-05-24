package sd2526.trab.impl.rest.servers;

import java.util.List;

import jakarta.inject.Singleton;
import sd2526.trab.api.Message;
import sd2526.trab.api.java.Messages;
import sd2526.trab.api.rest.RestMessages;
import sd2526.trab.impl.api.java.AdminMessages;
import sd2526.trab.impl.api.rest.RestAdminMessages;
import sd2526.trab.impl.java.servers.JavaMessagesKafka;
import jakarta.ws.rs.ext.Provider;
import sd2526.trab.kafka.VersionHeaderHandler;

@Singleton
@Provider
public class RestMessagesRepResource extends RestResource implements RestMessages, RestAdminMessages{

    Messages impl;

    synchronized Messages impl() {
		if( impl == null )
			impl =  JavaMessagesKafka.getInstance();	
		return impl;
	}

    public RestMessagesRepResource() {}

    @Override
	public String postMessage(String pwd, Message msg) {
        String mid = super.resultOrThrow( impl().postMessage(pwd, msg));
        VersionHeaderHandler.version.set(((JavaMessagesKafka) impl()).version.get());       
        return mid;
	}
	
	@Override
	public Message getMessage(String name, String mid, String pwd) {
		Message message = super.resultOrThrow( impl().getInboxMessage(name, mid, pwd));
        VersionHeaderHandler.version.set(((JavaMessagesKafka) impl()).version.get());
        return message;
	}
	
	@Override
	public List<String> getMessages(String name, String pwd, String query) {
		if( query != null && ! query.isEmpty() ){
            List<String> mids = super.resultOrThrow( impl().searchInbox(name, pwd, query));
            VersionHeaderHandler.version.set(((JavaMessagesKafka) impl()).version.get());
            return mids;
        }
		else{
            List<String> mids = super.resultOrThrow(impl().getAllInboxMessages(name, pwd));
            VersionHeaderHandler.version.set(((JavaMessagesKafka) impl()).version.get());
            return mids;
        }
	}
	
	@Override
	public void removeFromUserInbox(String name, String mid, String pwd) {
		super.resultOrThrow( impl().removeInboxMessage(name, mid, pwd) );
		VersionHeaderHandler.version.set(((JavaMessagesKafka) impl()).version.get());
		
	}
	
	@Override
	public void deleteMessage(String name, String mid, String pwd) {
		super.resultOrThrow( impl().deleteMessage(name, mid, pwd));
		VersionHeaderHandler.version.set(((JavaMessagesKafka) impl()).version.get());
	}

	@Override
	public void remotePostMessage(Message m) {
		super.resultOrThrow( ((AdminMessages)impl()).remotePostMessage(m));
		VersionHeaderHandler.version.set(((JavaMessagesKafka) impl()).version.get());
	}

	@Override
	public void remoteDeleteMessage(String mid) {
		super.resultOrThrow( ((AdminMessages)impl()).remoteDeleteMessage(mid));
		VersionHeaderHandler.version.set(((JavaMessagesKafka) impl()).version.get());
	}

	@Override
	public void remoteDeleteUserInbox(String name) {
		super.resultOrThrow( ((AdminMessages)impl()).remoteDeleteUserInbox(name));
		VersionHeaderHandler.version.set(((JavaMessagesKafka) impl()).version.get());
	}


}
