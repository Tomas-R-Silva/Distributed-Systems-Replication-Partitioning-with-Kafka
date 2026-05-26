package sd2526.trab.impl.rest.servers;

import java.util.logging.Logger;

import org.glassfish.jersey.server.ResourceConfig;
import java.util.concurrent.atomic.AtomicLong;

import sd2526.trab.api.java.Messages;
import sd2526.trab.kafka.ReplicationManager;
import sd2526.trab.kafka.VersionHeaderHandler;

public class RestMessagesRepServer extends AbstractRestServer{
    public static final int PORT = 9092;
   
    private static Logger Log = Logger.getLogger(RestMessagesRepServer.class.getName());
    final AtomicLong version = new AtomicLong(0L);

    RestMessagesRepServer() throws Exception{
        super(Log, Messages.SERVICE_NAME, PORT);
    }

    @Override
    void registerResources(ResourceConfig config) {
        ReplicationManager.getInstance();
        config.register(RestMessagesRepResource.class);
        config.register( VersionHeaderHandler.class);
    }

    public static void main(String[] args) throws Exception{
		new RestMessagesRepServer().start();
		
	}

}
