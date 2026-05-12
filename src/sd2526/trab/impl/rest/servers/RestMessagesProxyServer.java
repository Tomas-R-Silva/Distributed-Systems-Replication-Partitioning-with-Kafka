package sd2526.trab.impl.rest.servers;

import java.util.logging.Logger;

import org.glassfish.jersey.server.ResourceConfig;

import sd2526.trab.api.java.Messages;

public class RestMessagesProxyServer extends AbstractRestServer{
    public static final int PORT = 9999;
	
	private static Logger Log = Logger.getLogger(RestMessagesProxyServer.class.getName());

	RestMessagesProxyServer() throws Exception{
		super(Log, null, PORT); //what is the service name
	}

	@Override
	void registerResources(ResourceConfig config) {
		config.register(RestMessagesProxyResource.class);
	}

	public static void main(String[] args) throws Exception{
		new RestMessagesProxyServer().start();
	}
}
