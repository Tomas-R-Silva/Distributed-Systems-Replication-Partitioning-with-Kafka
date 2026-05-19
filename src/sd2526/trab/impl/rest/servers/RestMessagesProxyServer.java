package sd2526.trab.impl.rest.servers;

import java.util.logging.Logger;

import org.glassfish.jersey.server.ResourceConfig;

import sd2526.trab.api.java.Messages;

public class RestMessagesProxyServer extends AbstractRestServer{
    public static final int PORT = 9999;
	private boolean cleanState = false;
	
	private static Logger Log = Logger.getLogger(RestMessagesProxyServer.class.getName());

	RestMessagesProxyServer(boolean cleanState) throws Exception{
		super(Log, Messages.SERVICE_NAME, PORT); //what is the service name
		this.cleanState = cleanState;
	}

	@Override
	void registerResources(ResourceConfig config) {
		RestMessagesProxyResource.cleanState(cleanState);
		config.register(RestMessagesProxyResource.class);
	}

	public static void main(String[] args) throws Exception{
		new RestMessagesProxyServer(Boolean.parseBoolean(args[0])).start();
		//new RestMessagesProxyServer(true).start();
	}
}
