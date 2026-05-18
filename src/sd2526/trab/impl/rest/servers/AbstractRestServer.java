package sd2526.trab.impl.rest.servers;

import java.net.URI;
import java.net.UnknownHostException;
import java.util.logging.Logger;

import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import sd2526.trab.impl.discovery.Discovery;
import sd2526.trab.impl.java.servers.AbstractServer;
import sd2526.trab.impl.utils.IP;



public abstract class AbstractRestServer extends AbstractServer {
	private static final String SERVER_BASE_URI = "https://%s:%s/rest";
	static {
		System.setProperty("java.net.preferIPv4Stack", "true");
		System.setProperty("java.util.logging.SimpleFormatter.format", "%4$s: %5$s\n");
	}

	final protected int port;

	protected AbstractRestServer(Logger log, String service, int port) throws UnknownHostException{
		super(log, service, String.format(SERVER_BASE_URI, IP.hostname(), port));
		this.port = port;
	}

	protected void start() {
		try {
			ResourceConfig config = new ResourceConfig();
			registerResources(config);

			var uri = URI.create("https://0.0.0.0:%s/rest".formatted(port));
			Log.info("Starting server at: " + uri);

			JdkHttpServerFactory.createHttpServer(uri, config, javax.net.ssl.SSLContext.getDefault());

			if (service == null) {
				Log.info("ERROR: service name is null, cannot announce");
				return;
			}

			Log.info("Announcing: " + serviceName() + " @ " + super.serverURI);
			Discovery.getInstance().announce(serviceName(), super.serverURI);

			Log.info(String.format("%s Server ready @ %s\n", service, serverURI));

		} catch (Exception e) {
			Log.info("FATAL: Server startup failed — " + e.getMessage());
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
	
	abstract void registerResources( ResourceConfig config );
}
