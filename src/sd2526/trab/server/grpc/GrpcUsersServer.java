package sd2526.trab.server.grpc;

import java.io.FileInputStream;
import java.net.InetAddress;
import java.security.KeyStore;
import java.util.logging.Logger;

import javax.net.ssl.KeyManagerFactory;

import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.ServerCredentials;
import sd2526.trab.api.java.Users;
import sd2526.trab.discovery.Discovery;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyServerBuilder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

public class GrpcUsersServer {
	public static final int PORT = 9000;

	private static final String GRPC_CTX = "/grpc";
	private static final String SERVER_BASE_URI = "grpc://%s:%s%s";

	private static Logger Log = Logger.getLogger(GrpcUsersServer.class.getName());

	public static void main(String[] args) throws Exception {

		String keyStoreFilename = System.getProperty("javax.net.ssl.keyStore");
 		String keyStorePassword = System.getProperty("javax.net.ssl.keyStorePassword");
		KeyStore keystore = KeyStore.getInstance(KeyStore.getDefaultType());
        try(FileInputStream input = new FileInputStream(keyStoreFilename)) {
            keystore.load(input, keyStorePassword.toCharArray());
        }
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(
            KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keystore, keyStorePassword.toCharArray());

        SslContext context = GrpcSslContexts.configure(
            SslContextBuilder.forServer(keyManagerFactory)
            ).build();

		GrpcUsersServerController stub = new GrpcUsersServerController();

		Server server = NettyServerBuilder.forPort(PORT)
            .addService(stub).sslContext(context).build();

		String serverURI = String.format(SERVER_BASE_URI, InetAddress.getLocalHost().getHostName(), PORT, GRPC_CTX);

		String hostname = InetAddress.getLocalHost().getHostName();
		int dot = hostname.indexOf('.');
		String domain = (dot >= 0 && dot < hostname.length() - 1)
				? hostname.substring(dot + 1)
				: hostname;

		Discovery.announce(Users.SERVICE_NAME, domain, serverURI);

		Log.info(String.format("Users gRPC Server ready @ %s\n", serverURI));
		server.start().awaitTermination();
	}
}
