package sd2526.trab.client.grpc;

import java.io.FileInputStream;
import java.net.URI;
import java.security.KeyStore;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.logging.Logger;

import javax.net.ssl.TrustManagerFactory;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import sd2526.trab.api.java.Result;
import sd2526.trab.api.java.Result.ErrorCode;;

public class GrpcClient {

    final Logger logger;
    final protected Channel channel;


    protected GrpcClient(URI serverURI, Logger logger) {
        this.logger = logger;

        String trustStoreFilename = System.getProperty("javax.net.ssl.trustStore");
        String trustStorePassword = System.getProperty("javax.net.ssl.trustStorePassword");
        

        try {
            KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
            try (FileInputStream input = new FileInputStream(trustStoreFilename)) {
                trustStore.load(input, trustStorePassword.toCharArray());
            }
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(
                TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(trustStore);

            SslContext context = GrpcSslContexts.configure(
                SslContextBuilder.forClient().trustManager(trustManagerFactory)
                ).build();
            this.channel = NettyChannelBuilder
                .forAddress(serverURI.getHost(), serverURI.getPort())
                .sslContext(context)
                .enableRetry()
                .build();
        } catch (Exception e) {
        throw new RuntimeException("Failed to initialize SSL context", e);
        }
    }

    protected <T> Result<T> processResponse(Supplier<T> func) {
        try {
            return Result.ok(func.get());
        } catch (StatusRuntimeException sre) {
            logger.info("Exception:" + sre.getMessage());
            // sre.printStackTrace();
            return Result.error(statusToErrorCode(sre.getStatus()));
        } catch (Exception x) {
            x.printStackTrace();
            return Result.error(ErrorCode.INTERNAL_ERROR);
        }
    }

    protected Result<Void> processResponse(Runnable proc) {
        try {
            proc.run();
            return Result.ok();
        } catch (StatusRuntimeException sre) {
            logger.info("Exception:" + sre.getMessage());
            return Result.error(statusToErrorCode(sre.getStatus()));
        } catch (Exception x) {
            x.printStackTrace();
            return Result.error(ErrorCode.INTERNAL_ERROR);
        }
    }

    protected static ErrorCode statusToErrorCode(Status status) {
        return switch (status.getCode()) {
            case OK -> ErrorCode.OK;
            case NOT_FOUND -> ErrorCode.NOT_FOUND;
            case ALREADY_EXISTS -> ErrorCode.CONFLICT;
            case PERMISSION_DENIED -> ErrorCode.FORBIDDEN;
            case INVALID_ARGUMENT -> ErrorCode.BAD_REQUEST;
            case UNIMPLEMENTED -> ErrorCode.NOT_IMPLEMENTED;
            default -> ErrorCode.INTERNAL_ERROR;
        };
    }
}
