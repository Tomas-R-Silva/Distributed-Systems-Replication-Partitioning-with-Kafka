FROM nunopreguica/sd2526tpbase

# working directory inside docker image
WORKDIR /home/sd

ADD hibernate.cfg.xml .
ADD messages.props .

COPY tls/*.ks /home/sd/

# copy the jar created by assembly to the docker image
COPY target/sd*.jar sd2526.jar

CMD ["java", "-cp", "sd2526.jar", "-Djavax.net.ssl.keyStore=/home/sd/users-ourorg2-server.ks", "-Djavax.net.ssl.keyStorePassword=password", "-Djavax.net.ssl.trustStore=/home/sd/client-truststore.ks", "-Djavax.net.ssl.trustStorePassword=changeit", "sd2526.trab.impl.grpc.servers.GrpcUsersServer"]