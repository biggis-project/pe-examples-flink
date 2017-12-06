FROM anapsix/alpine-java

EXPOSE 8090
ENV CONSUL_LOCATION consul

ADD ./target/streampipes-examples-flink.jar  /streampipes-examples-flink.jar

ENTRYPOINT ["java", "-jar", "/streampipes-examples-flink.jar"]
