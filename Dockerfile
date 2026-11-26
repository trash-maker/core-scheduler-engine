# JVM image for the engine. Build the application first:  ./mvnw package -DskipTests
FROM eclipse-temurin:21-jre
ENV LANG=C.UTF-8 \
    JAVA_OPTS="-XX:+UseG1GC -XX:MaxRAMPercentage=75 -Djava.util.logging.manager=org.jboss.logmanager.LogManager"
WORKDIR /app
COPY target/quarkus-app/lib/ ./lib/
COPY target/quarkus-app/*.jar ./
COPY target/quarkus-app/app/ ./app/
COPY target/quarkus-app/quarkus/ ./quarkus/
RUN useradd -r -u 185 engine
USER 185
EXPOSE 8080
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/quarkus-run.jar"]
