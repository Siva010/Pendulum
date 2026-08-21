# ---- build ------------------------------------------------------------------
FROM eclipse-temurin:21-jdk-jammy AS build
WORKDIR /build

# Wrapper and POMs first, on their own layer. Dependencies change far less often than source, so
# this layer survives most rebuilds and the ~90MB of Maven downloads happens once rather than on
# every code change.
COPY mvnw ./
COPY .mvn/ .mvn/
COPY pom.xml ./
COPY pendulum-core/pom.xml pendulum-core/
COPY pendulum-server/pom.xml pendulum-server/
RUN chmod +x mvnw && ./mvnw -B -ntp -q dependency:go-offline

COPY pendulum-core/src pendulum-core/src
COPY pendulum-server/src pendulum-server/src

# Tests are skipped here on purpose: they need a Docker daemon for Testcontainers, and running
# Docker inside the image build is a rabbit hole. CI runs the suite before anything is built.
RUN ./mvnw -B -ntp -q package -DskipTests

# ---- runtime ----------------------------------------------------------------
FROM eclipse-temurin:21-jre-jammy AS runtime
WORKDIR /app

# curl is not in the JRE image and the healthcheck runs *inside* the container, so without this the
# check fails permanently and the container reports unhealthy while serving traffic perfectly.
RUN apt-get update     && apt-get install -y --no-install-recommends curl     && rm -rf /var/lib/apt/lists/*

# Non-root. The process needs to read a jar and open a socket, nothing else.
RUN groupadd --system pendulum && useradd --system --gid pendulum --no-create-home pendulum

COPY --from=build --chown=pendulum:pendulum /build/pendulum-server/target/pendulum-server-*.jar app.jar

USER pendulum
EXPOSE 8080

# MaxRAMPercentage rather than a fixed -Xmx: the JVM reads the container's memory limit, so the
# heap follows whatever the orchestrator grants instead of being wrong in both directions.
ENV JAVA_OPTS="-XX:MaxRAMPercentage=70 -XX:+ExitOnOutOfMemoryError"

# Kubernetes sends SIGTERM and waits terminationGracePeriodSeconds. The drain must fit inside that
# window (pendulum.drain-timeout, 20s by default) or the pod is killed mid-drain and every job it
# held waits out its full lease before anyone else can touch it.
STOPSIGNAL SIGTERM

HEALTHCHECK --interval=15s --timeout=3s --start-period=40s --retries=3 \
    CMD ["sh", "-c", "curl -fsS http://localhost:8080/actuator/health || exit 1"]

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar app.jar"]
