FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
COPY common/pom.xml common/
COPY task-api/pom.xml task-api/
COPY task-worker/pom.xml task-worker/
RUN mvn -B -pl task-worker -am dependency:go-offline

COPY common common
COPY task-worker task-worker
RUN mvn -B -pl task-worker -am package -DskipTests

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /app/task-worker/target/task-worker-*.jar app.jar
EXPOSE 8081
ENTRYPOINT ["java", "-jar", "app.jar"]
