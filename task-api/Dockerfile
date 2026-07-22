FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
COPY common/pom.xml common/
COPY task-api/pom.xml task-api/
COPY task-worker/pom.xml task-worker/
RUN mvn -B -pl task-api -am dependency:go-offline

COPY common common
COPY task-api task-api
RUN mvn -B -pl task-api -am package -DskipTests

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /app/task-api/target/task-api-*.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
