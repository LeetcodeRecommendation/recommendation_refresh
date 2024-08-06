FROM openjdk:24
COPY . /usr/src/app
WORKDIR /usr/src/app
RUN chmod -R 777 ./
RUN ./mvnw package -DskipTests
ENTRYPOINT ["java","-jar","target/job_scheduling-1.0.0.jar"]
