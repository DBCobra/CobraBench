FROM openjdk:8
WORKDIR /usr/src/txnbench

ENV GOOGLE_APPLICATION_CREDENTIALS="/usr/src/txnbench/cobra_key.json"

EXPOSE 8980
ENTRYPOINT ["java","-ea","-jar","target/txnTest-1-jar-with-dependencies.jar"]
