# Realtime Voting System

In this project, we'll build an end to end realtime voting system using technologies like Apache Kafka, Apache Spark and Streamlit. The tutorial is based on [Realtime Voting System | End-to-End Data Engineering Project](https://www.youtube.com/watch?v=X-JnC9daQxE)

### System flow

![system_flow](./images/system_flow.jpg)

### System architecture

![system_architecture](./images/system_architecture.jpg)

### Set up virtual env

Create new virtual environment

> python3 -m venv .venv

Activate virtual environment

> source .venv/bin/activate

Deactivate virtual environment

> deactivate

Install required packages

> pip3 install -r requirements.txt

Create `.env` file with following content:

```bash
PG_HOST=...
PG_USER=...
PG_PASSWORD=...
PG_PORT=...
PG_DATABASE=...
```

### Docker for system architecture

Create [docker-compose.yml](./docker-compose.yml) file and execute by

> docker-compose up -d

Then go inside `broker` container and execute following command in order to check if the broker is set up successfully

> kafka-topics --list --bootstrap-server broker:29092

If want to see the content of kafka topic, use this command:

> kafka-console-consumer --topic {topic_name} --bootstrap-server broker:29092

> kafka-console-consumer --topic voters_topic --bootstrap-server broker:29092

To delete topic

> kafka-topics --delete --topic {topic_name} --bootstrap-server broker:29092

### Access postgres

Go to `postgres` container and execute command

> psql -U postgres

Access database voting

> \c voting

List all tables

> \d

### Postgresql driver

Go to [Postgresql JDBC](https://jdbc.postgresql.org/download/) and download Java 8. Then copy and put into the working directory.

### Streamlit

Run streamlit command

> streamlit run streamlit-app.py

### STEP TO RUN

1. run python main.py to create all the table
2. run voting.py to generate votes
3. run python spark-streaming.py to process the data.
4. run streamlit run streamlit-app.py to visualize result.
