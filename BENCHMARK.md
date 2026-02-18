# Benchmark Anleitung

Dieses Projekt enthält einen einfachen Load-Test (Benchmark), um den Durchsatz von Schreib- und Leseoperationen zu messen.

## Vorbereitung

### 1. Datenbank starten
Starte eine PostgreSQL-Instanz via Docker:

```bash
docker run --name kvstore-postgres \
  -e POSTGRES_USER=kvuser \
  -e POSTGRES_PASSWORD=kvpassword \
  -e POSTGRES_DB=kvstore \
  -p 5432:5432 \
  -d postgres:16-alpine \
  -c max_connections=500
```

### 2. Anwendung starten
Baue und starte die Anwendung im Produktionsmodus (ohne Dev-Mode Overhead):

```bash
# Bauen
./mvnw clean package -DskipTests

# Starten
export QUARKUS_DATASOURCE_USERNAME=kvuser
export QUARKUS_DATASOURCE_PASSWORD=kvpassword
java -jar target/quarkus-app/quarkus-run.jar
```

Warte, bis die Anwendung gestartet ist (Log: `Listening on: http://0.0.0.0:8080`).

## Benchmark ausführen

In einem neuen Terminal-Fenster:

```bash
./mvnw -Dtest=SimpleBenchmarkTest test
```

## Ergebnisse

Der Benchmark führt standardmäßig **1000 Schreiboperationen** und **1000 Leseoperationen** mit **20 parallelen Threads** aus.
Die Ergebnisse werden im Terminal ausgegeben:

```text
WRITE RESULTS:
Total Time: 1132 ms
Throughput: 883,39 ops/sec
Avg Latency: 1,13 ms/op

READ RESULTS:
Total Time: 275 ms
Throughput: 3636,36 ops/sec
Avg Latency: 0,28 ms/op
```
