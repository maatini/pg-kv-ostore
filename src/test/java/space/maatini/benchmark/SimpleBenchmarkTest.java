package space.maatini.benchmark;

import org.junit.jupiter.api.Test;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class SimpleBenchmarkTest {

    private static final String BASE_URL = "http://localhost:8080/api/v1";
    private static final HttpClient client = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    private static final int NUM_OPERATIONS = 1000;
    private static final int CONCURRENCY = 20;

    @Test
    public void benchmark() throws Exception {
        System.out.println("Starting Benchmark...");
        System.out.println("Operations: " + NUM_OPERATIONS);
        System.out.println("Concurrency: " + CONCURRENCY);

        // 1. Create Bucket
        createBucket("bench-bucket");

        // 2. Benchmark Writes
        long startWrite = System.currentTimeMillis();
        runBenchmark("WRITE", i -> putKey("bench-bucket", "key-" + i, "value-" + i));
        long endWrite = System.currentTimeMillis();
        printStats("WRITE", startWrite, endWrite);

        // 3. Benchmark Reads
        long startRead = System.currentTimeMillis();
        runBenchmark("READ", i -> getKey("bench-bucket", "key-" + i));
        long endRead = System.currentTimeMillis();
        printStats("READ", startRead, endRead);
    }

    private static void runBenchmark(String name, Operation operation) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(CONCURRENCY);
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);

        for (int i = 0; i < NUM_OPERATIONS; i++) {
            final int index = i;
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    boolean success = operation.execute(index);
                    if (success)
                        successCount.incrementAndGet();
                    else
                        errorCount.incrementAndGet();
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    // e.printStackTrace();
                }
            }, executor));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        executor.shutdown();

        System.out.println(name + " finished. Success: " + successCount.get() + ", Errors: " + errorCount.get());
    }

    private static void printStats(String name, long start, long end) {
        long durationMs = end - start;
        double seconds = durationMs / 1000.0;
        double opsPerSec = NUM_OPERATIONS / seconds;
        double avgLatency = (double) durationMs / NUM_OPERATIONS;

        System.out.println("--------------------------------------------------");
        System.out.println(name + " RESULTS:");
        System.out.println("Total Time: " + durationMs + " ms");
        System.out.println("Throughput: " + String.format("%.2f", opsPerSec) + " ops/sec");
        System.out.println("Avg Latency: " + String.format("%.2f", avgLatency) + " ms/op");
        System.out.println("--------------------------------------------------");
    }

    private static void createBucket(String name) throws Exception {
        String json = "{\"name\":\"" + name + "\",\"description\":\"Benchmark Bucket\"}";
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(BASE_URL + "/kv/buckets"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 201 && response.statusCode() != 409) { // 409 if exists
            System.err.println("Failed to create bucket: " + response.statusCode() + " " + response.body());
        } else {
            System.out.println("Bucket created (or already exists).");
        }
    }

    private static boolean putKey(String bucket, String key, String value) {
        try {
            String json = "{\"value\":\"" + value + "\"}";
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(BASE_URL + "/kv/buckets/" + bucket + "/keys/" + key))
                    .header("Content-Type", "application/json")
                    .PUT(HttpRequest.BodyPublishers.ofString(json))
                    .build();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            return response.statusCode() == 200 || response.statusCode() == 201;
        } catch (Exception e) {
            return false;
        }
    }

    private static boolean getKey(String bucket, String key) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(BASE_URL + "/kv/buckets/" + bucket + "/keys/" + key))
                    .GET()
                    .build();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            return response.statusCode() == 200;
        } catch (Exception e) {
            return false;
        }
    }

    @FunctionalInterface
    interface Operation {
        boolean execute(int index) throws Exception;
    }
}
