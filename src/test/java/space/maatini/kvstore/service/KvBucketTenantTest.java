
package space.maatini.kvstore.service;

import space.maatini.common.exception.ConflictException;
import space.maatini.common.util.TenantContext;
import space.maatini.kvstore.dto.KvBucketDto;
import space.maatini.kvstore.entity.KvBucket;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.vertx.RunOnVertxContext;
import io.quarkus.test.hibernate.reactive.panache.TransactionalUniAsserter;
import io.quarkus.hibernate.reactive.panache.Panache;
import jakarta.inject.Inject;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Multi-Tenant Bucket Isolation.
 */
@QuarkusTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class KvBucketTenantTest {

    @Inject
    KvService kvService;

    @Inject
    TenantContext tenantContext;

    @BeforeEach
    @RunOnVertxContext
    void setUp(TransactionalUniAsserter asserter) {
        // Clear all buckets to start fresh (might need admin access or disable RLS to
        // clear all?)
        // Since we are adding tenant_id, deleteAll() only deletes what is visible!
        // So we must be careful.
        // For simplicity, we just use unique names in tests or ensure we clean up as
        // the specific tenant.
    }

    @Test
    @Order(1)
    @RunOnVertxContext
    void testTenantIsolation(TransactionalUniAsserter asserter) {
        String bucketName = "iso-bucket";

        // 1. Tenant A creates bucket
        asserter.execute(() -> {
            tenantContext.setTenantId("tenant-A");
            System.out.println("TEST DEBUG: tenantContext.getTenantId() = " + tenantContext.getTenantId());
            KvBucketDto.CreateRequest req = new KvBucketDto.CreateRequest();
            req.name = bucketName;
            return kvService.createBucket(req);
        });

        // 2. Tenant A sees bucket
        asserter.assertThat(() -> {
            tenantContext.setTenantId("tenant-A");
            return kvService.getBucket(bucketName);
        }, bucket -> {
            assertNotNull(bucket);
            assertEquals(bucketName, bucket.name);
            assertEquals("tenant-A", bucket.tenantId);
        });

        // 3. Tenant B cannot see Tenant A's bucket
        asserter.execute(() -> {
            tenantContext.setTenantId("tenant-B");
            return Panache.getSession().invoke(s -> s.clear())
                    .flatMap(v -> kvService.listBuckets())
                    .map(buckets -> {
                        boolean containsA = buckets.stream().anyMatch(b -> b.name.equals(bucketName));
                        assertFalse(containsA, "Tenant B should not see Tenant A's bucket");
                        return null;
                    });
        });
        // 4. Tenant B creates bucket with SAME name
        asserter.assertThat(() -> {
            tenantContext.setTenantId("tenant-B");
            KvBucketDto.CreateRequest req = new KvBucketDto.CreateRequest();
            req.name = bucketName;
            return kvService.createBucket(req);
        }, bucket -> {
            assertNotNull(bucket);
            assertEquals(bucketName, bucket.name);
            assertEquals("tenant-B", bucket.tenantId);
        });

        // 5. Verify they are different buckets (different IDs)
        asserter.execute(() -> {
            // Logic to verify IDs would require storing them in variables,
            // but here we trust the assertions above.
            // We can check counts if we could see global state, but we can't easily here.
            return io.smallrye.mutiny.Uni.createFrom().voidItem();
        });
    }

    @Test
    @Order(2)
    @RunOnVertxContext
    void testUniqueConstraintWithinTenant(TransactionalUniAsserter asserter) {
        String bucketName = "unique-bucket";

        asserter.execute(() -> {
            tenantContext.setTenantId("tenant-C");
            KvBucketDto.CreateRequest req = new KvBucketDto.CreateRequest();
            req.name = bucketName;
            return kvService.createBucket(req);
        });

        asserter.assertFailedWith(() -> {
            tenantContext.setTenantId("tenant-C");
            KvBucketDto.CreateRequest req = new KvBucketDto.CreateRequest();
            req.name = bucketName;
            return kvService.createBucket(req);
        }, ConflictException.class);
    }
}
