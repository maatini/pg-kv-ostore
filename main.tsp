import "@typespec/http";
import "@typespec/rest";
import "@typespec/openapi3";

using TypeSpec.Http;
using TypeSpec.Rest;

@service({
  title: "KV and Object Store Service",
})
@server("http://localhost:8080", "Development server")
namespace Maatini;

/** Multi-tenancy header */
model TenantHeader {
  @header("X-Tenant-ID") tenantId?: string;
}

/** Error response structure */
model ErrorResponse {
  @statusCode statusCode: int32;
  message: string;
}

// --- KV Store Models ---

model KvBucket {
  id: uuid;
  name: string;
  description?: string;
  maxValueSize: int32;
  maxHistoryPerKey: int32;
  ttlSeconds?: int64;
  createdAt: offsetDateTime;
  updatedAt?: offsetDateTime;
}

model KvBucketCreateRequest {
  @minLength(1) @maxLength(255) name: string;
  description?: string;
  maxValueSize?: int32;
  maxHistoryPerKey?: int32;
  ttlSeconds?: int64;
}

model KvBucketUpdateRequest {
  description?: string;
  maxValueSize?: int32;
  maxHistoryPerKey?: int32;
  ttlSeconds?: int64;
}

model KvEntry {
  id: uuid;
  bucket: string;
  key: string;
  value?: string; // Base64 encoded
  revision: int64;
  operation: "PUT" | "DELETE" | "PURGE";
  createdAt: offsetDateTime;
  expiresAt?: offsetDateTime;
}

model KvPutRequest {
  value: string;
  base64: boolean;
  ttlSeconds?: int64;
}

// --- Object Store Models ---

model ObjBucket {
  id: uuid;
  name: string;
  description?: string;
  chunkSize: int32;
  maxObjectSize: int64;
  createdAt: offsetDateTime;
  updatedAt?: offsetDateTime;
}

model ObjBucketCreateRequest {
  @minLength(1) @maxLength(255) name: string;
  description?: string;
  chunkSize?: int32;
  maxObjectSize?: int64;
}

model ObjMetadata {
  id: uuid;
  bucket: string;
  name: string;
  size: int64;
  chunkCount: int32;
  digest: string;
  digestAlgorithm: string;
  contentType?: string;
  description?: string;
  headers?: Record<string>;
  createdAt: offsetDateTime;
  updatedAt?: offsetDateTime;
}

model ObjectInfo {
  name: string;
  size: int64;
  digest: string;
  contentType?: string;
  createdAt: offsetDateTime;
}

model IntegrityResult {
  valid: boolean;
  message: string;
}

// --- API Endpoints ---

@tag("KV Buckets")
@route("/api/v1/kv/buckets")
interface KvBuckets {
  @post create(@body request: KvBucketCreateRequest, ...TenantHeader): KvBucket | ErrorResponse;
  @get list(...TenantHeader): KvBucket[];
  @get read(@path name: string, ...TenantHeader): KvBucket | ErrorResponse;
  @put update(@path name: string, @body request: KvBucketUpdateRequest, ...TenantHeader): KvBucket | ErrorResponse;
  @delete delete(@path name: string, ...TenantHeader): void | ErrorResponse;
  @route("/{name}/purge") @delete purge(@path name: string, ...TenantHeader): { count: int64 } | ErrorResponse;
}

@tag("KV Entries")
@route("/api/v1/kv/buckets/{bucket}/keys")
interface KvEntries {
  @get list(@path bucket: string, ...TenantHeader): string[];
  @get read(@path bucket: string, @path key: string, ...TenantHeader): KvEntry | ErrorResponse;
  
  @route("/{key}/revision/{revision}")
  @get readRevision(@path bucket: string, @path key: string, @path revision: int64, ...TenantHeader): KvEntry | ErrorResponse;
  
  @route("/{key}/history")
  @get listHistory(@path bucket: string, @path key: string, @query limit?: int32 = 10, ...TenantHeader): KvEntry[];
  
  @put put(
    @path bucket: string, 
    @path key: string, 
    @query expectedRevision?: int64, 
    @body request: KvPutRequest, 
    ...TenantHeader
  ): KvEntry | ErrorResponse;
  
  @delete delete(@path bucket: string, @path key: string, ...TenantHeader): KvEntry | ErrorResponse;
  
  @route("/{key}/purge")
  @delete purge(@path bucket: string, @path key: string, ...TenantHeader): { count: int64 } | ErrorResponse;
}

@tag("Object Buckets")
@route("/api/v1/objects/buckets")
interface ObjBuckets {
  @post create(@body request: ObjBucketCreateRequest, ...TenantHeader): ObjBucket | ErrorResponse;
  @get list(...TenantHeader): ObjBucket[];
  @get read(@path name: string, ...TenantHeader): ObjBucket | ErrorResponse;
  @put update(@path name: string, @body request: ObjBucketCreateRequest, ...TenantHeader): ObjBucket | ErrorResponse;
  @delete delete(@path name: string, ...TenantHeader): void | ErrorResponse;
}

@tag("Objects")
@route("/api/v1/objects/buckets/{bucket}/objects")
interface Objects {
  @get list(@path bucket: string, ...TenantHeader): ObjectInfo[];
  
  @route("/{name}/metadata")
  @get readMetadata(@path bucket: string, @path name: string, ...TenantHeader): ObjMetadata | ErrorResponse;
  
  @get download(
    @path bucket: string, 
    @path name: string, 
    @header Range?: string, 
    ...TenantHeader
  ): bytes | ErrorResponse;
  
  @put upload(
    @path bucket: string, 
    @path name: string, 
    @header "Content-Type"?: string,
    @header "X-Object-Description"?: string,
    @body data: bytes,
    ...TenantHeader
  ): ObjMetadata | ErrorResponse;
  
  @delete delete(@path bucket: string, @path name: string, ...TenantHeader): void | ErrorResponse;
  
  @route("/{name}/verify")
  @get verify(@path bucket: string, @path name: string, ...TenantHeader): IntegrityResult | ErrorResponse;
}
