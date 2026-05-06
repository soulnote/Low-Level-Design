# URL Shortening System


### 1. Requirement Clarification

* **Functional Requirements**
  * Generate a unique short URL from a long URL
  * HTTP redirect (301/302) from short code to original URL
  * Custom alias support (user-defined short code)
  * Configurable expiration (TTL or absolute timestamp)
  * Analytics: click count, unique visitors, geographic/device metadata
* **Non-Functional Requirements**
  * Read-heavy workload (100:1 read:write ratio)
  * Redirect latency < 30ms P99
  * 99.99% availability, graceful degradation on DB failure
  * Scale: billions of URLs, millions of redirects/sec
  * Eventual consistency for analytics; strong consistency for mappings
* **Assumptions**
  * Long URL max length: 2048 chars, validated via RFC 3986
  * Short code length: 7 chars (Base62 → ~3.5 trillion combinations)
  * Collision handling: DB unique constraint + exponential backoff retry
  * Analytics processed asynchronously via message queue

---

### 2. Core Entities & Responsibilities

| Entity | Responsibility |
|--------|----------------|
| `URLShortenerSystem` | Root orchestrator; bootstraps managers/services, ensures single initialization point |
| `URLMapping` | Immutable domain object holding long URL, short code, alias, expiration, status, timestamps |
| `AnalyticsData` | Click counter, last accessed time, metadata snapshot; lifecycle bound to mapping |
| `ShortCodeGenerator` | Abstract strategy interface for generating collision-resistant codes |
| `RedirectService` | Handles lookup, expiration validation, cache/DB routing, and response preparation |
| `AnalyticsService` | Ingests click events, aggregates stats, provides query interface for dashboards |
| `ExpirationManager` | Evaluates TTL on read, triggers async cleanup jobs, handles 410 responses |
| `KeyGenerationManager` | Singleton coordinator for distributed ID reservation, collision retries, and strategy routing |

---

### 3. Class Design (Java Implementation)

```java
// ==================== DOMAIN MODELS ====================

public final class URLMapping {
    private final String shortCode;
    private final String longUrl;
    private final String customAlias;
    private final long createdAtMs;
    private final Long expiresAtMs;
    private boolean isActive;
    private final AnalyticsData analytics; // Composition

    private URLMapping(String shortCode, String longUrl, String customAlias, Long expiresAtMs) {
        this.shortCode = Objects.requireNonNull(shortCode);
        this.longUrl = Objects.requireNonNull(longUrl);
        this.customAlias = customAlias;
        this.createdAtMs = System.currentTimeMillis();
        this.expiresAtMs = expiresAtMs;
        this.isActive = true;
        this.analytics = new AnalyticsData(shortCode);
    }

    public String getShortCode() { return shortCode; }
    public String getLongUrl() { return longUrl; }
    public String getCustomAlias() { return customAlias; }
    public Long getExpiresAtMs() { return expiresAtMs; }
    public boolean isActive() { return isActive; }
    public boolean isExpired() { return expiresAtMs != null && System.currentTimeMillis() > expiresAtMs; }
    public AnalyticsData getAnalytics() { return analytics; }

    public void deactivate() { this.isActive = false; }

    // Static Factory for clean creation
    public static URLMapping create(String longUrl, String shortCode, String alias, Long ttlMs) {
        long expiresAt = ttlMs != null ? System.currentTimeMillis() + ttlMs : null;
        return new URLMapping(shortCode, longUrl, alias, expiresAt);
    }
}

public final class AnalyticsData {
    private final String shortCode;
    private volatile long clickCount;
    private volatile long lastClickTimestamp;
    private volatile int uniqueVisitorCount; // Simplified

    AnalyticsData(String shortCode) {
        this.shortCode = shortCode;
        this.clickCount = 0;
        this.lastClickTimestamp = 0;
        this.uniqueVisitorCount = 0;
    }

    public synchronized void recordClick() {
        this.clickCount++;
        this.lastClickTimestamp = System.currentTimeMillis();
    }

    public String getShortCode() { return shortCode; }
    public long getClickCount() { return clickCount; }
    public long getLastClickTimestamp() { return lastClickTimestamp; }
}

// ==================== STRATEGY & FACTORY ====================

public interface ShortCodeGenerationStrategy {
    String generate();
}

public class Base62HashStrategy implements ShortCodeGenerationStrategy {
    private final MessageDigest sha256;
    public Base62HashStrategy() {
        try { sha256 = MessageDigest.getInstance("SHA-256"); } catch (NoSuchAlgorithmException e) { throw new RuntimeException(e); }
    }
    @Override public String generate() {
        byte[] hash = sha256.digest(UUID.randomUUID().toString().getBytes());
        return Base62.encode(hash).substring(0, 7);
    }
}

public class DistributedCounterStrategy implements ShortCodeGenerationStrategy {
    private final AtomicLong counter;
    public DistributedCounterStrategy(long seed) { this.counter = new AtomicLong(seed); }
    @Override public String generate() {
        return Base62.encode(counter.getAndIncrement());
    }
}

public class StrategyFactory {
    public static ShortCodeGenerationStrategy createStrategy(String type) {
        return switch (type) {
            case "HASH" -> new Base62HashStrategy();
            case "COUNTER" -> new DistributedCounterStrategy(System.currentTimeMillis());
            default -> throw new IllegalArgumentException("Unknown strategy: " + type);
        };
    }
}

// ==================== SERVICES & MANAGERS ====================

// Interfaces (Dependency Inversion Principle)
public interface URLMappingRepository { void save(URLMapping mapping); URLMapping findByShortCode(String code); }
public interface AnalyticsRepository { void batchSave(List<AnalyticsData> batch); }
public interface CacheProvider { Optional<URLMapping> get(String key); void put(String key, URLMapping val, Duration ttl); }

public class KeyGenerationManager {
    // Singleton Pattern (Thread-Safe Initialization)
    private static volatile KeyGenerationManager instance;
    private final ShortCodeGenerationStrategy strategy;

    private KeyGenerationManager(String strategyType) {
        this.strategy = StrategyFactory.createStrategy(strategyType);
    }

    public static synchronized KeyGenerationManager getInstance(String strategyType) {
        if (instance == null) { instance = new KeyGenerationManager(strategyType); }
        return instance;
    }

    public String generateShortCode() {
        return strategy.generate();
    }
}

public class CacheManager implements CacheProvider {
    private final Map<String, URLMapping> cache = new ConcurrentHashMap<>();
    private final Duration defaultTtl;

    public CacheManager(Duration ttl) { this.defaultTtl = ttl; }

    @Override public Optional<URLMapping> get(String key) { return Optional.ofNullable(cache.get(key)); }
    @Override public void put(String key, URLMapping val, Duration ttl) { 
        cache.put(key, val); 
        // In production: delegate to Redis with TTL. Here: conceptual eviction would run async.
    }
}

// Proxy Pattern: CachingRedirectService proxies DB lookup
public interface RedirectService { Optional<URLMapping> resolve(String code); }

public class DatabaseRedirectService implements RedirectService {
    private final URLMappingRepository repo;
    public DatabaseRedirectService(URLMappingRepository repo) { this.repo = repo; }
    @Override public Optional<URLMapping> resolve(String code) { return Optional.ofNullable(repo.findByShortCode(code)); }
}

public class CachingRedirectService implements RedirectService {
    // Proxy Pattern Implementation
    private final CacheProvider cache;
    private final RedirectService delegate;
    private final ExpirationManager expirationManager;

    public CachingRedirectService(CacheProvider cache, RedirectService delegate, ExpirationManager expManager) {
        this.cache = cache;
        this.delegate = delegate;
        this.expirationManager = expManager;
    }

    @Override public Optional<URLMapping> resolve(String code) {
        Optional<URLMapping> cached = cache.get(code);
        if (cached.isPresent() && !expirationManager.isExpired(cached.get())) return cached;

        return delegate.resolve(code).map(mapping -> {
            cache.put(code, mapping, Duration.ofHours(24));
            return mapping;
        });
    }
}

public class ExpirationManager {
    public boolean isExpired(URLMapping mapping) { return mapping.isExpired(); }
    public void scheduleCleanup(String shortCode) { /* MQ/Kafka job trigger */ }
}

public class URLService {
    private final URLMappingRepository repo;
    private final CacheProvider cache;
    private final KeyGenerationManager keyGen;
    private final URLValidator validator;

    public URLService(URLMappingRepository repo, CacheProvider cache, KeyGenerationManager keyGen, URLValidator validator) {
        this.repo = repo; this.cache = cache; this.keyGen = keyGen; this.validator = validator;
    }

    public URLMapping createShortUrl(String longUrl, String customAlias, Long ttlMs) {
        validator.validate(longUrl);
        String code = (customAlias != null && !customAlias.isBlank()) ? customAlias : keyGen.generateShortCode();
        URLMapping mapping = URLMapping.create(longUrl, code, customAlias, ttlMs);
        
        try {
            repo.save(mapping);
            cache.put(code, mapping, Duration.ofHours(2));
            return mapping;
        } catch (DuplicateKeyException e) {
            throw new IllegalArgumentException("Short code already exists");
        }
    }
}

public class AnalyticsService {
    private final AnalyticsRepository repo;
    private final BlockingQueue<AnalyticsData> asyncQueue = new LinkedBlockingQueue<>();

    public AnalyticsService(AnalyticsRepository repo) { this.repo = repo; }

    public void recordClick(AnalyticsData data) {
        data.recordClick();
        asyncQueue.offer(data);
        // Worker thread drains queue -> batch inserts
    }

    public long getClickCount(String shortCode) {
        return asyncQueue.stream().filter(a -> a.getShortCode().equals(shortCode))
                .mapToLong(AnalyticsData::getClickCount).sum();
    }
}

// Utility for validation
class URLValidator {
    public void validate(String url) {
        if (url == null || url.length() > 2048 || !url.matches("^https?://.*")) {
            throw new IllegalArgumentException("Invalid URL format or length");
        }
    }
}
```

---

### 4. Relationships & Class Diagram (Textual UML)

```
[URLMapping] "1" *-- "1" AnalyticsData (Composition)
[URLMapping] "1" o-- "0..*" ExpirationManager (Dependency)
[URLMapping] "1" --> "1" URLMappingRepository (Association)

[RedirectService] <|.. [CachingRedirectService] (Realization/Proxy)
[CachingRedirectService] "1" o-- "1" CacheProvider (Composition)
[CachingRedirectService] "1" --> "1" DatabaseRedirectService (Delegation)

[ShortCodeGenerationStrategy] <|.. [Base62HashStrategy] (Generalization)
[ShortCodeGenerationStrategy] <|.. [DistributedCounterStrategy]
[StrategyFactory] --> [ShortCodeGenerationStrategy] (Creation)

[KeyGenerationManager] "1" --> "1" ShortCodeGenerationStrategy (Association)
[URLService] "1" --> "1" KeyGenerationManager (Dependency)
[URLService] "1" --> "1" CacheProvider (Association)

Multiplicity Notes:
1 *-- 1 = Composition (AnalyticsData lifecycle bound to URLMapping)
1 --> 1 = Association/Dependency
<|.. = Interface implementation / Proxy wrapping
```

---

### 5. Design Patterns Used

| Pattern | Implementation Context | Why Used |
|---------|------------------------|----------|
| **Singleton** | `KeyGenerationManager` | Guarantees single strategy initialization across JVM; thread-safe lazy init; prevents distributed counter skew within a node |
| **Factory** | `StrategyFactory` | Decouples strategy instantiation from business logic; enables runtime selection (`HASH` vs `COUNTER`) without modifying `KeyGenerationManager` |
| **Strategy** | `ShortCodeGenerationStrategy` interface + `Base62HashStrategy`/`DistributedCounterStrategy` | Encapsulates key generation algorithms; OCP-compliant; allows swapping encoding logic for compliance or performance needs |
| **Proxy** | `CachingRedirectService` wrapping `DatabaseRedirectService` | Adds caching layer transparently; controls access to DB; handles cache-aside logic & TTL without polluting core redirect logic |

---

### 6. Core Flow (Very Important)

#### URL Shortening Flow
1. Client submits `longUrl`, optional `customAlias`, `ttl` → `URLService.createShortUrl()`
2. `URLValidator` checks RFC compliance & length
3. If `customAlias` provided → DB unique constraint check
4. Else → `KeyGenerationManager.generateShortCode()` invokes active `Strategy`
5. `URLMapping` domain object constructed via static factory
6. `URLMappingRepository.save()` persists (async WAL if needed)
7. `CacheProvider.put()` warms cache with 2h TTL
8. Return short code to client

#### Redirect Flow
1. Request hits `/s/{code}` → `CachingRedirectService.resolve(code)`
2. Proxy checks `CacheProvider.get(code)` → if hit & not expired → return
3. Cache miss → `DatabaseRedirectService.resolve(code)` queries DB
4. If found → `cache.put()` for subsequent requests
5. `AnalyticsService.recordClick()` pushed to async queue
6. Return `302 Found` with `Location: mapping.getLongUrl()`

#### Custom Alias Flow
1. User submits `/create { alias: "mybrand" }`
2. `URLService` bypasses `KeyGenerationManager`, uses provided alias
3. DB `UNIQUE` constraint on `custom_alias` column prevents collisions
4. On conflict → `409 Conflict` returned; app-level retry suggested
5. On success → cache warmed, `201 Created` returned

#### Expiration Flow
1. `RedirectService` calls `ExpirationManager.isExpired(mapping)` on every resolution
2. If `System.currentTimeMillis() > expiresAtMs` → `mapping.deactivate()`
3. DB `UPDATE` marks `is_active = false`
4. Async job triggers `scheduleCleanup()` for storage reclamation
5. Redirect returns `410 Gone` or redirects to expiration landing page

---

### 7. API Design (Conceptual Only)

| Endpoint | Method | Request | Response | Status Codes |
|----------|--------|---------|----------|--------------|
| `/v1/shorten` | `POST` | `{ long_url: "https://...", custom_alias?: "abc", ttl_seconds?: 3600 }` | `{ short_code: "xYz1234", full_url: "https://sho.rt/xYz1234", expires_at: 1710000000 }` | `201 Created`, `400 Bad URL`, `409 Alias Taken`, `429 Rate Limit` |
| `/{shortCode}` | `GET` | Path param only | HTTP `302 Redirect` to original URL | `302 Found`, `404 Not Found`, `410 Gone (Expired)` |
| `/v1/analytics/{shortCode}` | `GET` | `{ metric?: "clicks", range?: "7d" }` | `{ clicks: 1420, unique: 890, last_accessed: 1709990000 }` | `200 OK`, `404 Not Found` |

*Behavioral Notes*: All creation requests require authentication. Redirect endpoint is public & unauthenticated. Analytics uses sliding window aggregation. Idempotency key optional for shortening to prevent duplicate mappings under network retries.

---

### 8. Database Schema Design

**`url_mappings`**
| Column | Type | Constraint |
|--------|------|------------|
| short_code | VARCHAR(8) | PK, NOT NULL |
| long_url | VARCHAR(2048) | NOT NULL |
| custom_alias | VARCHAR(50) | UNIQUE, INDEXED |
| owner_id | BIGINT | FK -> users, INDEXED |
| created_at | TIMESTAMP | DEFAULT NOW() |
| expires_at | TIMESTAMP | NULLABLE, INDEXED |
| is_active | BOOLEAN | DEFAULT TRUE |
| **Index** | `(short_code)` (PK), `(expires_at, is_active)` (for TTL jobs), `(owner_id)` |

**`analytics_daily_agg`** (Partitioned by date for scale)
| Column | Type | Constraint |
|--------|------|------------|
| short_code | VARCHAR(8) | PK, FK -> url_mappings |
| date | DATE | PK, Partition Key |
| total_clicks | BIGINT | DEFAULT 0 |
| unique_visitors | INT | DEFAULT 0 |
| last_updated | TIMESTAMP | |
| **Index** | `(short_code, date)` for dashboard queries |

*Note*: Raw click logs stream to Kafka → batch aggregated hourly to `analytics_daily_agg`. Keeps DB lean for fast redirects.

---

### 9. Concurrency & Scalability

* **High Concurrent Creation**: Distributed ID reservation via `Redis INCR` blocks (1000 IDs per node) or Snowflake generators. DB `INSERT ... ON CONFLICT` handles rare collisions atomically.
* **Avoid Key Collisions**: Base62 space (62^7 ≈ 3.5T) minimizes hash collisions. Counter strategy uses shard-local atomic increments. DB unique constraint is final arbiter.
* **Distributed ID Generation**: 64-bit Snowflake (timestamp + worker_id + sequence) → encoded to Base62 → guarantees monotonic, sortable, collision-free codes across cluster.
* **Caching**: Redis Cluster with `LRU` eviction. Cache-aside pattern with randomized TTL jitter (±15%) to prevent thundering herd on popular links.
* **Load Balancing**: L7 routing (HAProxy/Envoy) with sticky sessions for analytics ingestion workers. Read replicas for analytics queries; primary DB for mappings.

---

### 10. Edge Cases

| Case | Mitigation |
|------|------------|
| **Duplicate Long URLs** | Allow by design (users may want separate aliases/stats). Optional: `(owner_id, long_url)` unique index to prevent accidental dupes |
| **Key Collision** | Catch `ConstraintViolationException`, retry up to 3x with new code. Fallback to longer Base62 string if needed |
| **Expired Links** | Soft-delete (`is_active=false`). Background cleaner purges after 30d grace period. Redirect returns `410 Gone` immediately |
| **Invalid URLs** | Strict RFC 3986 parsing, reject private IPs, block known malicious domains via threat intel feed |
| **High Traffic Spikes** | Redis auto-scaling, circuit breaker on DB failover, serve stale cache with `X-Cache: stale` header, rate limit anonymous creators via sliding window |

---

### 11. Sample SQL Queries

```sql
-- 1. Fetch original URL by short code (highly indexed lookup)
SELECT long_url, expires_at, is_active 
FROM url_mappings 
WHERE short_code = :code 
LIMIT 1;

-- 2. Get total click count (reads aggregated table)
SELECT COALESCE(SUM(total_clicks), 0) as clicks 
FROM analytics_daily_agg 
WHERE short_code = :code;

-- 3. Find expired URLs for cleanup batch job
SELECT short_code, custom_alias 
FROM url_mappings 
WHERE expires_at < NOW() - INTERVAL '7 days' 
  AND is_active = true 
LIMIT 1000 FOR UPDATE SKIP LOCKED;
```

---

### 12. Trade-offs & Extensions

* **Limitations**
  * Cache-aside means first redirect per node pays DB latency
  * Aggregated analytics loses granular timestamp/user-agent data unless raw logs stored separately
  * Base62 encoding adds CPU overhead vs raw numeric IDs
* **Improvements**
  * **Distributed Architecture**: Split into independent microservices (`shortening-service`, `redirect-gateway`, `analytics-pipeline`). Deploy `redirect-gateway` as stateless edge nodes close to users via CDN/Anycast
  * **Geo-based Redirection**: Store regional endpoints per mapping; `RedirectService` resolves `X-Forwarded-For` → selects nearest origin
  * **Advanced Analytics**: Stream raw clicks to Kafka → Flink windowed aggregation → ClickHouse for sub-second dashboard queries. Add bot filtering via IP/User-Agent heuristics
  * **Rate Limiting**: Redis + Token Bucket per `owner_id`. Global anonymous limit enforced at edge proxy (WAF). Prevents abuse and DB thrashing
