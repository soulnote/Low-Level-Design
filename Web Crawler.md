# Web Crawler


### 1. Requirement Clarification

**Functional Requirements**
- Seed URL ingestion with recursive link extraction
- Configurable crawl depth & max pages per domain
- Duplicate URL detection & canonical normalization
- `robots.txt` compliance (respect `Disallow`, `Crawl-delay`, `User-agent`)
- Extracted content storage (HTML, title, metadata, links)
- Pause/Resume & status reporting

**Non-Functional Requirements**
- **High Throughput**: Millions of pages/day via parallel workers
- **Politeness**: Strict per-domain rate limiting to avoid server overload
- **Fault Tolerance**: Retry with exponential backoff, circuit breakers, dead-letter handling
- **Scalability**: Horizontally partitionable frontier, distributed worker coordination
- **Low Memory Footprint**: Streaming parsing, externalized state for frontier/visited sets

**Assumptions**
- Single base currency/region for LLD; multi-region via message brokers in production
- Max page size: 10MB (abort larger)
- Default `robots.txt` refresh TTL: 24h
- Storage backend: Distributed KV/Columnar DB (e.g., Cassandra/S3)
- URL normalization strips fragments, lowercases host, sorts query params, removes tracking parameters

---

### 2. Core Entities & Responsibilities

| Entity | Responsibility |
|--------|----------------|
| `WebCrawlerSystem` | Singleton orchestrator. Coordinates lifecycle, worker pools, and configuration. |
| `CrawlTask` | Immutable work unit: normalized URL, depth, priority, retry count, domain. |
| `PageData` | Extracted payload: raw/processed content, metadata, discovered links, status. |
| `URLFrontier` | Distributed queue of pending `CrawlTask`s. Handles priority & domain-aware scheduling. |
| `URLFetcher` | HTTP client wrapper. Manages timeouts, retries, redirects, and response validation. |
| `Parser` | Extracts links, metadata, and content type. Strategy-based for HTML/XML/PDF. |
| `RobotsTxtCache` | Fetches, parses, and caches per-domain crawl rules with TTL. |
| `StorageService` | Persists `PageData` & visited state to durable backend. |
| `CrawlScheduler` | Determines execution order based on strategy (BFS, DFS, Priority/Score). |

---

### 3. Class Design (Java Implementation Required)

```java
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

// ================= ENUMS & DOMAINS =================
enum CrawlStatus { PENDING, FETCHING, PARSED, STORED, FAILED, SKIPPED }
enum SchedulingType { BFS, DFS, PRIORITY }

class CrawlTask {
    private final URI url;
    private final int depth;
    private final int priority;
    private final int retryCount;
    private final String domain;
    private final Instant scheduledAt;

    public CrawlTask(URI url, int depth, int priority, int retryCount, Instant scheduledAt) {
        this.url = url;
        this.depth = depth;
        this.priority = priority;
        this.retryCount = retryCount;
        this.domain = url.getHost().toLowerCase();
        this.scheduledAt = scheduledAt;
    }
    public URI getUrl() { return url; }
    public String getDomain() { return domain; }
    public int getDepth() { return depth; }
    public int getPriority() { return priority; }
    public int getRetryCount() { return retryCount; }
    public Instant getScheduledAt() { return scheduledAt; }
}

class PageData {
    private final URI sourceUrl;
    private final String title;
    private final String rawContent;
    private final Map<String, String> metadata;
    private final List<URI> discoveredLinks;
    private final Instant crawledAt;
    private final CrawlStatus status;

    public PageData(URI url, String title, String content, List<URI> links, CrawlStatus status) {
        this.sourceUrl = url; this.title = title; this.rawContent = content;
        this.metadata = new HashMap<>(); this.discoveredLinks = links;
        this.crawledAt = Instant.now(); this.status = status;
    }
    public URI getSourceUrl() { return sourceUrl; }
    public String getTitle() { return title; }
    public List<URI> getDiscoveredLinks() { return Collections.unmodifiableList(discoveredLinks); }
}

// ================= STRATEGY INTERFACES =================
interface SchedulingStrategy {
    void enqueue(CrawlTask task, BlockingQueue<CrawlTask> queue);
}

class BFSStrategy implements SchedulingStrategy {
    public void enqueue(CrawlTask task, BlockingQueue<CrawlTask> queue) {
        queue.offer(task); // FIFO ordering naturally implements BFS
    }
}

class PriorityStrategy implements SchedulingStrategy {
    private final PriorityBlockingQueue<CrawlTask> priorityQueue;
    public PriorityStrategy(PriorityBlockingQueue<CrawlTask> q) { this.priorityQueue = q; }
    public void enqueue(CrawlTask task, BlockingQueue<CrawlTask> queue) {
        priorityQueue.offer(task);
    }
}

interface ParseStrategy {
    PageData parse(URI url, String html);
}

class HtmlParserStrategy implements ParseStrategy {
    public PageData parse(URI url, String html) {
        // Simplified DOM parsing mock
        String title = "Mock Title";
        List<URI> links = Arrays.asList(URI.create("https://example.com/page1"), URI.create("https://example.com/page2"));
        return new PageData(url, title, html, links, CrawlStatus.PARSED);
    }
}

// ================= FACTORY PATTERN =================
class ParserFactory {
    public static ParseStrategy create(String contentType) {
        if (contentType != null && contentType.contains("html")) {
            return new HtmlParserStrategy();
        }
        throw new UnsupportedOperationException("Unsupported content type: " + contentType);
    }
}

// ================= OBSERVER PATTERN =================
interface CrawlEventListener {
    void onNewUrlsDiscovered(URI source, List<URI> urls);
    void onTaskCompleted(CrawlTask task, PageData page);
    void onTaskFailed(CrawlTask task, Exception ex);
}

// ================= POLITE RATE LIMITER =================
class DomainRateLimiter {
    private final Map<String, Semaphore> domainPermits = new ConcurrentHashMap<>();
    public void allowAccess(String domain) throws InterruptedException {
        Semaphore sem = domainPermits.computeIfAbsent(domain, d -> new Semaphore(1, true));
        sem.acquire();
        // Simulate polite delay (e.g., 1 sec per domain)
        Thread.sleep(1000); 
        sem.release();
    }
}

// ================= MANAGER (SINGLETON) =================
class FrontierManager {
    private static volatile FrontierManager instance;
    private final BlockingQueue<CrawlTask> queue = new LinkedBlockingQueue<>();
    private final Set<String> visitedUrls = ConcurrentHashMap.newKeySet(); // Prod: Bloom Filter + Redis
    private final DomainRateLimiter rateLimiter = new DomainRateLimiter();
    private final SchedulingStrategy strategy = new BFSStrategy();
    private final List<CrawlEventListener> listeners = new CopyOnWriteArrayList<>();

    private FrontierManager() {}
    public static FrontierManager getInstance() {
        if (instance == null) { synchronized (FrontierManager.class) { if (instance == null) instance = new FrontierManager(); } }
        return instance;
    }

    public void registerListener(CrawlEventListener l) { listeners.add(l); }
    public void submitTask(CrawlTask task) {
        if (!visitedUrls.add(task.getUrl().toString())) return;
        strategy.enqueue(task, queue);
    }

    public boolean isVisited(URI url) { return visitedUrls.contains(url.toString()); }
    public CrawlTask nextTask() { return queue.poll(); }
    public void allowDomain(String domain) throws InterruptedException { rateLimiter.allowAccess(domain); }
    public void notifyNewUrls(URI src, List<URI> urls) { listeners.forEach(l -> l.onNewUrlsDiscovered(src, urls)); }
    public void notifyCompleted(CrawlTask t, PageData p) { listeners.forEach(l -> l.onTaskCompleted(t, p)); }
    public void notifyFailed(CrawlTask t, Exception e) { listeners.forEach(l -> l.onTaskFailed(t, e)); }
    public int pendingCount() { return queue.size(); }
}

// ================= SERVICES =================
interface FetchService {
    String fetch(URI url, Duration timeout) throws Exception;
}

class HttpFetchService implements FetchService {
    public String fetch(URI url, Duration timeout) {
        // Mock HTTP client with timeout/redirect handling
        return "<html><title>Mock</title><a href='/page1'>1</a><a href='/page2'>2</a></html>";
    }
}

class StorageService {
    public void persist(PageData page) {
        // Write to KV/Document DB
    }
}

// ================= PRODUCER-CONSUMER WORKER =================
class CrawlWorker implements Runnable {
    private final FetchService fetcher = new HttpFetchService();
    private final StorageService storage = new StorageService();
    private final int maxDepth;
    private final Duration timeout = Duration.ofSeconds(5);
    private final int maxRetries = 3;

    public CrawlWorker(int maxDepth) { this.maxDepth = maxDepth; }

    @Override
    public void run() {
        FrontierManager fm = FrontierManager.getInstance();
        try {
            while (!Thread.currentThread().isInterrupted()) {
                CrawlTask task = fm.nextTask();
                if (task == null) { Thread.sleep(500); continue; }

                try {
                    fm.allowDomain(task.getDomain());
                    String html = fetcher.fetch(task.getUrl(), timeout);
                    ParseStrategy parser = ParserFactory.create("text/html");
                    PageData page = parser.parse(task.getUrl(), html);

                    storage.persist(page);
                    fm.notifyCompleted(task, page);

                    // Producer Phase: Enqueue discovered links
                    if (task.getDepth() < maxDepth) {
                        List<URI> validLinks = page.getDiscoveredLinks().stream()
                                .filter(u -> !fm.isVisited(u))
                                .collect(Collectors.toList());
                        
                        fm.notifyNewUrls(task.getUrl(), validLinks);
                        for (URI link : validLinks) {
                            fm.submitTask(new CrawlTask(link, task.getDepth() + 1, 1, 0, Instant.now()));
                        }
                    }
                } catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
                catch (Exception e) {
                    handleRetry(task, e);
                }
            }
        } catch (Exception ex) { ex.printStackTrace(); }
    }

    private void handleRetry(CrawlTask task, Exception ex) {
        FrontierManager fm = FrontierManager.getInstance();
        if (task.getRetryCount() < maxRetries) {
            long backoff = (long) Math.pow(2, task.getRetryCount()) * 1000;
            try { Thread.sleep(backoff); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); return; }
            fm.submitTask(new CrawlTask(task.getUrl(), task.getDepth(), task.getPriority(), task.getRetryCount() + 1, Instant.now()));
        } else {
            fm.notifyFailed(task, ex);
        }
    }
}

// ================= ORCHESTRATOR / SINGLETON FACADE =================
class WebCrawlerSystem {
    private static volatile WebCrawlerSystem instance;
    private ExecutorService workerPool;
    private volatile boolean running;
    private final int maxWorkers = 10;
    private final int maxDepth;

    private WebCrawlerSystem(int maxDepth) {
        this.maxDepth = maxDepth;
        this.running = false;
        this.workerPool = Executors.newFixedThreadPool(maxWorkers);
    }

    public static WebCrawlerSystem createInstance(int maxDepth) {
        if (instance == null) { synchronized (WebCrawlerSystem.class) { if (instance == null) instance = new WebCrawlerSystem(maxDepth); } }
        return instance;
    }

    public synchronized void startCrawl(List<URI> seedUrls) {
        if (running) return;
        running = true;
        FrontierManager fm = FrontierManager.getInstance();
        for (URI seed : seedUrls) {
            fm.submitTask(new CrawlTask(seed, 0, 1, 0, Instant.now()));
        }
        for (int i = 0; i < maxWorkers; i++) {
            workerPool.submit(new CrawlWorker(maxDepth));
        }
    }

    public synchronized void stopCrawl() {
        running = false;
        workerPool.shutdownNow();
    }

    public String getStatus() {
        FrontierManager fm = FrontierManager.getInstance();
        return String.format("Running: %s | Pending: %d | Visited: %d", running, fm.pendingCount(), 0);
    }
}
```

---

### 4. Relationships & Class Diagram (Textual UML)

```
[WebCrawlerSystem] 1 <1..> * [CrawlWorker]            (Composition: Lifecycle management)
[WebCrawlerSystem] 1 --> 1 [FrontierManager]          (Dependency/Singleton)

[FrontierManager] 1 *--> * [CrawlTask]                (Aggregation: Queue holds tasks)
[FrontierManager] 1 --> 1 [SchedulingStrategy]        (Composition/Strategy Injection)
[FrontierManager] 1 --> 1 [DomainRateLimiter]         (Aggregation: Politeness control)

[CrawlTask] 1 --> 1 [URI]                             (Association: Normalized URL reference)
[CrawlTask] 1 o--> 1 [PageData]                       (Aggregation: Output of crawl)

[PageData] 1 *--> * [URI]                             (Composition: Discovered links owned by page)
[ParserFactory] ..> [ParseStrategy]                   (Factory Creation)
[ParseStrategy] <|-- [HtmlParserStrategy]             (Inheritance)

[FrontierManager] 1 o--> * [CrawlEventListener]       (Aggregation: Observer Pattern)
[CrawlWorker] ..> [FetchService], [StorageService]    (Dependency Injection/Interface Segregation)
```

---

### 5. Design Patterns Used

| Pattern | Implementation in Code |
|---------|------------------------|
| **Singleton** | `WebCrawlerSystem` & `FrontierManager` use double-checked locking. Guarantees single frontier registry and crawler lifecycle control across threads. |
| **Factory** | `ParserFactory.create(contentType)` returns appropriate `ParseStrategy`. Decouples worker from HTML/XML/PDF parsing logic. Easily extensible. |
| **Strategy** | `SchedulingStrategy` interface with `BFSStrategy`/`PriorityStrategy`. `FrontierManager` injects strategy, allowing runtime switching without modifying queue logic. |
| **Producer-Consumer** | `CrawlWorker` acts as consumer (polls queue, fetches) and producer (parses links, enqueues new `CrawlTask`s). `BlockingQueue` decouples stages, enables backpressure. |
| **Observer** | `CrawlEventListener` interface + `CopyOnWriteArrayList`. `FrontierManager` fires events on discovery/completion/failure. Decouples indexing, analytics, and alerting from core crawl loop. |

---

### 6. Core Flow (Very Important)

#### Crawl Initialization Flow
1. **Configuration**: `WebCrawlerSystem.createInstance(maxDepth)` boots worker pool.
2. **Seed Ingestion**: `startCrawl(seeds)` normalizes URLs, wraps in `CrawlTask(depth=0)`, pushes to `FrontierManager`.
3. **Dedup Check**: `visitedUrls.add()` filters duplicates atomically.
4. **Worker Boot**: `ExecutorService` launches N `CrawlWorker` threads.

#### Crawl Execution Flow
1. **Poll**: Worker calls `fm.nextTask()`. Blocks/returns null if queue empty.
2. **Politeness**: `fm.allowDomain(task.domain)` acquires domain semaphore, enforces delay.
3. **Fetch**: `HttpFetchService.fetch()` retrieves HTML with timeout/redirect handling.
4. **Parse**: `ParserFactory` selects `HtmlParserStrategy`. Extracts title, metadata, and absolute URIs.
5. **Store**: `StorageService.persist(page)` writes to durable backend.
6. **Produce**: Discovered links filtered by depth & visited set. New tasks enqueued.
7. **Notify**: Observer broadcasts completion/discovery for downstream consumers.

#### Duplicate Detection Flow
1. **Normalization**: URL canonicalization (lowercase host, strip fragments, sort query params, remove UTM/tracking params).
2. **Atomic Check**: `ConcurrentHashMap.newKeySet().add()` returns false if already present.
3. **Production Replacement**: In-memory `Set` swapped for **Bloom Filter** (probabilistic, low memory) + **Redis SET/DB** (deterministic fallback).

#### Politeness & Rate Limiting Flow
1. **Domain Extraction**: `task.getDomain()` parsed from URI.
2. **Token Bucket / Semaphore**: `DomainRateLimiter` maintains per-domain concurrency limit (e.g., 1 concurrent fetch/sec).
3. **robots.txt**: Cached rules parsed on first domain hit. `Crawl-delay` overrides default sleep. `Disallow` paths filtered pre-fetch.

#### Failure Handling Flow
1. **Catch Exception**: Network timeout, 5xx, malformed HTML.
2. **Retry Logic**: `handleRetry()` checks `retryCount < maxRetries`. Applies exponential backoff `2^attempt * 1s`.
3. **Re-enqueue**: Task submitted with incremented retry count. Priority unchanged.
4. **Dead Letter**: After max retries, `notifyFailed()` triggers DLQ routing & alerting.

---

### 7. API Design (Conceptual Only)

| Operation | Method | Request Payload | Response | Status Codes |
|-----------|--------|-----------------|----------|--------------|
| `startCrawl` | `POST /crawler/start` | `{ seedUrls: ["https://example.com"], maxDepth: 3, maxPagesPerDomain: 5000, strategy: "BFS" }` | `{ crawlId: "c_8x2", status: "RUNNING", queuedTasks: 1 }` | 202 Accepted, 409 Already Running |
| `addSeedUrl` | `POST /crawler/seed` | `{ urls: ["https://target.io/docs"], priority: 10 }` | `{ addedCount: 1, queueSize: 5400 }` | 201 Created, 400 Invalid URL |
| `stopCrawl` | `POST /crawler/stop` | N/A | `{ crawlId: "c_8x", status: "STOPPED", pagesProcessed: 4210 }` | 200 OK, 404 Not Running |
| `getCrawlStatus` | `GET /crawler/status` | Query: `?metrics=throughput,errors` | `{ status: "RUNNING", visited: 4210, pending: 120, avgLatency: 450ms, errorRate: 0.02 }` | 200 OK, 404 Invalid ID |

---

### 8. Database / Storage Design

**Schema / Data Structures**
- `crawled_pages` (`url_hash` VARCHAR PK, `original_url` TEXT, `content` TEXT/CLOB, `title` VARCHAR, `content_type` VARCHAR, `crawled_at` TIMESTAMP, `status` ENUM, `domain` VARCHAR)
- `visited_urls` (`url_hash` VARCHAR PK, `url` TEXT, `first_seen` TIMESTAMP, `last_visited` TIMESTAMP, `retry_count` INT, `status` ENUM)
- `crawl_tasks` (External Queue: Kafka/Redis) -> `task_id`, `url_hash`, `depth`, `priority`, `scheduled_ts`, `retry_count`, `state`
- `robots_cache` (`domain` VARCHAR PK, `rules_json` JSONB, `fetched_at` TIMESTAMP, `ttl` INT)

**Indexing Strategy**
- `crawled_pages(domain, crawled_at DESC)` → Fast domain-scoped retrieval & pagination
- `visited_urls(url_hash)` → Hash index for O(1) dedup lookup
- `robots_cache(domain)` → Exact match cache lookup
- **Time-Series Partitioning**: `crawled_pages` partitioned monthly by `crawled_at` for archival & TTL cleanup
- **Bloom Filter Alternative**: In-memory `Set` → Redis `SETNX` + probabilistic filter for 99.9% fast rejects

---

### 9. Concurrency & Scalability

- **Multi-Threaded Workers**: `ExecutorService` with fixed pool. Each worker independently polls, fetches, parses. Bounded by thread count & I/O capacity.
- **Producer-Consumer Decoupling**: `BlockingQueue` absorbs burst link discovery. Workers act as consumers. Parse step acts as producer for new URLs. Backpressure via queue capacity limits.
- **Distributed Crawling**: 
  - **Frontier Partitioning**: Kafka topics partitioned by `domain hash`. Ensures all URLs for a domain hit the same consumer group → guarantees politeness without distributed locks.
  - **Consistent Hashing**: Workers register to partitions. Master node reassigns on node failure.
  - **Stateless Workers**: Only depend on queue & storage. Horizontally scalable.
- **Load Balancing**: Dynamic pool sizing based on queue depth & CPU/I/O utilization metrics. Circuit breakers per domain prevent worker starvation on slow hosts.

---

### 10. Edge Cases

| Scenario | Handling |
|----------|----------|
| **Infinite Loops (Cyclic Links)** | `maxDepth` caps recursion. `visitedUrls` Bloom Filter prevents re-fetching identical URLs regardless of path structure. |
| **Duplicate URLs with Query Params** | Canonical normalization strips `utm_*`, `fbclid`, sorts remaining params alphabetically. `url_hash` computed post-normalization. |
| **Slow/Unresponsive Servers** | Timeout (5s) enforced. Circuit breaker opens after 50% failure rate. Worker skips domain temporarily, retries later with backoff. |
| **Large Pages / Timeouts** | Stream response with max size limit (10MB). Abort if exceeded. Partial parse rejected. Logs truncated size. |
| **robots.txt Restrictions** | Fetched & cached on first domain access. `Disallow` paths filtered before fetch. `Crawl-delay` overrides semaphore sleep. TTL 24h. |

---

### 11. Sample Queries

**Fetch crawled pages for a domain (last 7 days):**
```sql
SELECT url_hash, title, crawled_at, length(content) as size_bytes
FROM crawled_pages
WHERE domain = 'example.com' 
  AND status = 'STORED'
  AND crawled_at >= NOW() - INTERVAL '7 days'
ORDER BY crawled_at DESC
LIMIT 100;
```

**Get crawl statistics (aggregated):**
```sql
SELECT 
  COUNT(*) as total_pages,
  COUNT(CASE WHEN status = 'STORED' THEN 1 END) as successful,
  COUNT(CASE WHEN status = 'FAILED' THEN 1 END) as failed,
  AVG(EXTRACT(EPOCH FROM (crawled_at - first_seen))) as avg_processing_time_sec
FROM visited_urls v
JOIN crawled_pages c ON v.url_hash = c.url_hash;
```

**Retrieve stored page data:**
```sql
SELECT title, content_type, crawled_at, substring(content, 1, 500) as preview
FROM crawled_pages
WHERE url_hash = SHA256('https://example.com/page1');
```

---

### 12. Trade-offs & Extensions

**Limitations of Current Design**
- **In-Memory Frontier & Visited Set**: `ConcurrentHashMap` & `Set` limit to single-node memory. Production requires distributed KV (Redis/Cassandra) + Bloom Filters.
- **Blocking I/O per Worker**: Thread-per-fetch scales to thousands, not millions. Async I/O (Netty/Apache HttpAsyncClient) required for extreme throughput.
- **Simple Rate Limiter**: `Semaphore` + `sleep` lacks token-bucket precision & burst handling.

**Improvements & Extensions**
1. **Distributed Crawler Architecture**: Master-Worker model. Master manages partitioned Kafka frontier, health checks, and dynamic scaling. Workers stateless, consume partitions.
2. **Priority-Based Crawling**: Inject `ScoreStrategy` (e.g., PageRank, domain authority, freshness decay). `PriorityStrategy` sorts queue by score × recency.
3. **Content Indexing Pipeline**: Replace direct `StorageService` with event stream to Elasticsearch/OpenSearch. Parse → Extract Entities → Embed → Index → Serve Search.
4. **URL Normalization & Dedup Optimization**: Use **MinHash/SimHash** for near-duplicate detection. Handle parameterized URLs (`/article?id=123` vs `/article?id=123&utm_source=tw`).
5. **Adaptive Politeness**: ML model predicts optimal crawl rate per domain based on historical response times & error rates. Dynamically adjusts semaphore permits.
6. **Resumable & Idempotent Crawling**: Task states persisted. Workers track `last_processed_offset`. Crash recovery replays from checkpoint without re-fetching.
