# Twitter

### 1. Requirement Clarification

* **Functional Requirements**
  * Post tweets (text-only or with media attachments)
  * Follow/unfollow users (directed graph)
  * Engage: Like, Reply, Retweet
  * Home Timeline (personalized, merged feed of followees)
  * User Timeline (chronological posts by self)
  * Notifications (follows, likes, replies, retweets)
* **Non-Functional Requirements**
  * Scale: 100M+ DAU, 1M+ RPS peak, 10K TPS writes
  * Latency: <200ms P95 for feed generation, <50ms for writes
  * Availability: 99.99% read HA, graceful degradation on writes
  * Consistency: Eventual consistency for timelines & engagement counts; strong consistency for auth/user profile
* **Assumptions**
  * Tweet payload ≤ 4KB; Media handled via external CDN/S3 (URLs stored in DB)
  * Snowflake-like IDs for monotonic, time-sortable ordering
  * Fan-out Strategy: Hybrid (Push for ≤500 followers, Pull for >500, with caching layer)
  * Idempotency keys required for all mutation APIs

---

### 2. Core Entities & Responsibilities

| Entity | Responsibility |
|--------|----------------|
| `TwitterSystem` | Root coordinator; bootstraps services, manages lifecycle, ensures single entry point |
| `User` | Profile state, immutable identifiers, aggregation roots for relationships |
| `Tweet` | Immutable content payload, author ref, metadata, mutable engagement counters |
| `Timeline` (`HomeTimeline`, `UserTimeline`) | Ordered, paginated view of tweets; manages insertion, eviction, and merging logic |
| `FollowRelation` | Directed edge (follower → followee), tracks timestamps, bidirectional invalidation triggers |
| `Like / Retweet / Reply` | Engagement types extending base `Engagement`; track actor, target, timestamp, soft-deletes |
| `Notification` | Event carrier, delivery status, routing payload, expiry window |
| `FeedService` | Orchestrates timeline retrieval, applies generation strategy, coordinates caching |

---

### 3. Class Design (Java Implementation)

```java
// ==================== DOMAIN MODELS ====================

public class User {
    private final long userId;
    private final String username;
    private boolean active;

    public User(long userId, String username) {
        this.userId = userId;
        this.username = username;
        this.active = true;
    }

    public long getUserId() { return userId; }
    public String getUsername() { return username; }
    public boolean isActive() { return active; }
    public void suspend() { this.active = false; }
}

public class Tweet {
    private final String tweetId;
    private final long authorId;
    private final String content;
    private final TweetType type;
    private final String mediaUrl;
    private final long createdAtMs;
    private volatile int likeCount;
    private volatile int retweetCount;

    // Private constructor enforces Builder usage
    private Tweet(Builder builder) {
        this.tweetId = builder.tweetId;
        this.authorId = builder.authorId;
        this.content = builder.content;
        this.type = builder.type;
        this.mediaUrl = builder.mediaUrl;
        this.createdAtMs = builder.createdAtMs;
        this.likeCount = 0;
        this.retweetCount = 0;
    }

    // Getters & immutable accessors
    public String getTweetId() { return tweetId; }
    public long getAuthorId() { return authorId; }
    public String getContent() { return content; }
    public TweetType getType() { return type; }
    public long getCreatedAtMs() { return createdAtMs; }
    public int getLikeCount() { return likeCount; }
    public int getRetweetCount() { return retweetCount; }

    // Thread-safe counter updates
    public synchronized void incrementLikes() { likeCount++; }
    public synchronized void incrementRetweets() { retweetCount++; }

    public enum TweetType { TEXT, MEDIA, QUOTED }

    // Builder Pattern Implementation
    public static class Builder {
        private final String tweetId;
        private final long authorId;
        private String content;
        private TweetType type = TweetType.TEXT;
        private String mediaUrl = null;
        private long createdAtMs;

        public Builder(String tweetId, long authorId) {
            this.tweetId = tweetId;
            this.authorId = authorId;
        }
        public Builder content(String content) { this.content = content; return this; }
        public Builder type(TweetType type) { this.type = type; return this; }
        public Builder mediaUrl(String url) { this.mediaUrl = url; return this; }
        public Builder createdAtMs(long ms) { this.createdAtMs = ms; return this; }
        public Tweet build() { return new Tweet(this); }
    }
}

// ==================== TIMELINE & FEED GENERATION ====================

public abstract class Timeline {
    protected final List<Tweet> tweets = new ArrayList<>();
    protected final int maxCapacity = 1000;

    public synchronized void addTweet(Tweet tweet) {
        // Insertion sort for chronological order
        int i = 0;
        while (i < tweets.size() && tweets.get(i).getCreatedAtMs() > tweet.getCreatedAtMs()) i++;
        tweets.add(i, tweet);
        if (tweets.size() > maxCapacity) tweets.remove(tweets.size() - 1);
    }

    public List<Tweet> fetch(int limit, int offset) {
        int end = Math.min(offset + limit, tweets.size());
        return end <= offset ? Collections.emptyList() : tweets.subList(offset, end);
    }

    public abstract void clear();
    public abstract void mergeExternal(Timeline external);
}

public class UserTimeline extends Timeline {
    public UserTimeline() {}
    @Override public void clear() { tweets.clear(); }
    @Override public void mergeExternal(Timeline external) {} // Not applicable
}

public class HomeTimeline extends Timeline {
    private final MergeStrategy mergeStrategy;

    public HomeTimeline(MergeStrategy strategy) {
        this.mergeStrategy = strategy;
    }
    @Override public void clear() { tweets.clear(); }
    @Override public void mergeExternal(Timeline external) {
        mergeStrategy.merge(this.tweets, external);
    }
}

public interface MergeStrategy {
    void merge(List<Tweet> base, Timeline external);
}

// ==================== ENGAGEMENT ====================

public abstract class Engagement {
    private final String engagementId;
    private final long userId;
    private final String tweetId;
    private final long timestamp;

    public Engagement(String engagementId, long userId, String tweetId) {
        this.engagementId = engagementId;
        this.userId = userId;
        this.tweetId = tweetId;
        this.timestamp = System.currentTimeMillis();
    }
    public String getTweetId() { return tweetId; }
}
public class Like extends Engagement { public Like(String id, long uid, String tid) { super(id, uid, tid); } }
public class Retweet extends Engagement { public Retweet(String id, long uid, String tid) { super(id, uid, tid); } }
public class Reply extends Engagement { public Reply(String id, long uid, String tid) { super(id, uid, tid); } }

// ==================== SERVICES & MANAGERS ====================

// Repository Interfaces (Dependency Inversion)
public interface TweetRepository { void save(Tweet tweet); Tweet findById(String id); }
public interface FollowRepository { List<Long> getFollowees(long userId); boolean follow(long followerId, long followeeId); }
public interface NotificationPublisher { void publish(Notification n); }
public interface EngagementRepository { void record(Engagement e); }

public class Notification {
    public enum Type { LIKE, REPLY, FOLLOW, RETWEET }
    private final Type type;
    private final long actorId;
    private final String targetId;
    private final long createdAt;

    public Notification(Type type, long actorId, String targetId) {
        this.type = type; this.actorId = actorId; this.targetId = targetId; this.createdAt = System.currentTimeMillis();
    }
    public Type getType() { return type; }
    public long getActorId() { return actorId; }
    public String getTargetId() { return targetId; }
}

// Observer Pattern Interface
public interface FollowerObserver {
    void onNewTweet(Tweet tweet, long authorId);
}

public class FeedService implements FollowerObserver {
    // Singleton Pattern
    private static volatile FeedService instance;
    private final Map<Long, Timeline> timelineCache = new ConcurrentHashMap<>();
    private final FeedGenerationStrategy strategy;
    private final NotificationPublisher publisher;

    private FeedService(FeedGenerationStrategy strategy, NotificationPublisher publisher) {
        this.strategy = strategy;
        this.publisher = publisher;
    }

    public static synchronized FeedService getInstance(FeedGenerationStrategy strategy, NotificationPublisher publisher) {
        if (instance == null) { instance = new FeedService(strategy, publisher); }
        return instance;
    }

    public HomeTimeline getHomeTimeline(long userId) {
        return (HomeTimeline) timelineCache.computeIfAbsent(userId, id -> new HomeTimeline(new DefaultMergeStrategy()));
    }

    // Observer callback
    @Override
    public synchronized void onNewTweet(Tweet tweet, long authorId) {
        strategy.pushToFollowers(tweet, authorId, publisher);
    }
}

public interface FeedGenerationStrategy {
    void pushToFollowers(Tweet tweet, long authorId, NotificationPublisher publisher);
    List<Tweet> pullFromFollowees(long userId, FollowRepository repo, int limit);
}

public class HybridStrategy implements FeedGenerationStrategy {
    @Override public void pushToFollowers(Tweet tweet, long authorId, NotificationPublisher publisher) {
        // Async push to low-follower users, queue high-follower updates
    }
    @Override public List<Tweet> pullFromFollowees(long userId, FollowRepository repo, int limit) {
        // Fetch recent tweets from DB/Cache, merge client-side
        return Collections.emptyList();
    }
}

public class TimelineManager {
    private final FeedStrategy strategy;
    private final FeedService feedService;

    public TimelineManager(FeedStrategy strategy, FeedService feedService) {
        this.strategy = strategy;
        this.feedService = feedService;
    }

    public void updateTimelines(Tweet tweet, long authorId) {
        feedService.onNewTweet(tweet, authorId);
    }
}

public class TweetService {
    private final TweetRepository repo;
    private final NotificationPublisher publisher;
    private final TimelineManager timelineManager;
    private final TweetFactory factory;

    public TweetService(TweetRepository repo, NotificationPublisher publisher, 
                        TimelineManager manager, TweetFactory factory) {
        this.repo = repo;
        this.publisher = publisher;
        this.timelineManager = manager;
        this.factory = factory;
    }

    public Tweet createTweet(String id, long authorId, TweetType type, String content, String mediaUrl) {
        // Factory Pattern delegation
        Tweet tweet = factory.create(id, authorId, type, content, mediaUrl);
        repo.save(tweet);
        timelineManager.updateTimelines(tweet, authorId);
        return tweet;
    }

    public void engage(String tweetId, Engagement engagement) {
        repo.findById(tweetId).ifPresent(t -> {
            if (engagement instanceof Like) t.incrementLikes();
            else if (engagement instanceof Retweet) t.incrementRetweets();
            repo.save(t); // Upsert
        });
        // Publish notification to author
        publisher.publish(new Notification(getType(engagement), engagement.getUserId(), tweetId));
    }

    private Notification.Type getType(Engagement e) {
        if (e instanceof Like) return Notification.Type.LIKE;
        if (e instanceof Retweet) return Notification.Type.RETWEET;
        return Notification.Type.REPLY;
    }
}

// Factory Pattern
public interface TweetFactory { Tweet create(String id, long authorId, TweetType type, String content, String mediaUrl); }

public class DefaultTweetFactory implements TweetFactory {
    @Override
    public Tweet create(String id, long authorId, TweetType type, String content, String mediaUrl) {
        return new Tweet.Builder(id, authorId)
                .content(content).type(type).mediaUrl(mediaUrl)
                .createdAtMs(System.currentTimeMillis())
                .build();
    }
}

public class TwitterSystem {
    private static volatile TwitterSystem instance;
    public TweetService tweetService;
    public FeedService feedService;

    public static synchronized TwitterSystem initialize() {
        if (instance != null) return instance;
        instance = new TwitterSystem();
        // Wire dependencies (DI in production)
        return instance;
    }
}
```

---

### 4. Relationships & Class Diagram (Textual UML)

```
[User] "1" o-- "1" UserTimeline (Composition)
[User] "1" *-- "0..*" Tweet (Composition)
[User] "1" --> "0..*" FollowRelation (Aggregation)
[User] "1" --> "0..*" Notification (Association)

[Tweet] "1" *-- "0..*" Engagement (Aggregation)
[Like] --|> [Engagement] (Inheritance)
[Retweet] --|> [Engagement]
[Reply] --|> [Engagement]

[Timeline] <|.. [UserTimeline] (Generalization)
[Timeline] <|.. [HomeTimeline]
[HomeTimeline] "1" o-- "1..*" MergeStrategy (Composition)

[TweetService] --> [TimelineManager] (Dependency)
[TweetService] --> [TweetFactory] (Dependency/Strategy)
[TweetService] --> [NotificationPublisher] (Dependency)

[FeedService] "1" --> "1" FeedGenerationStrategy (Association/Strategy)
[FeedService] implements FollowerObserver (Realization)
```

**Multiplicity Notes**: 
- `1 o-- 0..*` = Composition (lifecycle bound)
- `1 *-- 0..*` = Aggregation (independent lifecycle)
- `1 --> *` = Association
- `|..` = Realization/Implementation

---

### 5. Design Patterns Used

| Pattern | Implementation Context | Why Used |
|---------|------------------------|----------|
| **Singleton** | `TwitterSystem`, `FeedService` | Global coordination point & centralized timeline cache; ensures single source of truth for feed orchestration |
| **Factory** | `TweetFactory` / `DefaultTweetFactory` | Decouples tweet instantiation logic; allows future extension for `QuotedTweet`, `PollTweet` without modifying `TweetService` |
| **Builder** | `Tweet.Builder` | Handles optional media, metadata, and validation cleanly; prevents telescoping constructors and enforces immutable domain object |
| **Strategy** | `FeedGenerationStrategy`, `MergeStrategy` | Enables runtime selection of push/pull/hybrid models and timeline merge algorithms; OCP compliant |
| **Observer** | `FollowerObserver` interface + `FeedService.onNewTweet()` | Decouples tweet publishing from follower notification/timeline updates; enables async event dispatch |

---

### 6. Core Flow (Very Important)

#### Post Tweet Flow
1. Client submits payload + idempotency key → `TweetService.createTweet()`
2. Validate length, auth, rate limits
3. `TweetFactory` builds immutable `Tweet` via Builder
4. Persist to primary DB (async write-ahead log for HA)
5. `TimelineManager` triggers `FeedService.onNewTweet()`
6. Observer dispatches to `FeedGenerationStrategy` (push for low-fanout followers)
7. Publish engagement/follow notifications via MQ

#### Feed Generation Flow
1. Client calls `FeedService.getHomeTimeline(userId)`
2. Check Redis cache; if miss/hit threshold → apply `HybridStrategy.pullFromFollowees()`
3. Fetch latest tweet IDs from followees (indexed query)
4. Resolve metadata from DB/cache, merge in-memory via `MergeStrategy` (O(N log N) sort by Snowflake ID)
5. Return paginated `HomeTimeline` slice; update cache with TTL

#### Follow/Unfollow Flow
1. Validate cycle & limits → `FollowRepository` upserts edge
2. Invalidate cached timeline slices for follower
3. Async job backfills last 100 tweets from new followee
4. Publish `FOLLOW` notification to followee

#### Like/Retweet Flow
1. Idempotency check → `TweetService.engage()`
2. Acquire row-level lock (or use `UPDATE ... RETURNING` / `atomic increment`)
3. Increment counters in-memory & persist asynchronously
4. Trigger `NotificationPublisher` for original author
5. Invalidate specific engagement cache keys

#### Notification Flow
1. Triggered by `engage()`, `follow()`, or `reply`
2. `Notification` object serialized → pushed to Kafka/SQS
3. Consumer workers deduplicate, aggregate, store in `notifications` table
4. Pusher service delivers via WebSocket/APNs; marks `delivered = true`

---

### 7. API Design (Conceptual Only)

| Endpoint | Method | Request | Response | Status Codes |
|----------|--------|---------|----------|--------------|
| `/v1/tweets` | `POST` | `{ content, media_url?, type, idempotency_key }` | `{ tweet_id, created_at, status: "queued" }` | `201 Created`, `400 Bad Request`, `409 Conflict (dup key)`, `413 Too Large` |
| `/v1/timelines/home` | `GET` | `?limit=20&cursor=abc123` | `{ tweets: [...], next_cursor, has_more }` | `200 OK`, `401 Unauthorized`, `429 Rate Limited` |
| `/v1/users/{id}/follow` | `POST` | `{ follower_id }` | `{ success: true, timestamp }` | `201 Created`, `404 Not Found`, `403 Self-follow` |
| `/v1/users/{id}/follow` | `DELETE` | `{ follower_id }` | `{ success: true }` | `200 OK`, `404 Not Found` |
| `/v1/tweets/{tid}/like` | `POST` | `{ user_id, idempotency_key }` | `{ like_id, tweet_like_count }` | `200 OK`, `409 Already Liked` |
| `/v1/notifications` | `GET` | `?since=ts&limit=10` | `{ notifications: [...], unread_count }` | `200 OK` |

*Behavioral Notes*: All mutations use `Idempotency-Key` header. Pagination uses cursor-based offsets for stable sorting. Read endpoints return eventual-consistent data with `X-Feed-Version` header.

---

### 8. Database Schema Design

**`users`**
| Column | Type | Constraint |
|--------|------|------------|
| user_id | BIGINT | PK |
| username | VARCHAR(64) | UNIQUE, NOT NULL |
| created_at | TIMESTAMP | DEFAULT NOW() |
| status | VARCHAR(16) | DEFAULT 'active' |

**`tweets`**
| Column | Type | Constraint |
|--------|------|------------|
| tweet_id | VARCHAR(36) | PK |
| author_id | BIGINT | FK -> users |
| content | VARCHAR(4000) | NOT NULL |
| type | VARCHAR(10) | ENUM('TEXT','MEDIA') |
| media_url | VARCHAR(500) | NULL |
| like_count | INT | DEFAULT 0 |
| retweet_count | INT | DEFAULT 0 |
| created_at | BIGINT | Indexed |
| **Index** | `(author_id, created_at DESC)` | For user timelines |

**`follows`**
| Column | Type | Constraint |
|--------|------|------------|
| follower_id | BIGINT | PK, FK -> users |
| followee_id | BIGINT | PK, FK -> users |
| followed_at | TIMESTAMP | |
| **Index** | `(follower_id)`, `(followee_id)` | For fan-out & lookup |

**`engagements`** (Likes, RTs, Replies unified)
| Column | Type | Constraint |
|--------|------|------------|
| engagement_id | VARCHAR(36) | PK |
| user_id | BIGINT | FK -> users |
| tweet_id | VARCHAR(36) | FK -> tweets |
| type | VARCHAR(10) | ENUM('LIKE','RT','REPLY') |
| created_at | TIMESTAMP | |
| **Index** | UNIQUE(user_id, tweet_id, type) | Idempotency enforcement |

**`timelines`** (Fan-out cache table)
| Column | Type | Constraint |
|--------|------|------------|
| user_id | BIGINT | PK, Part of PK |
| tweet_id | VARCHAR(36) | PK, Part of PK |
| sort_order | BIGINT | Indexed DESC (Snowflake/timestamp) |

**`notifications`**
| Column | Type | Constraint |
|--------|------|------------|
| id | BIGINT | PK (Auto-increment/Snowflake) |
| recipient_id | BIGINT | FK -> users, Indexed |
| actor_id | BIGINT | FK -> users |
| target_id | VARCHAR(36) | tweet/user ID |
| type | VARCHAR(20) | Indexed |
| status | VARCHAR(10) | 'UNREAD', 'READ' |
| **Index** | `(recipient_id, created_at DESC)` | Notification fetch |

---

### 9. Concurrency & Scalability

* **Write Throughput**: Use Kafka/SQS for async timeline fan-out and notification delivery. Primary DB writes are batched via WAL. Idempotency keys prevent duplicate processing under retries.
* **Feed Optimization**: 
  * **Fan-out on Write**: ≤500 followers → insert directly into `timelines` table/Redis sorted set.
  * **Fan-out on Read**: >500 followers → fetch on-demand, merge in-app, cache result for 15s.
  * Hybrid approach guarantees low P99 latency for both celebs and average users.
* **Caching**: Redis `ZSET` keyed by `timeline:{userId}`. TTL 30s-2m. Hot followers pre-warmed. Stale cache serves degraded content.
* **Sharding**: User-based horizontal partitioning (`hash(user_id) % 1024`). Tweets follow author shard. Cross-shard joins avoided via denormalized `timelines` table.
* **Consistency Model**: Eventual for feeds/counts. Optimistic concurrency control (`version` column) for engagement updates. Conflict resolution via LWW (Last Write Wins) on counters.

---

### 10. Edge Cases

| Case | Mitigation |
|------|------------|
| **Duplicate Tweets** | `Idempotency-Key` in headers + DB unique constraint on `(author_id, idempotency_key)` |
| **Out-of-order Feed** | Snowflake IDs guarantee monotonic ordering; `ORDER BY tweet_id DESC` as primary sort, `created_at` as secondary |
| **Celebrity Problem** (10M+ followers) | Never push-write to timeline. Use read-time aggregation, pre-rank, and edge caching. Fallback to sampled feed under extreme load |
| **Deleted Tweets** | Soft delete with `is_deleted` flag. Async invalidation job sweeps `timelines` table. Cache tagged with `tweet_version` for immediate miss |
| **Spam Handling** | Rate limiting per IP/user. ML classifier on content hash/velocity. Shadow-ban: writes succeed but excluded from timeline generation |

---

### 11. Sample SQL Queries

```sql
-- 1. Fetch user timeline (last 50 tweets)
SELECT t.tweet_id, t.author_id, t.content, t.like_count, t.retweet_count
FROM tweets t
WHERE t.author_id = :userId 
ORDER BY t.created_at DESC
LIMIT 50;

-- 2. Get followers list for fan-out or notification
SELECT f.follower_id 
FROM follows f 
WHERE f.followee_id = :authorId;

-- 3. Fetch home timeline (Hybrid Pull: recent from followees)
SELECT t.tweet_id, t.author_id, t.content, t.created_at
FROM follows f
JOIN tweets t ON f.followee_id = t.author_id
WHERE f.follower_id = :userId 
  AND t.created_at > :last_fetch_ts
ORDER BY t.tweet_id DESC -- Snowflake ordering
LIMIT 20;

-- 4. Idempotent engagement insert
INSERT INTO engagements (engagement_id, user_id, tweet_id, type, created_at)
VALUES (:id, :userId, :tweetId, :type, NOW())
ON CONFLICT (user_id, tweet_id, type) DO NOTHING;
```

---

### 12. Trade-offs & Extensions

* **Limitations**
  * Relational schema struggles with real-time graph traversals; high fan-out write storms still risk DB contention
  * Timeline `timelines` table grows rapidly; requires aggressive partitioning & TTL management
  * Eventual consistency means users may briefly see stale like counts or missing recent follows
* **Improvements**
  * **Distributed Feed**: Move to Kafka + Flink for streaming merge. Use Materialized Views (e.g., Druid/RocksDB) for low-latency reads.
  * **ML Ranking**: Replace chronological `MergeStrategy` with a lightweight scoring model (`engagement_prob * decay * relevance`). Batch-score offline, cache top 500.
  * **Media Handling**: Direct upload to S3/GCS via presigned URLs. CDN edge caching with signed cookies. Store only thumbnails & CDN links in DB.
  * **Trending**: Sliding window counters via Redis HyperLogLog + TopK. Aggregate by hashtag/mention in 5-min buckets.
