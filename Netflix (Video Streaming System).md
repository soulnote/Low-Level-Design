# Netflix (Video Streaming System)

### 1. Requirement Clarification

**Functional Requirements**
- **Browse & Search**: Query content by title, genre, release year, or actor.
- **Playback**: Stream video with pause, resume, seek, and quality adjustment.
- **Profiles**: Multiple profiles per account, personalized preferences & language.
- **Watch History**: Track progress, resume across devices, mark as watched.
- **Recommendations**: Generate content suggestions based on viewing patterns.
- **Content Upload**: Admin workflow for ingesting, encoding, and publishing media.

**Non-Functional Requirements**
- **High Scalability**: Support 10M+ concurrent viewers globally.
- **Low Latency**: Sub-second playback start, minimal buffering via adaptive bitrate.
- **High Availability**: 99.99% uptime for catalog & playback APIs.
- **Fault Tolerance**: Graceful CDN fallback, retry mechanisms, stateful session recovery.

**Assumptions**
- Media delivered via CDN using HLS/DASH protocols.
- Adaptive bitrate (ABR) handled client-side; server provides manifest & chunk URLs.
- Geo-licensing enforced via DRM/license server.
- Max 5 profiles per account.
- Watch progress synced every 10-15 seconds via heartbeat.

---

### 2. Core Entities & Responsibilities

| Entity | Responsibility |
|--------|----------------|
| `StreamingSystem` | Singleton facade. Orchestrates playback, session management, and catalog routing. |
| `User` / `Profile` | Account identity, preferences, device mappings, and personalized settings. |
| `Content` | Abstract catalog item. Holds metadata, region restrictions, and genre tags. |
| `Movie` / `Series` / `Episode` | Concrete content types. `Series` aggregates `Episode` entities. |
| `VideoMetadata` | Technical details: resolutions, codecs, subtitles, duration, DRM info. |
| `PlaybackSession` | Manages active streaming lifecycle. Tracks state, position, device, and quality. |
| `WatchHistory` | Stores per-profile progress, completion status, and timestamps. |
| `RecommendationService` | Computes & serves personalized content suggestions. |
| `CDNService` | External dependency interface. Returns manifest URLs, edge locations, and health status. |

---

### 3. Class Design (Java Implementation Required)

```java
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

// ================= ENUMS =================
enum ContentType { MOVIE, SERIES, EPISODE }
enum PlaybackStateType { IDLE, PLAYING, PAUSED, BUFFERING, STOPPED }
enum DeviceType { MOBILE, TV, WEB, TABLET }

// ================= DOMAIN MODELS =================
class UserProfile {
    private final String id;
    private final String name;
    private final String preferredLanguage;
    private final List<String> favorites;
    public UserProfile(String id, String name, String lang) {
        this.id = id; this.name = name; this.preferredLanguage = lang; this.favorites = new ArrayList<>();
    }
    public String getId() { return id; }
    public String getName() { return name; }
}

abstract class Content {
    protected final String id;
    protected final String title;
    protected final String genre;
    protected final Duration duration;
    protected final List<String> regionRestrictions;

    public Content(String id, String title, String genre, Duration duration, List<String> restrictions) {
        this.id = id; this.title = title; this.genre = genre; this.duration = duration;
        this.regionRestrictions = restrictions;
    }
    public String getId() { return id; }
    public String getTitle() { return title; }
    public boolean isAvailableInRegion(String region) { return !regionRestrictions.contains(region); }
}

class Movie extends Content {
    private final String releaseYear;
    public Movie(String id, String title, String genre, Duration dur, List<String> rest, String year) {
        super(id, title, genre, dur, rest); this.releaseYear = year;
    }
}

class Series extends Content {
    private final List<Episode> episodes;
    public Series(String id, String title, String genre, Duration totalDur, List<String> rest, List<Episode> eps) {
        super(id, title, genre, totalDur, rest); this.episodes = Collections.unmodifiableList(eps);
    }
    public List<Episode> getEpisodes() { return episodes; }
}

class Episode extends Content {
    private final String seriesId;
    private final int season;
    private final int episodeNum;
    public Episode(String id, String title, String genre, Duration dur, List<String> rest, String seriesId, int s, int e) {
        super(id, title, genre, dur, rest);
        this.seriesId = seriesId; this.season = s; this.episodeNum = e;
    }
}

class PlaybackSession {
    private final String sessionId;
    private final UserProfile profile;
    private final Content content;
    private final DeviceType device;
    private Duration currentPosition;
    private PlaybackStateType currentState;
    private PlaybackState stateDelegate;
    private String selectedQuality;
    private Instant createdAt;

    public PlaybackSession(String id, UserProfile profile, Content content, DeviceType device) {
        this.sessionId = id; this.profile = profile; this.content = content;
        this.device = device; this.currentPosition = Duration.ZERO;
        this.stateDelegate = new IdleState(this); this.currentState = PlaybackStateType.IDLE;
        this.createdAt = Instant.now();
    }

    void setState(PlaybackState next) { this.stateDelegate = next; this.currentState = next.getStateType(); }
    public void play() { stateDelegate.play(); }
    public void pause() { stateDelegate.pause(); }
    public void seek(Duration pos) { stateDelegate.seek(pos); }
    public void stop() { stateDelegate.stop(); }
    
    public String getSessionId() { return sessionId; }
    public UserProfile getProfile() { return profile; }
    public Content getContent() { return content; }
    public Duration getCurrentPosition() { return currentPosition; }
    public PlaybackStateType getCurrentState() { return currentState; }
    public DeviceType getDevice() { return device; }
}

// ================= STATE PATTERN =================
abstract class PlaybackState {
    protected PlaybackSession context;
    PlaybackState(PlaybackSession ctx) { this.context = ctx; }
    abstract PlaybackStateType getStateType();
    abstract void play();
    abstract void pause();
    abstract void seek(Duration pos);
    abstract void stop();
    protected void transition(PlaybackState next) { context.setState(next); }
}

class IdleState extends PlaybackState {
    IdleState(PlaybackSession ctx) { super(ctx); }
    public PlaybackStateType getStateType() { return PlaybackStateType.IDLE; }
    public void play() { transition(new PlayingState(context)); }
    public void pause() {}
    public void seek(Duration pos) { context.currentPosition = pos; }
    public void stop() { transition(new StoppedState(context)); }
}

class PlayingState extends PlaybackState {
    PlayingState(PlaybackSession ctx) { super(ctx); }
    public PlaybackStateType getStateType() { return PlaybackStateType.PLAYING; }
    public void play() {}
    public void pause() { transition(new PausedState(context)); }
    public void seek(Duration pos) { transition(new BufferingState(context)); context.currentPosition = pos; }
    public void stop() { transition(new StoppedState(context)); context.currentPosition = Duration.ZERO; }
}

class PausedState extends PlaybackState {
    PausedState(PlaybackSession ctx) { super(ctx); }
    public PlaybackStateType getStateType() { return PlaybackStateType.PAUSED; }
    public void play() { transition(new PlayingState(context)); }
    public void pause() {}
    public void seek(Duration pos) { context.currentPosition = pos; }
    public void stop() { transition(new StoppedState(context)); }
}

class BufferingState extends PlaybackState {
    BufferingState(PlaybackSession ctx) { super(ctx); }
    public PlaybackStateType getStateType() { return PlaybackStateType.BUFFERING; }
    public void play() { transition(new PlayingState(context)); }
    public void pause() { transition(new PausedState(context)); }
    public void seek(Duration pos) { context.currentPosition = pos; }
    public void stop() { transition(new StoppedState(context)); }
}

class StoppedState extends PlaybackState {
    StoppedState(PlaybackSession ctx) { super(ctx); }
    public PlaybackStateType getStateType() { return PlaybackStateType.STOPPED; }
    public void play() { transition(new PlayingState(context)); context.currentPosition = Duration.ZERO; }
    public void pause() {}
    public void seek(Duration pos) { context.currentPosition = pos; }
    public void stop() {}
}

// ================= STRATEGY PATTERN =================
interface RecommendationStrategy {
    List<Content> recommend(List<Content> catalog, UserProfile profile, List<String> watchHistory);
}

class CollaborativeFilteringStrategy implements RecommendationStrategy {
    public List<Content> recommend(List<Content> catalog, UserProfile profile, List<String> history) {
        // Mock: Returns top 5 unwatched items matching genre history
        Set<String> watchedGenres = new HashSet<>(); // In prod: ML model consumes history
        return catalog.stream().limit(5).toList();
    }
}

interface QualitySelectionStrategy {
    String selectQuality(DeviceType device, double bandwidthMbps);
}
class AdaptiveQualityStrategy implements QualitySelectionStrategy {
    public String selectQuality(DeviceType device, double bw) {
        if (bw > 25) return "4K_HDR";
        if (bw > 5) return "1080p";
        return "720p";
    }
}

// ================= OBSERVER PATTERN =================
interface PlaybackEventObserver {
    void onProgressUpdated(PlaybackSession session, Duration position, double completionPct);
    void onSessionEnded(PlaybackSession session);
}

class HistoryEventPublisher {
    private final List<PlaybackEventObserver> observers = new CopyOnWriteArrayList<>();
    public void register(PlaybackEventObserver o) { observers.add(o); }
    public void notifyProgress(PlaybackSession s, Duration pos, double pct) {
        observers.forEach(o -> o.onProgressUpdated(s, pos, pct));
    }
}

// ================= FACTORY PATTERN =================
class ContentFactory {
    public static Content create(String type, String id, String title, String genre, Duration dur, List<String> regions) {
        return switch (type.toLowerCase()) {
            case "movie" -> new Movie(id, title, genre, dur, regions, "2026");
            case "series" -> new Series(id, title, genre, dur, regions, new ArrayList<>());
            default -> throw new IllegalArgumentException("Unknown type");
        };
    }
}

// ================= SERVICES & MANAGERS =================
interface CDNService {
    String getManifestUrl(Content content, String region);
    String getHealthStatus();
}

class SessionManager {
    private static volatile SessionManager instance;
    private final Map<String, PlaybackSession> activeSessions = new ConcurrentHashMap<>();
    public static SessionManager getInstance() {
        if (instance == null) { synchronized(SessionManager.class) { if(instance==null) instance=new SessionManager(); } }
        return instance;
    }
    public PlaybackSession create(UserProfile p, Content c, DeviceType d) {
        String id = "sess-" + UUID.randomUUID().toString().substring(0,8);
        PlaybackSession s = new PlaybackSession(id, p, c, d);
        activeSessions.put(id, s);
        return s;
    }
    public PlaybackSession get(String id) { return activeSessions.get(id); }
    public void remove(String id) { activeSessions.remove(id); }
}

class WatchHistoryService implements PlaybackEventObserver {
    private final Map<String, Map<String, Duration>> history = new ConcurrentHashMap<>();
    public void onProgressUpdated(PlaybackSession s, Duration pos, double pct) {
        history.computeIfAbsent(s.getProfile().getId(), k -> new ConcurrentHashMap<>())
               .put(s.getContent().getId(), pos);
    }
    public void onSessionEnded(PlaybackSession s) {
        history.computeIfAbsent(s.getProfile().getId(), k -> new ConcurrentHashMap<>())
               .remove(s.getContent().getId());
    }
    public Duration getProgress(UserProfile p, String contentId) {
        return history.getOrDefault(p.getId(), Collections.emptyMap()).getOrDefault(contentId, Duration.ZERO);
    }
}

class RecommendationService {
    private final RecommendationStrategy strategy;
    public RecommendationService(RecommendationStrategy s) { this.strategy = s; }
    public List<Content> getRecommendations(UserProfile p, List<Content> catalog, List<String> history) {
        return strategy.recommend(catalog, p, history);
    }
}

class PlaybackService {
    private final CDNService cdn;
    private final QualitySelectionStrategy qualityStrategy;
    private final HistoryEventPublisher publisher;

    public PlaybackService(CDNService cdn, QualitySelectionStrategy qs, HistoryEventPublisher pub) {
        this.cdn = cdn; this.qualityStrategy = qs; this.publisher = pub;
    }

    public PlaybackSession startPlayback(UserProfile profile, Content content, DeviceType device, double bandwidth) {
        if (!content.isAvailableInRegion("US")) throw new SecurityException("Region restricted");
        
        PlaybackSession session = SessionManager.getInstance().create(profile, content, device);
        session.play();
        
        String manifest = cdn.getManifestUrl(content, "US");
        String quality = qualityStrategy.selectQuality(device, bandwidth);
        
        // Client uses manifest + quality to start ABR streaming
        return session;
    }

    public void updateProgress(String sessionId, Duration position) {
        PlaybackSession s = SessionManager.getInstance().get(sessionId);
        if (s != null) {
            s.seek(position);
            double pct = (double) position.toMillis() / s.getContent().getDuration().toMillis();
            publisher.notifyProgress(s, position, pct);
        }
    }

    public void pausePlayback(String sessionId) {
        PlaybackSession s = SessionManager.getInstance().get(sessionId);
        if (s != null) s.pause();
    }

    public void endPlayback(String sessionId) {
        PlaybackSession s = SessionManager.getInstance().get(sessionId);
        if (s != null) {
            s.stop();
            publisher.onSessionEnded(s);
            SessionManager.getInstance().remove(sessionId);
        }
    }
}

// ================= SINGLETON FACADE =================
class StreamingSystem {
    private static volatile StreamingSystem instance;
    private final PlaybackService playbackService;
    private final RecommendationService recService;
    private final SessionManager sessionManager;

    private StreamingSystem() {
        WatchHistoryService hist = new WatchHistoryService();
        HistoryEventPublisher pub = new HistoryEventPublisher();
        pub.register(hist);
        recService = new RecommendationService(new CollaborativeFilteringStrategy());
        playbackService = new PlaybackService(new MockCDNService(), new AdaptiveQualityStrategy(), pub);
        sessionManager = SessionManager.getInstance();
    }

    public static StreamingSystem getInstance() {
        if (instance == null) { synchronized(StreamingSystem.class) { if(instance==null) instance=new StreamingSystem(); } }
        return instance;
    }
    public PlaybackService getPlayback() { return playbackService; }
    public RecommendationService getRecommendations() { return recService; }
}

// Mock external dependency
class MockCDNService implements CDNService {
    public String getManifestUrl(Content content, String region) { return "https://cdn.vod.example.com/" + content.getId() + "/master.m3u8"; }
    public String getHealthStatus() { return "HEALTHY"; }
}
```

---

### 4. Relationships & Class Diagram (Textual UML)

```
[StreamingSystem] 1 <1..> 1 [PlaybackService]      (Composition)
[StreamingSystem] 1 <1..> 1 [RecommendationService] (Composition)
[PlaybackService] 1 --> 1 [SessionManager]         (Aggregation/Dependency)
[PlaybackService] 1 --> 1 [HistoryEventPublisher]  (Composition)

[User] 1 <1..> * [UserProfile]                     (Composition: Profiles owned by account)
[UserProfile] 1 <1..> * [PlaybackSession]          (Association: Session runs under profile)

[Content] <|-- [Movie]                             (Inheritance)
[Content] <|-- [Series]                            (Inheritance)
[Series] 1 *--> * [Episode]                        (Composition: Series cannot exist without Episodes in this context)
[PlaybackSession] 1 --> 1 [Content]                (Association: Session plays one content item)

[PlaybackSession] 1 --> 1 [PlaybackState]          (Composition/Context: State Pattern delegation)
[PlaybackState] <|-- [IdleState], [PlayingState]   (Inheritance: Concrete states)

[RecommendationService] --> [RecommendationStrategy] (Dependency Injection: Strategy)
[QualitySelectionStrategy] <|-- [AdaptiveQualityStrategy] (Inheritance)
[HistoryEventPublisher] 1 o--> * [PlaybackEventObserver] (Aggregation: Observer)
[ContentFactory] ..> [Content]                     (Creation: Factory Method)
```

---

### 5. Design Patterns Used

| Pattern | Implementation in Code |
|---------|------------------------|
| **Singleton** | `SessionManager` & `StreamingSystem` use double-checked locking. Ensures single source of truth for active sessions & system bootstrap. |
| **Factory** | `ContentFactory.create()` abstracts `Movie`/`Series` instantiation. Open-Closed compliant: new `Documentary` or `LiveEvent` types added without modifying services. |
| **Strategy** | `RecommendationStrategy` & `QualitySelectionStrategy` interfaces decouple algorithms from core playback. Swappable at runtime (e.g., `BandwidthBasedQuality` vs `DeviceCapQuality`). |
| **State** | `PlaybackSession` delegates behavior to `PlaybackState`. Transitions (`PLAYING → PAUSED → BUFFERING`) enforced by concrete states. Prevents illegal ops (e.g., pause from IDLE). |
| **Observer** | `HistoryEventPublisher` maintains `PlaybackEventObserver` list. Fires on progress updates & session end. Decouples persistence, analytics, and recommendation triggers from playback logic. |

---

### 6. Core Flow (Very Important)

#### Content Browsing Flow
1. **Request**: Client sends search/filter query (genre, title, release year).
2. **Cache Check**: `ContentService` queries Redis cache for pre-indexed metadata.
3. **DB Fallback**: Cache miss triggers catalog DB lookup with full-text index.
4. **Ranking & Filter**: Removes geo-restricted content. Applies trending/personalization scoring.
5. **Response**: Returns paginated `ContentMetadata` list with thumbnails & runtime.

#### Playback Start Flow
1. **Validation**: `PlaybackService.startPlayback()` checks region license & DRM entitlements.
2. **Session Creation**: `SessionManager` generates unique `sessionId`. Initializes `IdleState`.
3. **CDN Routing**: Requests `master.m3u8` manifest from nearest CDN edge via `CDNService`.
4. **Quality Selection**: `QualitySelectionStrategy` evaluates device capability & bandwidth. Returns `1080p`/`4K` profile.
5. **Handoff**: Returns `{ manifestUrl, sessionId, initialQuality, resumePosition }` to client. Player begins ABR stream.

#### Adaptive Streaming Flow
1. **Client Monitoring**: Player measures buffer health, network throughput, & packet loss.
2. **Bitrate Switch**: Player requests next chunk at higher/lower bitrate from CDN.
3. **Server Logging**: `PlaybackService.updateProgress()` receives periodic heartbeat. Updates `currentPosition` & logs quality switch for analytics.
4. **Fallback**: If CDN edge fails, player retries with `backup.m3u8` URL. `CDNService` routes to healthy origin.

#### Pause/Resume Flow
1. **Pause**: Client calls `pausePlayback(sessionId)`. `PlaybackSession` transitions to `PausedState`. Position frozen.
2. **Sync**: Heartbeat stops. `HistoryEventPublisher` logs final position.
3. **Resume**: Client calls `startPlayback()` with same `sessionId` or new request. Server returns `resumePosition`. Player seeks & transitions to `PlayingState`.

#### Watch History Flow
1. **Heartbeat**: Client emits progress event every 10s.
2. **Observer Trigger**: `HistoryEventPublisher` notifies `WatchHistoryService`.
3. **Async Persist**: Service writes to Kafka/DB: `(profileId, contentId, timestamp, pct)`.
4. **Recommendation Update**: Observer triggers background ML job to recompute similarity scores. Refreshes "Because You Watched" row.

---

### 7. API Design (Conceptual Only)

| Endpoint | Method | Request Payload | Response | Status Codes |
|----------|--------|-----------------|----------|--------------|
| `searchContent` | `GET /catalog/search` | Query: `?q=action&genre=scifi&page=1&lang=en` | `{ items: [{id, title, thumbnail, runtime, maturity}], total: 42 }` | 200 OK, 400 Invalid Params |
| `playContent` | `POST /playback/start` | `{ profileId, contentId, deviceId, bandwidthMbps: 12.5 }` | `{ sessionId, manifestUrl, initialQuality: "1080p", resumeAt: 0, expiresAt: "..." }` | 200 OK, 403 GeoRestricted, 409 AlreadyPlaying |
| `pausePlayback` | `POST /playback/pause` | `{ sessionId }` | `{ status: "PAUSED", lastPosition: "PT00:42:15" }` | 200 OK, 404 Invalid Session |
| `updateProgress` | `PATCH /playback/progress` | `{ sessionId, positionMs: 2535000 }` | N/A | 204 No Content, 400 Invalid Timestamp |
| `getRecommendations` | `GET /recommendations` | Query: `?profileId=xyz&limit=10` | `{ rows: [{name: "Trending", items: [...]}, {name: "Because you watched X", items: [...]}] }` | 200 OK, 401 Unauthorized |

*Note: All APIs are idempotent where applicable. DRM tokens passed via headers.*

---

### 8. Database Schema Design

**Tables & Columns**
- `users` (`id` PK UUID, `email` VARCHAR UNIQUE, `status` ENUM, `created_at` TIMESTAMP)
- `profiles` (`id` PK UUID, `user_id` FK, `name` VARCHAR, `lang_pref` VARCHAR, `avatar_url` VARCHAR)
- `content` (`id` PK UUID, `type` ENUM, `title` VARCHAR, `genre` VARCHAR, `duration_ms` INT, `release_year` INT, `region_whitelist` TEXT[], `metadata_json` JSONB)
- `episodes` (`id` PK UUID, `series_id` FK, `season` INT, `episode_num` INT, `title` VARCHAR, `duration_ms` INT)
- `playback_sessions` (`id` PK UUID, `profile_id` FK, `content_id` FK, `device_type` ENUM, `status` ENUM, `started_at` TIMESTAMP)
- `watch_history` (`profile_id` VARCHAR, `content_id` VARCHAR, `last_position_ms` INT, `completion_pct` DECIMAL, `updated_at` TIMESTAMP, PRIMARY KEY(profile_id, content_id))

**Indexing Strategy**
- `content(genre, release_year DESC)` → Fast genre/year filtering for browse.
- `watch_history(profile_id, updated_at DESC)` → Instant resume point & history fetch.
- `profiles(user_id)` → B-Tree for account profile resolution.
- `content(title)` + Full-Text Index (TSVECTOR) → Search autocomplete & ranking.
- `episodes(series_id, season, episode_num)` → Episode sequence ordering.

---

### 9. Concurrency & Scalability

- **Stateless Playback APIs**: Play/Pause/Seek endpoints scale horizontally behind ALB. Session state offloaded to Redis (`SESSION:{id}` → JSON).
- **CDN-First Delivery**: 95%+ traffic hits edge caches. Origin servers only serve initial manifests & cache misses. Reduces backend load drastically.
- **Async Progress Tracking**: Heartbeats published to Kafka. Consumers batch-write to DB. Prevents write amplification under millions of concurrent streams.
- **Connection Pooling & Keep-Alive**: CDN & API servers use HTTP/2 multiplexing. Reduces TCP handshake overhead.
- **Read/Write Splitting**: Catalog reads use replicas. Session writes go to primary. Cache-aside pattern (Redis) for hot content metadata.
- **Geo-Routing**: DNS & Global Load Balancer direct clients to nearest CDN PoP & regional API clusters.

---

### 10. Edge Cases

| Scenario | Handling |
|----------|----------|
| **Network Interruption** | Client buffers & retries. If >30s offline, marks session idle. Server cleans up Redis session after TTL. |
| **Playback Failures (DRM/404)** | Player receives 403/404. Fallback to backup CDN or lower quality manifest. Logs error to analytics. |
| **Device Switching** | User pauses on TV, opens app on mobile. Server returns `resumeAt` from `watch_history`. Player seeks automatically. |
| **Region Restriction** | License check fails at `startPlayback()`. Returns 403 with `region_blocked` code. UI hides play button. |
| **Sync Conflicts** | Two devices report progress simultaneously. DB uses `updated_at` for last-write-wins. Client reconciles on next heartbeat. |

---

### 11. Sample SQL Queries

**Fetch trending content (last 7 days):**
```sql
SELECT c.id, c.title, c.genre, COUNT(wh.profile_id) AS viewership
FROM watch_history wh
JOIN content c ON wh.content_id = c.id
WHERE wh.updated_at >= NOW() - INTERVAL '7 days'
GROUP BY c.id, c.title, c.genre
ORDER BY viewership DESC, wh.updated_at DESC
LIMIT 50;
```

**Get watch history for a profile (resume points):**
```sql
SELECT wh.content_id, c.title, wh.last_position_ms, wh.completion_pct, c.duration_ms
FROM watch_history wh
JOIN content c ON wh.content_id = c.id
WHERE wh.profile_id = $1
ORDER BY wh.updated_at DESC
LIMIT 20;
```

**Get recommended content (simple genre-based):**
```sql
SELECT c.* FROM content c
WHERE c.genre IN (
    SELECT genre FROM content WHERE id IN (
        SELECT content_id FROM watch_history WHERE profile_id = $1 ORDER BY updated_at DESC LIMIT 5
    )
)
AND c.id NOT IN (SELECT content_id FROM watch_history WHERE profile_id = $1)
ORDER BY c.release_year DESC
LIMIT 10;
```

---

### 12. Trade-offs & Extensions

**Limitations of Current Design**
- **In-Memory/Redis State in LLD**: Real production requires distributed session stores with replication (Redis Cluster/DynamoDB).
- **Synchronous Start Playback**: DRM/license validation can add 100-300ms latency. Pre-warming tokens improves UX.
- **Basic Recommendation Strategy**: Rule-based/genre matching lacks personalization. Scales poorly with 100M+ catalog.

**Improvements & Extensions**
1. **Multi-Region Active-Active**: Deploy regional API clusters with async CDC replication for catalog & history. Cross-region failover via Route53 latency routing.
2. **ML-Based Recommendations**: Replace `CollaborativeFilteringStrategy` with TensorFlow/PyTorch models consuming Kafka streams. Real-time feature store (Redis + Feast) for instant inference.
3. **Offline Downloads**: Generate DRM-protected encrypted chunks for local storage. License server issues offline tokens with expiration. Sync progress on reconnect.
4. **Content Prefetching**: Predict next episode or popular titles based on watch velocity. Pre-warm CDN edges in target PoPs. Reduces startup latency by 40%.
5. **Advanced ABR Client Logging**: Telemetry pipeline captures chunk-level throughput, rebuffer ratio, & bitrate switches. Powers QoE dashboards & adaptive algorithm tuning.
6. **Live Event Streaming**: Extend `Content` to `LiveStream`. Use Low-Latency HLS (LL-HLS) & chunked transfer encoding. Session manager tracks concurrent viewers for ad insertion scaling.
