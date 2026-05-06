# Cricbuzz
### 1. Requirement Clarification

**Functional Requirements**
- Display live match scores, run rate, partnerships, and fall of wickets.
- Provide ball-by-ball commentary with event metadata (runs, extras, wicket details).
- Maintain match schedule, status tracking, and historical results.
- Track & display player & team statistics (strike rates, economy, milestones).
- Push real-time notifications for key events: wickets, boundaries, 50s/100s, match end.

**Non-Functional Requirements**
- **Real-time Updates**: End-to-end latency < 1.5s from official feed to client delivery.
- **High Scalability**: Handle 5M+ concurrent users during marquee matches (World Cup/IPL).
- **High Availability**: 99.99% uptime; graceful degradation if feed is delayed.
- **Event-Driven Architecture**: Decouple ingestion, processing, scoring, and delivery.

**Assumptions**
- External scoring provider delivers structured JSON via HTTPS/Webhook at ~100ms intervals.
- Standard cricket rules apply; Duckworth-Lewis-Stern (DLS) handled as extension.
- Push updates via WebSockets/SSE; fallback to HTTP polling for legacy clients.
- Event sequence numbers guarantee ordering and idempotency.

---

### 2. Core Entities & Responsibilities

| Entity | Responsibility |
|--------|----------------|
| `CricketSystem` | Singleton orchestrator. Bootstraps services, manages match lifecycle, routes events. |
| `Match` | Aggregate root. Holds format, teams, state, active innings, and score reference. |
| `Team` / `Player` | Static roster data with role, batting order, bowling style. |
| `Innings` / `Over` / `Ball` | Hierarchical scoring structure. Tracks runs, wickets, extras, and sequence. |
| `Scorecard` | Calculates derived stats: run rate, partnerships, projections, milestone tracking. |
| `Commentary` | Textual + structured metadata per delivery. Persists for history & replay. |
| `MatchEvent` | Abstract domain event representing a scoring action (Wicket, Boundary, Wide, etc.). |
| `NotificationService` | Filters events against user subscriptions, formats payloads, dispatches pushes. |

---

### 3. Class Design (Java Implementation Required)

```java
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

// ================= ENUMS =================
enum MatchFormat { T20, ODI, TEST }
enum MatchStatus { UPCOMING, LIVE, COMPLETED, ABANDONED }
enum DeliveryType { NORMAL, WIDE, NO_BALL, BYE, LEG_BYE }
enum WicketType { BOWLED, CAUGHT, LBW, RUN_OUT, STUMPED }

// ================= DOMAIN MODELS =================
class Player {
    private final String id;
    private final String name;
    private final String role; // BATTER, BOWLER, ALL_ROUNDER, WICKET_KEEPER
    public Player(String id, String name, String role) { this.id = id; this.name = name; this.role = role; }
    public String getId() { return id; }
    public String getName() { return name; }
}

class Team {
    private final String id;
    private final String name;
    private final List<Player> squad;
    public Team(String id, String name, List<Player> squad) {
        this.id = id; this.name = name; this.squad = Collections.unmodifiableList(squad);
    }
    public String getId() { return id; }
    public String getName() { return name; }
}

class Ball {
    private final int sequence;
    private final int overNumber;
    private final String ballNumber; // e.g., "0.3", "1.2"
    private final String batsmanId;
    private final String bowlerId;
    private final int runsScored;
    private final DeliveryType type;
    private final String commentaryText;

    public Ball(int seq, int overNum, String ballNum, String batsman, String bowler, int runs, DeliveryType type, String text) {
        this.sequence = seq; this.overNumber = overNum; this.ballNumber = ballNum;
        this.batsmanId = batsman; this.bowlerId = bowler; this.runsScored = runs;
        this.type = type; this.commentaryText = text;
    }
    public int getRunsScored() { return runsScored; }
    public String getBowlerId() { return bowlerId; }
    public DeliveryType getType() { return type; }
    public int getSequence() { return sequence; }
}

class Over {
    private final int overNumber;
    private final String bowlerId;
    private final List<Ball> deliveries = new ArrayList<>();
    private int runs = 0;
    private int wickets = 0;

    public Over(int overNumber, String bowlerId) { this.overNumber = overNumber; this.bowlerId = bowlerId; }
    public synchronized void addBall(Ball ball) {
        deliveries.add(ball);
        runs += ball.getRunsScored();
        // Wicket logic simplified for brevity
    }
    public int getOverNumber() { return overNumber; }
    public int getRuns() { return runs; }
}

class Innings {
    private final int inningsNumber;
    private final Team battingTeam;
    private final Team bowlingTeam;
    private final List<Over> overs = new ArrayList<>();
    private volatile int totalRuns = 0;
    private volatile int totalWickets = 0;
    private volatile int ballsBowled = 0;
    private final ReentrantLock lock = new ReentrantLock();

    public Innings(int num, Team batting, Team bowling) {
        this.inningsNumber = num; this.battingTeam = batting; this.bowlingTeam = bowling;
    }
    public synchronized void recordBall(Ball ball) {
        lock.lock(); try {
            totalRuns += ball.getRunsScored();
            if (ball.getType() != DeliveryType.WIDE && ball.getType() != DeliveryType.NO_BALL) ballsBowled++;
            // Over management logic would go here
        } finally { lock.unlock(); }
    }
    public double getRunRate() { return ballsBowled == 0 ? 0.0 : (totalRuns * 6.0) / ballsBowled; }
    public int getTotalRuns() { return totalRuns; }
    public int getTotalWickets() { return totalWickets; }
}

class Match {
    private final String id;
    private final MatchFormat format;
    private final Team team1;
    private final Team team2;
    private volatile MatchStatus status;
    private MatchState state;
    private Innings currentInnings;

    public Match(String id, MatchFormat format, Team t1, Team t2) {
        this.id = id; this.format = format; this.team1 = t1; this.team2 = t2;
        this.status = MatchStatus.UPCOMING;
        this.state = new UpcomingState(this);
    }
    public synchronized void transitionState() { this.state = this.state.transition(); this.status = this.state.getStatus(); }
    public void processBall(Ball b) { state.recordBall(b); }
    public String getId() { return id; }
    public MatchFormat getFormat() { return format; }
    public Innings getCurrentInnings() { return currentInnings; }
    public MatchStatus getStatus() { return status; }
    public void setCurrentInnings(Innings i) { this.currentInnings = i; this.status = MatchStatus.LIVE; }
}

class Commentary {
    private final String matchId;
    private final int ballSequence;
    private final String text;
    private final Instant timestamp;
    public Commentary(String matchId, int seq, String text) {
        this.matchId = matchId; this.ballSequence = seq; this.text = text; this.timestamp = Instant.now();
    }
}

// ================= STATE PATTERN =================
interface MatchState {
    MatchState transition();
    MatchStatus getStatus();
    void recordBall(Ball b);
}

class UpcomingState implements MatchState {
    private final Match ctx;
    public UpcomingState(Match m) { this.ctx = m; }
    public MatchState transition() { ctx.transitionState(); return new LiveState(ctx); }
    public MatchStatus getStatus() { return MatchStatus.UPCOMING; }
    public void recordBall(Ball b) { throw new IllegalStateException("Match not started"); }
}

class LiveState implements MatchState {
    private final Match ctx;
    public LiveState(Match m) { this.ctx = m; }
    public MatchState transition() { ctx.transitionState(); return new CompletedState(ctx); }
    public MatchStatus getStatus() { return MatchStatus.LIVE; }
    public void recordBall(Ball b) { 
        if (ctx.getCurrentInnings() != null) ctx.getCurrentInnings().recordBall(b);
    }
}

class CompletedState implements MatchState {
    private final Match ctx;
    public CompletedState(Match m) { this.ctx = m; }
    public MatchState transition() { return this; }
    public MatchStatus getStatus() { return MatchStatus.COMPLETED; }
    public void recordBall(Ball b) { /* Ignore late events */ }
}

// ================= STRATEGY & FACTORY =================
interface MatchFormatStrategy {
    int getMaxOvers();
    int getPowerplayOvers();
}

class T20FormatStrategy implements MatchFormatStrategy {
    public int getMaxOvers() { return 20; }
    public int getPowerplayOvers() { return 6; }
}
class ODIFormatStrategy implements MatchFormatStrategy {
    public int getMaxOvers() { return 50; }
    public int getPowerplayOvers() { return 10; }
}

interface MatchEvent {}
class WicketEvent implements MatchEvent { public final WicketType type; public final String batsmanId; public WicketEvent(WicketType t, String id) { type=t; batsmanId=id; } }
class MilestoneEvent implements MatchEvent { public final String playerId; public final String milestone; public MilestoneEvent(String id, String m) { playerId=id; milestone=m; } }

class EventFactory {
    public static MatchEvent create(String eventType, Map<String, Object> data) {
        return switch (eventType) {
            case "WICKET" -> new WicketEvent(WicketType.BOWLED, (String) data.get("batsman"));
            case "MILESTONE" -> new MilestoneEvent((String) data.get("player"), (String) data.get("desc"));
            default -> throw new IllegalArgumentException("Unknown event");
        };
    }
}

// ================= OBSERVER PATTERN =================
interface MatchSubscriber {
    void onBallUpdate(String matchId, Ball ball);
    void onWicket(String matchId, WicketEvent event);
    void onMatchComplete(String matchId);
}

class EventPublisher {
    private final List<MatchSubscriber> subscribers = new CopyOnWriteArrayList<>();
    public void subscribe(MatchSubscriber s) { subscribers.add(s); }
    public void publishBall(String mId, Ball b) { subscribers.forEach(s -> s.onBallUpdate(mId, b)); }
    public void publishWicket(String mId, WicketEvent w) { subscribers.forEach(s -> s.onWicket(mId, w)); }
    public void publishComplete(String mId) { subscribers.forEach(s -> s.onMatchComplete(mId)); }
}

// ================= SERVICES & MANAGERS =================
class MatchManager {
    private static volatile MatchManager instance;
    private final Map<String, Match> activeMatches = new ConcurrentHashMap<>();
    private final EventPublisher publisher;

    private MatchManager(EventPublisher pub) { this.publisher = pub; }
    public static MatchManager getInstance(EventPublisher p) {
        if (instance == null) { synchronized(MatchManager.class) { if(instance==null) instance = new MatchManager(p); } }
        return instance;
    }
    public void registerMatch(Match m) { activeMatches.put(m.getId(), m); }
    public Match getMatch(String id) { return activeMatches.get(id); }
    public EventPublisher getPublisher() { return publisher; }
}

class ScoreManager {
    private final Map<String, MatchFormatStrategy> strategies;
    public ScoreManager() {
        strategies = Map.of(MatchFormat.T20.name(), new T20FormatStrategy(), MatchFormat.ODI.name(), new ODIFormatStrategy());
    }
    public void processRawFeed(String matchId, Map<String, Object> rawBallData) {
        Match match = MatchManager.getInstance(null).getMatch(matchId); // Simplified lookup
        if (match == null) return;
        
        Ball ball = new Ball(
            (int)rawBallData.get("seq"), (int)rawBallData.get("overNum"),
            (String)rawBallData.get("ballNum"), (String)rawBallData.get("bat"),
            (String)rawBallData.get("bowl"), (int)rawBallData.get("runs"),
            (DeliveryType)rawBallData.get("type"), (String)rawBallData.get("commentary")
        );
        match.processBall(ball);
        publishUpdates(matchId, ball, rawBallData);
    }

    private void publishUpdates(String mId, Ball b, Map<String, Object> raw) {
        MatchManager.getInstance(null).getPublisher().publishBall(mId, b);
        if ("WICKET".equals(raw.get("eventType"))) {
            MatchEvent ev = EventFactory.create("WICKET", Map.of("batsman", b.getBatsmanId()));
            MatchManager.getInstance(null).getPublisher().publishWicket(mId, (WicketEvent)ev);
        }
    }
}

class CommentaryService {
    private final Map<String, List<Commentary>> history = new ConcurrentHashMap<>();
    public void addCommentary(String matchId, String text, int seq) {
        history.computeIfAbsent(matchId, k -> Collections.synchronizedList(new ArrayList<>()))
               .add(new Commentary(matchId, seq, text));
    }
    public List<Commentary> getHistory(String matchId, int limit) {
        List<Commentary> list = history.getOrDefault(matchId, Collections.emptyList());
        return list.subList(Math.max(0, list.size() - limit), list.size());
    }
}

class NotificationService implements MatchSubscriber {
    public void onBallUpdate(String matchId, Ball ball) { /* Push via FCM/WebSocket */ }
    public void onWicket(String matchId, WicketEvent event) { /* Push Wicket Alert */ }
    public void onMatchComplete(String matchId) { /* Push Result Alert */ }
}

// ================= SINGLETON FACADE =================
class CricketSystem {
    private static volatile CricketSystem instance;
    private final MatchManager matchManager;
    private final ScoreManager scoreManager;
    private final CommentaryService commentaryService;
    private final NotificationService notificationService;
    private final EventPublisher publisher;

    private CricketSystem() {
        publisher = new EventPublisher();
        matchManager = MatchManager.getInstance(publisher);
        scoreManager = new ScoreManager();
        commentaryService = new CommentaryService();
        notificationService = new NotificationService();
        publisher.subscribe(notificationService);
    }

    public static CricketSystem getInstance() {
        if (instance == null) { synchronized(CricketSystem.class) { if(instance==null) instance=new CricketSystem(); } }
        return instance;
    }
    public MatchManager getMatchManager() { return matchManager; }
    public ScoreManager getScoreManager() { return scoreManager; }
    public CommentaryService getCommentaryService() { return commentaryService; }
}
```

---

### 4. Relationships & Class Diagram (Textual UML)

```
[CricketSystem] 1 <1..> 1 [MatchManager], [ScoreManager]  (Composition: Singleton orchestration)
[MatchManager] 1 *--> * [Match]                           (Aggregation: Active matches registry)

[Match] 1 <1..> 1 [MatchState]                            (Composition/Context: State Pattern)
[MatchState] <|-- [UpcomingState]                         (Inheritance)
[MatchState] <|-- [LiveState]
[MatchState] <|-- [CompletedState]

[Match] 1 --> 1 [Innings]                                 (Aggregation: Current batting innings)
[Innings] 1 *--> * [Over]                                 (Composition: Overs belong to innings)
[Over] 1 *--> * [Ball]                                    (Composition: Deliveries owned by over)

[MatchEvent] <|-- [WicketEvent]                           (Inheritance: Factory products)
[MatchEvent] <|-- [MilestoneEvent]
[EventFactory] ..> [MatchEvent]                           (Creation)

[MatchManager] 1 --> 1 [EventPublisher]                   (Composition: Observer hub)
[EventPublisher] 1 o--> * [MatchSubscriber]               (Aggregation: Observer Pattern)
[NotificationService] implements [MatchSubscriber]        (Implementation)
```

---

### 5. Design Patterns Used

| Pattern | Implementation in Code |
|---------|------------------------|
| **Singleton** | `CricketSystem` & `MatchManager` use double-checked locking. Guarantees single match registry, unified event routing, and thread-safe state coordination. |
| **Factory** | `EventFactory.create()` decouples raw feed parsing from domain event creation. Easily extensible for `BoundaryEvent`, `PowerplayEvent`, etc. without modifying scoring logic. |
| **Strategy** | `MatchFormatStrategy` encapsulates format-specific rules (`maxOvers`, `powerplayOvers`). `ScoreManager` injects appropriate strategy. Swappable at runtime for league variations. |
| **Observer** | `EventPublisher` + `MatchSubscriber` interface. `NotificationService` subscribes to ball/wicket/completion events. Enables async push delivery, WebSocket broadcasting, and analytics pipelines without tight coupling. |
| **State** | `MatchState` controls lifecycle transitions (`UPCOMING → LIVE → COMPLETED`). Concrete states validate allowed operations (e.g., reject ball recording if match not live). Prevents illegal state jumps. |

---

### 6. Core Flow (Very Important)

#### Match Initialization Flow
1. **Schedule Ingestion**: Admin/Feed API registers match. `Match(id, format, t1, t2)` created.
2. **State Setup**: Initialized as `UpcomingState`. Strategy injected (`T20FormatStrategy`).
3. **Registry**: Stored in `MatchManager`. WebSocket channels provisioned for `match_{id}`.

#### Live Update Flow
1. **Feed Arrival**: Official scorer posts JSON. `ScoreManager.processRawFeed()` invoked.
2. **Domain Mapping**: Raw data mapped to `Ball` object. Sequence number guarantees order.
3. **State Validation**: `match.processBall()` delegates to `LiveState.recordBall()`.
4. **Score Update**: `Innings.recordBall()` updates `totalRuns`, `ballsBowled` atomically. Run rate recalculated.
5. **Event Publishing**: `EventPublisher` broadcasts `Ball` & `Wicket` (if applicable) to subscribers.

#### Score Fetch Flow
1. **Client Request**: HTTP GET or WebSocket subscribe.
2. **Cache Hit**: Redis returns cached `MatchSnapshot` (runs, wickets, RR, partnerships).
3. **Live Sync**: If WebSocket, delta updates pushed. If HTTP, latest state returned with ETag for conditional fetch.

#### Commentary Flow
1. **Text Generation**: Scorer feed includes `commentaryText`. `CommentaryService.addCommentary()` stores in memory/DB.
2. **Publish**: Pushed alongside ball update to frontend.
3. **History**: Client requests `getHistory(matchId, limit)` for pagination. Served from time-sorted index.

#### Notification Flow
1. **Trigger**: `WicketEvent` or `MilestoneEvent` published.
2. **Filter**: `NotificationService` matches event against user preferences (favorite teams/players).
3. **Dispatch**: Push via FCM/APNs, or WebSocket message. Rate-limited to prevent spam (max 1/min per user).

---

### 7. API Design (Conceptual Only)

| Endpoint | Method | Request Payload | Response | Status Codes |
|----------|--------|-----------------|----------|--------------|
| `getLiveScore` | `GET /matches/{id}/live` | Headers: `If-None-Match: etag123` | `{ status: "LIVE", runs: 142, wickets: 3, overs: 15.4, rr: 8.7, partnerships: {...} }` | 200 OK, 304 Not Modified |
| `getCommentary` | `GET /matches/{id}/commentary` | Query: `?limit=20&before=seq_145` | `[ {seq: 150, overNum: 16, ball: "16.2", text: "Short ball, pulled for SIX!", runs: 6} ... ]` | 200 OK, 404 Not Found |
| `getMatchSchedule` | `GET /schedule` | Query: `?date=2026-05-10&type=t20` | `[ {id, t1, t2, venue, startTime, format, status} ]` | 200 OK, 400 Invalid Date |
| `subscribeMatchUpdates` | `WS /ws/matches/{id}` | `{ auth_token, preferences: ["WICKETS", "MILESTONES"] }` | Stream: `{ event: "BALL", payload: {...} }`, `{ event: "WICKET", payload: {...} }` | 101 Switching Protocols, 401 Unauthorized |

---

### 8. Database Schema Design

**Tables & Columns**
- `matches` (`id` PK UUID, `format` VARCHAR, `t1_id` FK, `t2_id` FK, `venue` VARCHAR, `start_time` TIMESTAMP, `status` VARCHAR)
- `innings` (`id` PK UUID, `match_id` FK, `innings_num` INT, `batting_team_id` FK, `bowling_team_id` FK, `total_runs` INT, `total_wickets` INT, `balls_faced` INT)
- `overs` (`id` PK UUID, `innings_id` FK, `over_num` INT, `bowler_id` FK, `runs_conceded` INT, `wickets_taken` INT)
- `balls` (`id` PK BIGINT, `over_id` FK, `sequence` INT, `batsman_id` FK, `bowler_id` FK, `runs` INT, `type` VARCHAR, `commentary` TEXT)
- `commentary_cache` (`match_id` VARCHAR, `seq` INT, `text` TEXT, `timestamp` TIMESTAMP, PRIMARY KEY(match_id, seq))

**Indexing Strategy**
- `balls(over_id, sequence)` → Clustered index. Fast sequential ball retrieval.
- `commentary_cache(match_id, seq DESC)` → Supports `ORDER BY seq DESC LIMIT ?` pagination.
- `innings(match_id, innings_num)` → Direct score lookup.
- `matches(status, start_time)` → Schedule & live match filtering.
- **Time-Series Partitioning**: `balls` & `commentary` partitioned by `match_id` or `date` for TTL archival.

---

### 9. Concurrency & Scalability

- **Event Queue Ingestion**: Kafka topics per match (`topic.match.{id}`). Workers consume in order. Guarantees sequential processing & backpressure handling.
- **State Machine Isolation**: Each match processed by dedicated worker thread/pod. No cross-match locks. Horizontal scaling via consumer groups.
- **Real-Time Push**: Redis Pub/Sub or NATS streams WebSocket gateways. Gateways maintain fan-out connections. Decouples scoring from delivery.
- **Caching Strategy**: Hot scores cached in Redis (`match:{id}:snapshot`, 1s TTL). CDN caches static schedules & player profiles. DB only serves writes & history pagination.
- **Idempotency & Ordering**: Events carry `sequence_number`. Workers reject `seq <= last_processed`. Ensures exactly-once semantics per partition.

---

### 10. Edge Cases

| Scenario | Handling |
|----------|----------|
| **Delayed/Duplicate Events** | Sequence check drops duplicates. Late events logged, applied only if within tolerance window. Admin override available for corrections. |
| **Network Lag (Client)** | WebSocket heartbeats detect stale connections. Server buffers last 50 updates, syncs on reconnect via `last_seq` param. |
| **Match Interruptions (Rain)** | `MatchState` transitions to `DELAYED`. DLS calculator invoked. Overs/targets recalculated via `MatchFormatStrategy` extension. |
| **Incorrect Scorer Input** | Feed allows `CORRECTION` event with `corrects_seq`. System rolls back ball stats, replays corrected delivery, notifies subscribers. |
| **High Traffic Spike (World Cup)** | Auto-scale WebSocket gateways via KEDA. CDN static assets. Read replicas for historical queries. Push notifications throttled to prevent FCM rate limits. |

---

### 11. Sample SQL Queries

**Fetch live match score:**
```sql
SELECT m.id, m.status, i.total_runs, i.total_wickets, 
       (i.total_runs * 6.0 / NULLIF(i.balls_faced, 0)) AS run_rate
FROM matches m
JOIN innings i ON m.id = i.match_id AND i.innings_num = (SELECT MAX(innings_num) FROM innings WHERE match_id = m.id)
WHERE m.id = $1;
```

**Get player stats (career):**
```sql
SELECT p.name, SUM(b.runs) AS total_runs, COUNT(b.id) AS balls_faced,
       SUM(CASE WHEN e.type = 'WICKET' THEN 1 ELSE 0 END) AS wickets
FROM players p
LEFT JOIN balls b ON p.id = b.batsman_id
LEFT JOIN ball_events e ON b.id = e.ball_id AND e.event_type = 'WICKET'
WHERE p.id = $1
GROUP BY p.name;
```

**Retrieve commentary (latest 10):**
```sql
SELECT seq, commentary, timestamp
FROM commentary_cache
WHERE match_id = $1
ORDER BY seq DESC
LIMIT 10;
```

---

### 12. Trade-offs & Extensions

**Limitations of Current Design**
- **In-Memory State in LLD**: `ConcurrentHashMap` & local `EventPublisher` don't survive JVM crashes. Production requires Redis/Kafka for state & events.
- **Synchronous Feed Processing**: Blocks worker if DB write is slow. Async write-behind needed.
- **Single-Sport Schema**: Tables tightly coupled to cricket rules (overs, wickets). Hard to extend for football/basketball without major refactoring.

**Improvements & Extensions**
1. **Multi-Sport Abstraction**: Extract `MatchEvent`, `Scorecard`, `MatchState` into sport-agnostic interfaces. Implement `CricketRules`, `FootballRules` plugins.
2. **ML & AI Insights**: Stream processed balls to real-time feature store. Train models for win probability, next-ball prediction, player form curves. Serve via `/insights` API.
3. **Fantasy League Integration**: Expose `MatchEvent` stream to fantasy engines via gRPC/Webhook. Atomic point calculation & leaderboard updates.
4. **Predictive Prefetching & Caching**: Anticipate high-traffic matches (playoffs). Pre-warm edge caches, provision WebSocket gateways 15 mins before start.
5. **Event Replay & Audit**: Append-only `event_log` table with cryptographic hashes. Enables dispute resolution, compliance audits, and data reconciliation jobs.
