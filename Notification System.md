# Notification System

### 1. Requirement Clarification

**Functional Requirements**
- Multi-channel dispatch: Email, SMS, Push (FCM/APNs)
- Template rendering with placeholder substitution (`{name}`, `{otp}`, `{url}`)
- Bulk & single notifications with priority routing
- User preference management: opt-in/out per channel, quiet hours, frequency capping
- Scheduling: defer delivery to future timestamps
- Retry mechanism: configurable backoff on provider failure
- Delivery tracking & audit logging

**Non-Functional Requirements**
- High throughput: 50K–100K messages/sec horizontally scalable
- Low latency: < 1s end-to-end for real-time/urgent notifications
- Reliability: At-least-once delivery with idempotent provider calls
- Fault tolerance: graceful degradation, circuit breakers, dead-letter queues
- Observability: metrics (success/failure rates, latency, queue depth)

**Assumptions**
- External providers expose standard SDKs/REST APIs (e.g., SES, Twilio, Firebase)
- Message payload ≤ 4KB for push, ≤ 1MB for email/SMS
- Retry policy: exponential backoff (3s, 6s, 12s, 30s, 60s), max 5 attempts
- Idempotency enforced via `idempotency_key` + provider deduplication
- Single service deployment for LLD; production assumes distributed queue & worker fleet

---

### 2. Core Entities & Responsibilities

| Entity | Responsibility |
|--------|----------------|
| `NotificationRequest` | Input DTO carrying recipient ID, channel, template ID, payload, priority, schedule time. |
| `UserPreference` | Stores opt-in status per channel, frequency limits, quiet hours, timezone. |
| `Notification` | Domain entity representing a dispatch instance. Tracks lifecycle, rendered content, metadata. |
| `Template` | Reusable message schema with variables and fallback content. |
| `NotificationChannel` (Interface) | Contract for sending via specific medium. Implementations handle provider SDK integration. |
| `NotificationLog` | Immutable audit record of each attempt: timestamp, provider response, latency, status. |
| `RetryPolicy` | Defines attempt count, backoff multiplier, and failure thresholds. |
| `Scheduler` | Evaluates `scheduled_time <= now()` and enqueues pending notifications. |
| `NotificationService` | Orchestrator: validates requests, applies preferences, renders templates, dispatches via channels. |
| `QueueManager` | Manages in-flight notifications, partitions by priority, tracks concurrency. |

---

### 3. Class Design (Java Implementation)

```java
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

// ================= ENUMS =================
enum ChannelType { EMAIL, SMS, PUSH }
enum NotificationStatus { PENDING, PROCESSING, SENT, DELIVERED, FAILED, DROPPED, CANCELLED }
enum Priority { LOW, MEDIUM, HIGH, URGENT }
enum RetryStrategy { FIXED_DELAY, EXPONENTIAL_BACKOFF }

// ================= DOMAIN MODELS =================
class NotificationRequest {
    private final String requestId;
    private final String userId;
    private final String idempotencyKey;
    private final Set<ChannelType> channels;
    private final String templateId;
    private final Map<String, String> payload;
    private final Priority priority;
    private final LocalDateTime scheduleTime;

    public NotificationRequest(String requestId, String userId, String idempotencyKey,
                               Set<ChannelType> channels, String templateId,
                               Map<String, String> payload, Priority priority, LocalDateTime scheduleTime) {
        this.requestId = requestId; this.userId = userId; this.idempotencyKey = idempotencyKey;
        this.channels = channels; this.templateId = templateId; this.payload = payload;
        this.priority = priority; this.scheduleTime = scheduleTime;
    }
    public String getRequestId() { return requestId; }
    public String getUserId() { return userId; }
    public String getIdempotencyKey() { return idempotencyKey; }
    public Set<ChannelType> getChannels() { return channels; }
    public String getTemplateId() { return templateId; }
    public Map<String, String> getPayload() { return payload; }
    public Priority getPriority() { return priority; }
    public LocalDateTime getScheduleTime() { return scheduleTime != null ? scheduleTime : LocalDateTime.now(); }
}

class UserPreference {
    private final String userId;
    private final Map<ChannelType, Boolean> channelOptIns;
    private final int maxDailyNotifications;
    private final int[] quietHours; // [startHour, endHour]

    public UserPreference(String userId) {
        this.userId = userId;
        this.channelOptIns = new EnumMap<>(ChannelType.class);
        this.channelOptIns.putAll(Map.of(ChannelType.EMAIL, true, ChannelType.SMS, true, ChannelType.PUSH, true));
        this.maxDailyNotifications = 100;
        this.quietHours = new int[]{22, 7};
    }
    public boolean isAllowed(ChannelType channel, LocalDateTime time) {
        if (!Boolean.TRUE.equals(channelOptIns.get(channel))) return false;
        int hour = time.getHour();
        return !(hour >= quietHours[0] && hour < quietHours[1]);
    }
    public String getUserId() { return userId; }
}

class Template {
    private final String id;
    private final String subject; // For email
    private final String body;
    private final Set<String> requiredPlaceholders;

    public Template(String id, String subject, String body) {
        this.id = id; this.subject = subject; this.body = body;
        this.requiredPlaceholders = extractPlaceholders(body + (subject != null ? subject : ""));
    }
    private Set<String> extractPlaceholders(String text) {
        // Simplified regex extraction for LLD
        return new HashSet<>(); // Placeholder logic
    }
    public String getId() { return id; }
    public String render(Map<String, String> data) { return renderTemplate(body, data); }
    public String renderSubject(Map<String, String> data) {
        return subject != null ? renderTemplate(subject, data) : "";
    }
    private String renderTemplate(String template, Map<String, String> data) {
        String res = template;
        for (Map.Entry<String, String> e : data.entrySet()) {
            res = res.replace("{" + e.getKey() + "}", e.getValue());
        }
        return res;
    }
}

class Notification {
    private final String id;
    private final String userId;
    private final ChannelType channel;
    private final String renderedContent;
    private final Priority priority;
    private volatile NotificationStatus status;
    private int retryCount;
    private LocalDateTime nextAttempt;

    public Notification(String id, String userId, ChannelType channel, String content, Priority priority) {
        this.id = id; this.userId = userId; this.channel = channel;
        this.renderedContent = content; this.priority = priority;
        this.status = NotificationStatus.PENDING;
        this.retryCount = 0;
        this.nextAttempt = LocalDateTime.now();
    }
    public String getId() { return id; }
    public String getUserId() { return userId; }
    public ChannelType getChannel() { return channel; }
    public String getContent() { return renderedContent; }
    public Priority getPriority() { return priority; }
    public NotificationStatus getStatus() { return status; }
    public void setStatus(NotificationStatus s) { this.status = s; }
    public int getRetryCount() { return retryCount; }
    public void incrementRetry() { this.retryCount++; this.nextAttempt = LocalDateTime.now(); }
    public LocalDateTime getNextAttempt() { return nextAttempt; }
}

class NotificationLog {
    private final String logId;
    private final String notificationId;
    private final LocalDateTime timestamp;
    private final NotificationStatus status;
    private final String providerResponse;
    private final long latencyMs;

    public NotificationLog(String logId, String notificationId, NotificationStatus status, String response, long latencyMs) {
        this.logId = logId; this.notificationId = notificationId; this.timestamp = LocalDateTime.now();
        this.status = status; this.providerResponse = response; this.latencyMs = latencyMs;
    }
    public String getNotificationId() { return notificationId; }
    public NotificationStatus getStatus() { return status; }
}

// ================= STRATEGY & TEMPLATE METHOD =================
interface RetryStrategy {
    long calculateDelay(int attempt);
}

class ExponentialBackoff implements RetryStrategy {
    private final long baseDelayMs;
    public ExponentialBackoff(long baseDelayMs) { this.baseDelayMs = baseDelayMs; }
    public long calculateDelay(int attempt) { return baseDelayMs * (long) Math.pow(2, attempt); }
}

// Abstract Channel using Template Method Pattern
abstract class NotificationChannel {
    protected final ChannelType type;
    protected final RetryStrategy retryStrategy;

    public NotificationChannel(ChannelType type, RetryStrategy retryStrategy) {
        this.type = type; this.retryStrategy = retryStrategy;
    }

    // Template Method
    public final void dispatch(Notification notification) {
        if (!validate(notification)) {
            notification.setStatus(NotificationStatus.FAILED);
            return;
        }
        try {
            String response = sendToProvider(notification);
            handleSuccess(notification, response);
        } catch (Exception e) {
            handleFailure(notification, e);
        }
    }

    protected abstract boolean validate(Notification notification);
    protected abstract String sendToProvider(Notification notification);
    protected void handleSuccess(Notification n, String res) { n.setStatus(NotificationStatus.SENT); }
    protected void handleFailure(Notification n, Exception e) {
        n.incrementRetry();
        n.setStatus(n.getRetryCount() >= 5 ? NotificationStatus.DROPPED : NotificationStatus.FAILED);
    }

    public ChannelType getType() { return type; }
    public RetryStrategy getRetryStrategy() { return retryStrategy; }
}

class EmailChannel extends NotificationChannel {
    public EmailChannel() { super(ChannelType.EMAIL, new ExponentialBackoff(3000)); }
    protected boolean validate(Notification n) { return n.getContent().length() <= 1_000_000; }
    protected String sendToProvider(Notification n) { return "SMTP_OK_250"; } // Mock
}

class SmsChannel extends NotificationChannel {
    public SmsChannel() { super(ChannelType.SMS, new ExponentialBackoff(5000)); }
    protected boolean validate(Notification n) { return n.getContent().length() <= 160 * 6; }
    protected String sendToProvider(Notification n) { return "TWILIO_SENT_200"; } // Mock
}

class PushChannel extends NotificationChannel {
    public PushChannel() { super(ChannelType.PUSH, new ExponentialBackoff(2000)); }
    protected boolean validate(Notification n) { return n.getContent().length() <= 4096; }
    protected String sendToProvider(Notification n) { return "FCM_ACCEPTED_200"; } // Mock
}

// ================= OBSERVER =================
interface NotificationEventListener {
    void onStatusChange(NotificationLog log);
}

class NotificationEventPublisher {
    private final List<NotificationEventListener> listeners = new CopyOnWriteArrayList<>();
    public void subscribe(NotificationEventListener l) { listeners.add(l); }
    public void publish(NotificationLog log) {
        for (NotificationEventListener l : listeners) l.onStatusChange(log);
    }
}

// ================= SERVICES & MANAGERS =================
class TemplateService {
    private final Map<String, Template> templates = new ConcurrentHashMap<>();
    public TemplateService() { templates.put("WELCOME_01", new Template("WELCOME_01", "Welcome", "Hi {name}, welcome!")); }
    public Template get(String id) { return templates.get(id); }
}

class PreferenceService {
    private final Map<String, UserPreference> prefs = new ConcurrentHashMap<>();
    public UserPreference get(String userId) { return prefs.computeIfAbsent(userId, UserPreference::new); }
}

class ChannelFactory {
    public static NotificationChannel getChannel(ChannelType type) {
        return switch (type) {
            case EMAIL -> new EmailChannel();
            case SMS -> new SmsChannel();
            case PUSH -> new PushChannel();
        };
    }
}

// Singleton Queue Manager
class QueueManager {
    private static volatile QueueManager instance;
    private final PriorityBlockingQueue<Notification> queue;
    private final ReentrantLock enqueueLock = new ReentrantLock();

    private QueueManager() { queue = new PriorityBlockingQueue<>(1000, Comparator.comparingInt(n -> n.getPriority().ordinal())); }
    public static QueueManager getInstance() {
        if (instance == null) { synchronized(QueueManager.class) { if(instance==null) instance = new QueueManager(); } }
        return instance;
    }
    public void enqueue(Notification n) { enqueueLock.lock(); try { queue.add(n); } finally { enqueueLock.unlock(); } }
    public Notification dequeue() { return queue.poll(); }
    public boolean isEmpty() { return queue.isEmpty(); }
}

class NotificationService {
    private final TemplateService templateService;
    private final PreferenceService preferenceService;
    private final NotificationEventPublisher eventPublisher;
    private final Map<String, Boolean> idempotencyCache = new ConcurrentHashMap<>();
    private final ExecutorService workerPool = Executors.newFixedThreadPool(8);

    public NotificationService(TemplateService ts, PreferenceService ps, NotificationEventPublisher ep) {
        this.templateService = ts; this.preferenceService = ps; this.eventPublisher = ep;
        startWorkerThread();
    }

    public Notification submit(NotificationRequest req) {
        // 1. Idempotency Check
        if (idempotencyCache.containsKey(req.getIdempotencyKey())) {
            throw new IllegalArgumentException("Duplicate request detected.");
        }
        idempotencyCache.put(req.getIdempotencyKey(), true);

        // 2. Template Resolution & Render
        Template tmpl = templateService.get(req.getTemplateId());
        if (tmpl == null) throw new IllegalArgumentException("Template not found.");

        // 3. Preference Filtering
        UserPreference userPref = preferenceService.get(req.getUserId());
        Set<ChannelType> allowedChannels = new HashSet<>();
        for (ChannelType ch : req.getChannels()) {
            if (userPref.isAllowed(ch, req.getScheduleTime())) allowedChannels.add(ch);
        }
        if (allowedChannels.isEmpty()) throw new SecurityException("No allowed channels for user.");

        // 4. Create Notification(s)
        String content = tmpl.render(req.getPayload());
        ChannelType selectedChannel = resolveChannel(allowedChannels, req.getPriority());
        Notification notification = new Notification(
                "NOTIF-" + UUID.randomUUID().toString().substring(0, 8),
                req.getUserId(), selectedChannel, content, req.getPriority());
        
        // 5. Schedule or Enqueue
        if (!LocalDateTime.now().isBefore(req.getScheduleTime())) {
            QueueManager.getInstance().enqueue(notification);
        } else {
            // In production: use Quartz/Scheduler DB. Mock delayed enqueue.
            scheduleDelayed(notification, req.getScheduleTime());
        }
        return notification;
    }

    private ChannelType resolveChannel(Set<ChannelType> allowed, Priority p) {
        // Priority routing logic
        if (p == Priority.URGENT && allowed.contains(ChannelType.SMS)) return ChannelType.SMS;
        if (allowed.contains(ChannelType.EMAIL)) return ChannelType.EMAIL;
        return allowed.iterator().next();
    }

    private void scheduleDelayed(Notification n, LocalDateTime time) {
        long delayMs = ChronoUnit.MILLIS.between(LocalDateTime.now(), time);
        CompletableFuture.delayedExecutor(delayMs, TimeUnit.MILLISECONDS)
            .execute(() -> QueueManager.getInstance().enqueue(n));
    }

    private void startWorkerThread() {
        new Thread(() -> {
            while (true) {
                try {
                    Notification notif = QueueManager.getInstance().dequeue();
                    if (notif == null) { Thread.sleep(100); continue; }
                    workerPool.submit(() -> processNotification(notif));
                } catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
            }
        }, "NotificationDispatcher").start();
    }

    private void processNotification(Notification notif) {
        notif.setStatus(NotificationStatus.PROCESSING);
        NotificationChannel channel = ChannelFactory.getChannel(notif.getChannel());
        long start = System.currentTimeMillis();
        try {
            channel.dispatch(notif);
            logAndPublish(notif, notif.getStatus(), "OK", System.currentTimeMillis() - start);
            // If failed but retryable, re-enqueue
            if (notif.getStatus() == NotificationStatus.FAILED) {
                Thread.sleep(channel.getRetryStrategy().calculateDelay(notif.getRetryCount()));
                QueueManager.getInstance().enqueue(notif);
            }
        } catch (Exception e) {
            logAndPublish(notif, NotificationStatus.FAILED, e.getMessage(), System.currentTimeMillis() - start);
            if (notif.getRetryCount() < 5) QueueManager.getInstance().enqueue(notif);
        }
    }

    private void logAndPublish(Notification n, NotificationStatus status, String response, long latency) {
        NotificationLog log = new NotificationLog(UUID.randomUUID().toString(), n.getId(), status, response, latency);
        eventPublisher.publish(log);
    }
}
```

---

### 4. Relationships & Class Diagram (Textual UML)

```
[NotificationService] 1 -- 1 [TemplateService]      (Aggregation)
[NotificationService] 1 -- 1 [PreferenceService]    (Aggregation)
[NotificationService] 1 -- 1 [NotificationEventPublisher] (Composition)
[NotificationService] 1 --> 1 [QueueManager]        (Dependency: Singleton)

[NotificationChannel] <|-- [EmailChannel]           (Inheritance: Template Method)
[NotificationChannel] <|-- [SmsChannel]
[NotificationChannel] <|-- [PushChannel]

[ChannelFactory] ..> [NotificationChannel]          (Creation: Factory)
[NotificationChannel] 1 --> 1 [RetryStrategy]       (Dependency Injection: Strategy)

[Notification] 1 *--> 1 [NotificationLog]           (Composition: Logs belong to notification lifecycle)
[Notification] 1 --> 1 [NotificationEventPublisher] (Observer Pattern: Publishes status changes)

[User] 1 o--> * [UserPreference]                    (Aggregation: Preferences owned by user)
[NotificationRequest] 1 --> 1 [Notification]        (Creation/Mapping)
```

---

### 5. Design Patterns Used

| Pattern | Implementation in Code |
|---------|------------------------|
| **Factory** | `ChannelFactory.getChannel(type)` decouples concrete channel instantiation from the dispatcher. Enables adding `WhatsAppChannel` without modifying service logic. |
| **Strategy** | `RetryStrategy` interface (`FixedDelay` vs `ExponentialBackoff`). Injected into channels. Enables per-channel tuning based on provider SLAs. |
| **Template Method** | `AbstractNotificationChannel.dispatch()` defines the skeleton: `validate()` → `sendToProvider()` → `handleSuccess()/handleFailure()`. Subclasses override provider-specific logic. Enforces consistent dispatch flow. |
| **Observer** | `NotificationEventPublisher` maintains listeners. `NotificationService` publishes `NotificationLog` on every state change. Enables async analytics, alerting, and webhook callbacks without coupling domain to consumers. |
| **Singleton** | `QueueManager` ensures single shared priority queue instance across dispatcher threads. Double-checked locking guarantees thread-safe lazy initialization. |

---

### 6. Core Flow (Very Important)

#### Notification Send Flow
1. **Receive Request**: `NotificationService.submit(req)` invoked.
2. **Idempotency Check**: `idempotencyKey` verified in cache/DB. Reject duplicates.
3. **Template Resolution**: `TemplateService` fetches schema, substitutes `{placeholders}` with payload.
4. **Preference Evaluation**: `PreferenceService` checks opt-ins, quiet hours, daily caps. Filters allowed channels.
5. **Channel Selection**: `resolveChannel()` picks best medium based on priority & availability (e.g., URGENT → SMS if allowed, else Email/Push).
6. **Entity Creation**: `Notification` object created with `PENDING` status, rendered content, and metadata.
7. **Queue Routing**: If `scheduleTime > now()`, deferred via scheduler. Otherwise, enqueued to `QueueManager` (Priority Queue).
8. **Dispatch Worker**: Background thread dequeues, creates `Channel` via factory, calls `dispatch()`, updates status.
9. **Audit & Publish**: `NotificationLog` generated, published to observers for downstream tracking.

#### Retry Flow
1. **Failure Detection**: `sendToProvider()` throws exception or returns error code.
2. **State Update**: `handleFailure()` increments `retryCount`. Checks against max (5).
3. **Policy Application**: `RetryStrategy.calculateDelay(attempt)` computes wait time (e.g., 3s, 6s, 12s).
4. **Re-queue**: Notification re-enqueued to `QueueManager` with updated `nextAttempt`.
5. **Exhaustion**: If max retries reached, status transitions to `DROPPED`, routed to Dead Letter Queue (DLQ), alert fired.

#### Scheduled Notification Flow
1. **Validation & Render**: Same as send flow. `scheduleTime` extracted from request.
2. **Deferred Enqueue**: If future timestamp, notification stored in scheduling store (Redis sorted set / Quartz DB) with score = epoch time.
3. **Trigger Poll**: Scheduler thread evaluates `now() >= score`, moves to active `QueueManager`.
4. **Execution**: Dispatched like real-time notifications. Late scheduling tolerance configured (e.g., ±5 min).

#### Preference Handling Flow
1. **Fetch User Prefs**: `UserPreference` loaded from cache/DB.
2. **Filter Channels**: Iterate requested channels. Skip if `optIn == false` OR current time falls in `quietHours`.
3. **Frequency Cap**: Check daily sent count. If exceeded, queue delayed until cap resets or downgrade channel.
4. **Fallback Routing**: If primary channel blocked by prefs, automatically fallback to next allowed channel in priority list.
5. **Audit**: Dropped/skipped attempts logged with reason `OPT_OUT` or `QUIET_HOURS` for transparency.

---

### 7. API Design (Conceptual Only)

| Endpoint | Method | Request Payload | Response | Status Codes |
|----------|--------|-----------------|----------|--------------|
| `/notifications/send` | `POST` | `{ userId, idempotencyKey, channels: ["EMAIL"], templateId, payload: {"name":"John"}, priority: "HIGH" }` | `{ notificationId: "NOTIF-8X2A", status: "ACCEPTED", estimatedDelivery: "2026-05-06T10:15:05Z" }` | 202 Accepted, 400 Validation Error, 409 Duplicate, 403 Opt-Out Blocked |
| `/notifications/schedule` | `POST` | `{ ...request fields..., scheduleTime: "2026-05-07T09:00:00Z" }` | `{ notificationId: "NOTIF-9K3B", scheduledFor: "2026-05-07T09:00:00Z", status: "SCHEDULED" }` | 201 Created, 422 Invalid Schedule |
| `/notifications/{id}/status` | `GET` | N/A | `{ notificationId: "NOTIF-8X2A", status: "DELIVERED", channel: "EMAIL", logs: [{status, timestamp, latencyMs}] }` | 200 OK, 404 Not Found |
| `/users/{userId}/preferences` | `PUT` | `{ channels: {"SMS": true, "PUSH": false}, quietHours: [23, 6] }` | N/A | 204 No Content, 400 Invalid Format |

*Note: All endpoints are idempotent where applicable. `idempotencyKey` required in headers for retries.*

---

### 8. Database Schema Design

**Tables & Columns**
- `users` (`id` PK UUID, `email` VARCHAR, `phone` VARCHAR UNIQUE, `timezone` VARCHAR, `created_at` TIMESTAMP)
- `preferences` (`id` PK UUID, `user_id` FK UNIQUE, `email_opt_in` BOOLEAN, `sms_opt_in` BOOLEAN, `push_opt_in` BOOLEAN, `daily_cap` INT DEFAULT 100, `quiet_hours_start` INT, `quiet_hours_end` INT)
- `templates` (`id` PK VARCHAR, `name` VARCHAR, `subject_template` TEXT, `body_template` TEXT, `version` INT DEFAULT 1, `updated_at` TIMESTAMP)
- `notifications` (`id` PK UUID, `user_id` FK, `channel` ENUM, `status` ENUM, `priority` ENUM, `rendered_content` TEXT, `created_at` TIMESTAMP, `scheduled_at` TIMESTAMP NULL, `delivered_at` TIMESTAMP NULL)
- `notification_logs` (`id` PK UUID, `notification_id` FK, `attempt` INT, `status` ENUM, `provider_response` TEXT, `latency_ms` INT, `created_at` TIMESTAMP)
- `idempotency_keys` (`key` PK VARCHAR UNIQUE, `notification_id` FK, `expires_at` TIMESTAMP)

**Indexing Strategy**
- `notifications(user_id, created_at DESC)` → Fast history fetch per user.
- `notifications(status, scheduled_at)` → Efficient scheduler polling (`WHERE status='PENDING' AND scheduled_at <= NOW()`).
- `notification_logs(notification_id, attempt DESC)` → Quick retry debugging.
- `idempotency_keys(key)` with TTL index → Auto-cleanup after 7 days.
- `notifications(priority, created_at)` → Queue prioritization fallback.

---

### 9. Concurrency & Scalability

- **Queue Partitioning**: Split `QueueManager` by tenant/channel/priority using Kafka/RabbitMQ partitions. Workers scale horizontally per partition.
- **Thread Pool Isolation**: Separate worker pools for real-time vs bulk/scheduled notifications to prevent head-of-line blocking.
- **Idempotency Enforcement**: `idempotency_keys` table with unique constraint. DB-level deduplication before insert. Provider APIs called with `Idempotency-Key` header.
- **Rate Limiting**: Redis `RATELIMIT:provider:email` token bucket per minute. Workers check token before dispatch. If exhausted, backpressure triggers delayed requeue.
- **Distributed Locking**: Redis `SETNX` for template version swaps and preference cache refreshes to prevent stale routing.
- **At-Least-Once Guarantee**: DB transaction commits notification as `SENT` only after provider ACK. If crash, unacknowledged items replayed from DLQ or `FAILED` state workers.

---

### 10. Edge Cases

| Scenario | Handling |
|----------|----------|
| **External Provider Failure** | Circuit breaker opens after 50% failures/30s. Routes traffic to fallback channel or queues. Alerts Ops. |
| **Duplicate Notifications** | `idempotencyKey` checked upfront. DB unique constraint rejects duplicates. Provider headers prevent double charge/send. |
| **Invalid Contact Details** | Provider returns `400/404`. Bounce webhook updates `UserPreference` channel opt-in to false. Notification marked `DROPPED`. |
| **Template Render Failure** | Missing placeholders caught in `validate()`. Fallback template `DEFAULT_FALLBACK` used. Logged as `WARN`. |
| **Retry Exhaustion** | After 5 attempts, status → `DROPPED`. Notification routed to DLQ. Admin UI allows manual re-trigger or compensation. |

---

### 11. Sample Queries

**Fetch pending notifications for scheduling/dispatch:**
```sql
SELECT id, user_id, channel, priority, scheduled_at 
FROM notifications 
WHERE status = 'PENDING' 
  AND (scheduled_at IS NULL OR scheduled_at <= NOW())
ORDER BY priority DESC, scheduled_at ASC
LIMIT 1000 FOR UPDATE SKIP LOCKED;
```

**Fetch notification history for a user:**
```sql
SELECT n.id, n.channel, n.status, n.created_at, 
       l.attempt, l.latency_ms, l.provider_response
FROM notifications n
LEFT JOIN notification_logs l ON n.id = l.notification_id
WHERE n.user_id = 'usr_123'
ORDER BY n.created_at DESC, l.attempt DESC;
```

**Count failed notifications per channel (last 24h):**
```sql
SELECT channel, COUNT(*) AS failure_count
FROM notifications
WHERE status IN ('FAILED', 'DROPPED')
  AND created_at >= NOW() - INTERVAL '24 hours'
GROUP BY channel;
```

---

### 12. Trade-offs & Extensions

**Limitations of Current Design**
- In-memory queue & cache in LLD won't survive process restarts. Production requires persistent message brokers (Kafka/Pulsar) and Redis.
- `ChannelFactory` creates new instances per dispatch. Connection pooling for SMTP/Twilio/FCM SDKs is abstracted but required in prod.
- Single-region scheduling assumes monolithic time sync. Multi-region requires NTP/Chrony and distributed schedulers.

**Improvements & Extensions**
1. **Multi-Region Active-Active**: Deploy per-region with geo-routed Kafka. Cross-region replication for preferences/templates. Conflict-free preference merging using CRDTs.
2. **Dynamic Priority Queues**: Upgrade to Kafka with separate topics per priority (`topic.urgent`, `topic.normal`). Consumers auto-scale via KEDA/HPA based on lag metrics.
3. **Real-Time Analytics Dashboard**: Stream logs to ClickHouse/Druid via Flink. Expose P95 latency, delivery rate, bounce rate, opt-in trends. Power alerting thresholds.
4. **ML-Driven Send Time Optimization**: Analyze historical engagement per user. Inject `SmartSchedulerStrategy` to pick optimal delivery window, boosting open rates by 15-30%.
5. **Circuit Breaker & Fallback Routing**: Integrate Resilience4j patterns. If Email provider SLA drops below 99%, automatically reroute to Push/SMS with user consent.
6. **Bulk Optimization**: Chunk bulk requests into batches. Use provider batch APIs (e.g., SES `SendBulkTemplatedEmail`, FCM multicast). Reduces API calls by 10-50x.

This design prioritizes extensible OOP boundaries, explicit pattern application, robust state management, and clear dispatch workflows. It aligns with SDE2 expectations for production-grade messaging systems while remaining framework-agnostic and interview-ready.
