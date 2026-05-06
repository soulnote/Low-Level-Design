# Chat Application


### 1. Requirement Clarification

**Functional Requirements**
- 1:1 and Group messaging with participant management.
- Support text, images, audio, video, and files (up to 100MB).
- Real-time delivery with status tracking: `SENT → DELIVERED → READ`.
- Presence management: online/offline/typing indicators.
- Message history retrieval with pagination & search.
- Group admin controls (add/remove/mute/kick).

**Non-Functional Requirements**
- **Low Latency**: < 50ms for message delivery under 100K concurrent connections/node.
- **Scalability**: Support 10M+ DAU, 1K+ messages/sec per server cluster.
- **High Availability**: Multi-AZ deployment, graceful degradation during broker outages.
- **Reliability**: At-least-once delivery, eventual consistency for receipts, zero message loss.
- **Ordering**: Strict per-conversation message ordering.

**Assumptions**
- Max group size: 512 participants.
- Message retention: 2 years default, configurable per user.
- Client uses WebSockets for real-time; falls back to HTTP long-polling.
- Media stored in S3-compatible object storage; messages store only metadata/URL.
- Single region focus for LLD; multi-region via active-active sync in production.

---

### 2. Core Entities & Responsibilities

| Entity | Responsibility |
|--------|----------------|
| `ChatSystem` | Singleton facade. Routes requests, initializes managers/services, handles cross-cutting concerns. |
| `User` | Represents client identity, presence state, device tokens, connection metadata. |
| `Conversation` | Abstract chat room. Manages participants, message ordering, and lifecycle. |
| `Message` | Abstract payload carrier. Tracks content, metadata, timestamp, and status context. |
| `MessageStatus` | Per-participant delivery state machine. Handles transitions (`SENT → DELIVERED → READ`). |
| `Participant` | Links `User` to `Conversation`. Stores role, notification preferences, last read cursor. |
| `MediaMetadata` | Encapsulates file upload details: S3 path, MIME type, size, hash, encryption key. |
| `PresenceService` | Tracks WebSocket connections, publishes state changes, handles heartbeat. |
| `DeliveryManager` | Routes messages based on online status, manages fan-out, offline queueing. |

---

### 3. Class Design (Java Implementation)

```java
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

// ================= ENUMS =================
enum MessageType { TEXT, IMAGE, VIDEO, AUDIO, FILE }
enum ConversationType { DIRECT, GROUP }
enum MessageDeliveryState { SENT, DELIVERED, READ }
enum ParticipantRole { MEMBER, ADMIN }

// ================= DOMAIN MODELS =================
class User {
    private final String id;
    private final String name;
    private volatile boolean online;
    private String deviceToken; // FCM/APNs

    public User(String id, String name) { this.id = id; this.name = name; }
    public String getId() { return id; }
    public boolean isOnline() { return online; }
    public void setOnline(boolean online) { this.online = online; }
    public String getDeviceToken() { return deviceToken; }
    public void setDeviceToken(String token) { this.deviceToken = token; }
}

class Participant {
    private final User user;
    private final ParticipantRole role;
    private Instant lastReadTime;
    private boolean muted;

    public Participant(User user, ParticipantRole role) {
        this.user = user; this.role = role; this.lastReadTime = Instant.EPOCH;
    }
    public User getUser() { return user; }
    public Instant getLastReadTime() { return lastReadTime; }
    public void markRead(Instant time) { this.lastReadTime = time; }
    public boolean isMuted() { return muted; }
}

abstract class Message {
    protected final String id;
    protected final String conversationId;
    protected final String senderId;
    protected final Instant timestamp;
    protected final long sequenceNumber; // For ordering

    public Message(String id, String conversationId, String senderId, long sequenceNumber) {
        this.id = id; this.conversationId = conversationId;
        this.senderId = senderId; this.timestamp = Instant.now(); this.sequenceNumber = sequenceNumber;
    }
    public String getId() { return id; }
    public String getConversationId() { return conversationId; }
    public String getSenderId() { return senderId; }
    public long getSequenceNumber() { return sequenceNumber; }
    public abstract String getContentSummary();
}

class TextMessage extends Message {
    private final String text;
    public TextMessage(String id, String convId, String sender, long seq, String text) {
        super(id, convId, sender, seq); this.text = text;
    }
    public String getContentSummary() { return text.length() > 20 ? text.substring(0,20)+"..." : text; }
}

class MediaMessage extends Message {
    private final MediaMetadata media;
    public MediaMessage(String id, String convId, String sender, long seq, MediaMetadata media) {
        super(id, convId, sender, seq); this.media = media;
    }
    public String getContentSummary() { return "[Media: " + media.getType() + "]"; }
}

class MediaMetadata {
    private final String url;
    private final String mimeType;
    private final long sizeBytes;
    public MediaMetadata(String url, String mimeType, long size) {
        this.url = url; this.mimeType = mimeType; this.sizeBytes = size;
    }
    public String getType() { return mimeType; }
}

// ================= STATE PATTERN =================
abstract class MessageStatusState {
    protected MessageStatus context;
    public MessageStatusState(MessageStatus ctx) { this.context = ctx; }
    public abstract void deliver();
    public abstract void read();
    protected void transition(MessageStatusState newState) { context.setState(newState); }
}

class SentState extends MessageStatusState {
    public SentState(MessageStatus ctx) { super(ctx); }
    public void deliver() { transition(new DeliveredState(context)); }
    public void read() { transition(new ReadState(context)); } // Skip delivered if instant read
}
class DeliveredState extends MessageStatusState {
    public DeliveredState(MessageStatus ctx) { super(ctx); }
    public void deliver() { /* Already delivered */ }
    public void read() { transition(new ReadState(context)); }
}
class ReadState extends MessageStatusState {
    public ReadState(MessageStatus ctx) { super(ctx); }
    public void deliver() { /* Already past delivered */ }
    public void read() { /* Terminal state */ }
}

class MessageStatus {
    private final String messageId;
    private final String participantId;
    private MessageStatusState currentState;

    public MessageStatus(String messageId, String participantId) {
        this.messageId = messageId; this.participantId = participantId;
        this.currentState = new SentState(this);
    }
    public void setState(MessageStatusState state) { this.currentState = state; }
    public void transitionToDelivered() { currentState.deliver(); }
    public void transitionToRead() { currentState.read(); }
    public String getMessageId() { return messageId; }
    public String getParticipantId() { return participantId; }
}

// ================= CONVERSATION =================
class Conversation {
    private final String id;
    private final ConversationType type;
    private final List<Participant> participants;
    private volatile long sequenceCounter = 0;

    public Conversation(String id, ConversationType type, List<Participant> participants) {
        this.id = id; this.type = type; this.participants = new CopyOnWriteArrayList<>(participants);
    }
    public String getId() { return id; }
    public long nextSequence() { return ++sequenceCounter; }
    public List<Participant> getParticipants() { return participants; }
    public boolean hasParticipant(String userId) {
        return participants.stream().anyMatch(p -> p.getUser().getId().equals(userId));
    }
}

// ================= STRATEGY PATTERN =================
interface DeliveryStrategy {
    void execute(Message msg, List<String> recipientIds);
}

class OnlineDeliveryStrategy implements DeliveryStrategy {
    public void execute(Message msg, List<String> recipientIds) {
        // WebSocket push to connected nodes
        System.out.println("Pushing via WS: " + msg.getId());
    }
}
class OfflineDeliveryStrategy implements DeliveryStrategy {
    public void execute(Message msg, List<String> recipientIds) {
        // Persist to offline queue + FCM push
        System.out.println("Queuing + FCM push: " + msg.getId());
    }
}

// ================= OBSERVER PATTERN =================
interface ChatEventListener {
    void onMessageDelivered(MessageStatus status);
    void onMessageRead(MessageStatus status);
    void onUserStatusChange(String userId, boolean isOnline);
}

class ChatEventPublisher {
    private final List<ChatEventListener> listeners = new CopyOnWriteArrayList<>();
    public void register(ChatEventListener l) { listeners.add(l); }
    public void notifyDelivered(MessageStatus s) { listeners.forEach(l -> l.onMessageDelivered(s)); }
    public void notifyRead(MessageStatus s) { listeners.forEach(l -> l.onMessageRead(s)); }
}

// ================= FACTORY =================
class MessageFactory {
    public static Message create(MessageType type, String id, String convId, String sender, long seq, String payload) {
        return switch (type) {
            case TEXT -> new TextMessage(id, convId, sender, seq, payload);
            default -> {
                MediaMetadata meta = new MediaMetadata("s3://...", "image/jpeg", 1024*500);
                yield new MediaMessage(id, convId, sender, seq, meta);
            }
        };
    }
}

// ================= SERVICES & MANAGERS =================
class MessageStore {
    private final ConcurrentHashMap<String, Message> store = new ConcurrentHashMap<>();
    public void persist(Message msg) { store.put(msg.getId(), msg); }
    public Message get(String id) { return store.get(id); }
}

class PresenceService {
    private final Map<String, Boolean> onlineUsers = new ConcurrentHashMap<>();
    private final ChatEventPublisher publisher;

    public PresenceService(ChatEventPublisher p) { this.publisher = p; }
    public void connect(String userId) { onlineUsers.put(userId, true); publisher.onUserStatusChange(userId, true); }
    public void disconnect(String userId) { onlineUsers.put(userId, false); publisher.onUserStatusChange(userId, false); }
    public boolean isOnline(String userId) { return Boolean.TRUE.equals(onlineUsers.get(userId)); }
}

class DeliveryManager {
    private final PresenceService presence;
    private final DeliveryStrategy onlineStrategy;
    private final DeliveryStrategy offlineStrategy;

    public DeliveryManager(PresenceService ps) {
        this.presence = ps;
        this.onlineStrategy = new OnlineDeliveryStrategy();
        this.offlineStrategy = new OfflineDeliveryStrategy();
    }

    public void dispatch(Message msg, List<String> recipients) {
        List<String> online = new ArrayList<>();
        List<String> offline = new ArrayList<>();
        for (String uid : recipients) {
            if (presence.isOnline(uid)) online.add(uid); else offline.add(uid);
        }
        if (!online.isEmpty()) onlineStrategy.execute(msg, online);
        if (!offline.isEmpty()) offlineStrategy.execute(msg, offline);
    }
}

class ChatService {
    private final MessageStore store;
    private final DeliveryManager deliveryMgr;
    private final ChatEventPublisher eventPublisher;

    public ChatService(MessageStore store, DeliveryManager dm, ChatEventPublisher ep) {
        this.store = store; this.deliveryMgr = dm; this.eventPublisher = ep;
    }

    public Message sendMessage(Conversation conversation, User sender, MessageType type, String payload) {
        if (!conversation.hasParticipant(sender.getId())) throw new SecurityException("Not a participant");
        String msgId = UUID.randomUUID().toString().substring(0, 12);
        long seq = conversation.nextSequence();
        Message msg = MessageFactory.create(type, msgId, conversation.getId(), sender.getId(), seq, payload);
        store.persist(msg);

        // Initialize status for all recipients (except sender)
        List<String> recipients = new ArrayList<>();
        for (Participant p : conversation.getParticipants()) {
            if (!p.getUser().getId().equals(sender.getId())) {
                recipients.add(p.getUser().getId());
            }
        }

        deliveryMgr.dispatch(msg, recipients);
        return msg;
    }

    public void updateStatus(String messageId, String userId, MessageDeliveryState newState) {
        Message msg = store.get(messageId);
        if (msg == null) throw new IllegalArgumentException("Message not found");
        MessageStatus status = new MessageStatus(messageId, userId);
        if (newState == MessageDeliveryState.DELIVERED) {
            status.transitionToDelivered();
            eventPublisher.notifyDelivered(status);
        } else if (newState == MessageDeliveryState.READ) {
            status.transitionToRead();
            eventPublisher.notifyRead(status);
        }
    }
}

// ================= SINGLETON =================
class ChatSystem {
    private static volatile ChatSystem instance;
    private final ChatService chatService;
    private final PresenceService presenceService;
    private final ChatEventPublisher eventPublisher;

    private ChatSystem() {
        eventPublisher = new ChatEventPublisher();
        presenceService = new PresenceService(eventPublisher);
        DeliveryManager deliveryMgr = new DeliveryManager(presenceService);
        chatService = new ChatService(new MessageStore(), deliveryMgr, eventPublisher);
    }

    public static ChatSystem getInstance() {
        if (instance == null) {
            synchronized (ChatSystem.class) {
                if (instance == null) instance = new ChatSystem();
            }
        }
        return instance;
    }

    public ChatService getService() { return chatService; }
    public PresenceService getPresence() { return presenceService; }
}
```

---

### 4. Relationships & Class Diagram (Textual UML)

```
[ChatSystem] 1 <..> 1 [ChatService]               (Composition)
[ChatService] 1 --> 1 [DeliveryManager]           (Composition)
[ChatService] 1 --> 1 [MessageStore]              (Aggregation)

[Conversation] 1 *--> * [Participant]             (Aggregation: Participants can leave/join)
[Conversation] 1 --> * [Message]                  (Composition: Messages lifecycle bound to conversation)

[Participant] 1 --> 1 [User]                      (Association: Participant holds User reference)
[Message] <|-- [TextMessage]                      (Inheritance)
[Message] <|-- [MediaMessage]                     (Inheritance)
[MediaMessage] 1 --> 1 [MediaMetadata]            (Composition)

[MessageStatus] 1 --> 1 [MessageStatusState]      (Composition/State Context)
[MessageStatusState] <|-- [SentState]             (Inheritance)
[MessageStatusState] <|-- [DeliveredState]        (Inheritance)
[MessageStatusState] <|-- [ReadState]             (Inheritance)

[DeliveryManager] --> [DeliveryStrategy]          (Dependency/Strategy Injection)
[ChatEventPublisher] 1 o--> * [ChatEventListener] (Aggregation: Observer Pattern)
```

---

### 5. Design Patterns Used

| Pattern | Implementation in Code |
|---------|------------------------|
| **Singleton** | `ChatSystem.getInstance()` uses double-checked locking. Ensures single system bootstrap and shared service registry across threads. |
| **Factory** | `MessageFactory.create()` abstracts concrete message instantiation based on `MessageType`. Extensible for future types (location, sticker) without modifying service logic. |
| **Strategy** | `DeliveryStrategy` interface decouples online vs offline dispatch. `DeliveryManager` evaluates presence and routes to `OnlineDeliveryStrategy` (WebSocket push) or `OfflineDeliveryStrategy` (queue + FCM). |
| **Observer** | `ChatEventPublisher` maintains `CopyOnWriteArrayList<ChatEventListener>`. Status transitions trigger `notifyDelivered()`/`notifyRead()`. Decouples core messaging from analytics, UI updates, and notification routing. |
| **State** | `MessageStatus` holds a `MessageStatusState` reference. Transitions (`SENT → DELIVERED → READ`) are validated and executed by state classes. Prevents illegal jumps and encapsulates transition side-effects. |

---

### 6. Core Flow (Very Important)

#### Send Message Flow
1. **Client Submit**: User sends payload via WebSocket. Includes `conversationId`, `type`, `payload`, `clientSeq`.
2. **Validation**: `ChatService` verifies sender membership, payload size, and content safety.
3. **Sequence Assignment**: `conversation.nextSequence()` provides monotonic counter for strict ordering.
4. **Creation**: `MessageFactory` builds domain object. Timestamp assigned.
5. **Persistence**: `MessageStore.persist()` writes to primary DB & publishes to Kafka (async).
6. **Dispatch**: `DeliveryManager` evaluates recipient presence. Routes to online (WS) / offline (queue+FCM).
7. **Initial Status**: `MessageStatus` initialized as `SENT` for all recipients. Observer fires.

#### Receive Message Flow
1. **Connection Accept**: Client WebSocket connects. `PresenceService.connect()` marks user online.
2. **Real-time Push**: If online, `OnlineDeliveryStrategy` pushes message payload via WS frame.
3. **Ack**: Client replies with `DELIVERED` ack. Server transitions `MessageStatus → DELIVERED`.
4. **Offline Path**: If disconnected, message stored in pending queue. FCM notification sent.

#### Message Status Update Flow
1. **Trigger**: Client UI renders message or user opens chat.
2. **Update Request**: `markAsRead(messageId)` sent to server.
3. **State Transition**: `ChatService.updateStatus()` invokes `transitionToRead()` on `MessageStatus`.
4. **Notify**: Observer broadcasts to sender's WS connection. Read receipt badge updates.

#### Group Messaging Flow
1. **Fan-out Evaluation**: Iterates `conversation.participants` (excl sender).
2. **Batch Routing**: Groups recipients by connection node (via Redis session registry).
3. **Parallel Dispatch**: Async worker threads push to nodes. Each node handles local fan-out.
4. **Partial Failure Handling**: If node push fails for subset, retry via queue. Idempotent delivery ensures no duplicates.
5. **Status Tracking**: Tracks `DELIVERED` count vs `TOTAL_PARTICIPANTS`. Group read receipt shows partial progress.

#### Offline Message Handling
1. **Detection**: WS heartbeat missed > 30s. `PresenceService` marks offline.
2. **Queueing**: Messages routed to `OfflineDeliveryStrategy` (Kafka topic `offline_messages.user_{id}`).
3. **Reconnect**: Client resumes WS, sends `lastSequence` or `cursor`.
4. **Catch-up**: Server queries DB for `sequence > cursor` and replays. Updates status to `DELIVERED`/`READ` based on local client state.
5. **Ack Cleanup**: Confirmed messages removed from pending queue.

---

### 7. API Design (Conceptual Only)

| Operation | Method | Request Payload | Response | Status Codes |
|-----------|--------|-----------------|----------|--------------|
| `sendMessage` | `POST /api/v1/conversations/{convId}/messages` | `{ type: "TEXT", content: "Hey!", clientSeq: 105, idempotencyKey: "x-105" }` | `{ messageId: "MSG-8A3B", sequence: 105, timestamp: "2026-05-06T14:22:00Z" }` | 201 Created, 403 Forbidden, 409 Duplicate |
| `getMessages` | `GET /api/v1/conversations/{convId}/history` | Query: `?before=cursor&limit=50` | `{ messages: [{id, content, sender, seq, status}], nextCursor: "..." }` | 200 OK, 400 Invalid Cursor |
| `createConversation` | `POST /api/v1/conversations` | `{ type: "GROUP", name: "Dev Team", participants: ["u1", "u2", "u3"], isPrivate: false }` | `{ conversationId: "CONV-9F2A", admin: "me", createdAt: "..." }` | 201 Created, 422 Validation Error |
| `markAsRead` | `POST /api/v1/conversations/{convId}/read` | `{ upToSequence: 210, messageIds: ["MSG-1", "MSG-2"] }` | N/A | 204 No Content, 400 Invalid Range |

*WebSocket endpoints handle real-time push. HTTP used for bootstrapping, history, and admin ops.*

---

### 8. Database Schema Design

**Tables & Columns**
- `users` (`id` PK VARCHAR(36), `username` VARCHAR, `display_name` VARCHAR, `created_at` TIMESTAMP)
- `conversations` (`id` PK VARCHAR(36), `type` ENUM(DIRECT, GROUP), `name` VARCHAR NULL, `created_at` TIMESTAMP, `last_activity_seq` BIGINT)
- `participants` (`conv_id` FK, `user_id` FK, `role` ENUM(MEMBER, ADMIN), `joined_at` TIMESTAMP, PRIMARY KEY(conv_id, user_id))
- `messages` (`id` PK VARCHAR(36), `conv_id` FK, `sender_id` FK, `type` ENUM, `content` TEXT, `media_metadata` JSONB, `sequence` BIGINT, `created_at` TIMESTAMP)
- `message_receipts` (`conv_id`, `message_id` FK, `user_id` FK, `status` ENUM(SENT, DELIVERED, READ), `updated_at` TIMESTAMP, PRIMARY KEY(message_id, user_id))

**Indexing Strategy**
- `messages(conv_id, sequence DESC)` → Clustered index. Enables O(1) pagination & strict ordering.
- `participants(user_id, conv_id)` → Fast user conversation list lookup.
- `message_receipts(user_id, status, updated_at)` → Partial index `WHERE status != 'READ'`. Powers unread count queries.
- `conversations(last_activity_seq DESC)` → Sorting conversations by recent activity without JOIN.
- `messages(sender_id, created_at)` → Search/filter by sender.

---

### 9. Concurrency & Scalability

- **Strict Ordering**: Monotonic `sequence` per conversation assigned via DB sequence or Redis `INCR`. Kafka topic partitioned by `conversation_id` guarantees ordered consumption.
- **Idempotency**: `clientSeq` + `idempotencyKey` deduplicates at ingestion layer. DB unique constraint on `(conv_id, clientSeq)`.
- **Horizontal Scaling**: Chat servers are stateless for message processing. WebSocket connections tracked in Redis (`CONV:SESSION_MAP`). Cross-node routing via Redis Pub/Sub or gRPC fan-out.
- **Read/Write Splitting**: Write path → Primary DB. Read path → Read replicas + Redis cache for hot conversations & last read cursors.
- **Sticky vs Stateless**: WebSockets are stateful (TCP connection). Use connection gateway pattern: Gateway holds WS state, forwards messages to worker pool via internal queue. Enables zero-downtime gateway rolling updates.

---

### 10. Edge Cases

| Scenario | Handling |
|----------|----------|
| **Message Duplication** | Idempotency key enforced. Consumer marks message as processed. Client displays once via `messageId` dedup. |
| **Out-of-Order Delivery** | Client buffers by `sequence`. Server rejects `seq <= lastAck`. Gap detection triggers fetch-from-DB. |
| **Offline Users** | Pending queue with TTL. FCM fallback. Reconnection triggers delta sync. No message loss. |
| **Large Group Failure** | Chunk fan-out into batches of 50. Process async. Partial success logged. Retry failed chunks. Sender sees "Sending..." until ACK majority. |
| **Media Upload Failure** | Client uploads to S3 directly (pre-signed URL). Message created with `MEDIA_PENDING` status. If S3 upload fails, client retries or sends fallback text. |

---

### 11. Sample SQL Queries

**Fetch chat history (cursor-based pagination):**
```sql
SELECT m.id, m.sender_id, m.type, m.content, m.sequence, m.created_at,
       mr.status AS my_receipt_status
FROM messages m
LEFT JOIN message_receipts mr ON m.id = mr.message_id AND mr.user_id = $1
WHERE m.conversation_id = $2 AND m.sequence < $3
ORDER BY m.sequence DESC
LIMIT 50;
```

**Get unread message count per conversation for a user:**
```sql
SELECT p.conversation_id, COUNT(m.id) AS unread_count
FROM participants p
JOIN message_receipts mr ON p.conversation_id = mr.conversation_id
JOIN messages m ON mr.message_id = m.id
WHERE p.user_id = $1 
  AND mr.status = 'SENT' 
  AND m.sender_id != $1
GROUP BY p.conversation_id;
```

**Get user's active conversations sorted by last activity:**
```sql
SELECT c.id, c.type, c.name, c.last_activity_seq
FROM conversations c
JOIN participants p ON c.id = p.conversation_id
WHERE p.user_id = $1
ORDER BY c.last_activity_seq DESC;
```

---

### 12. Trade-offs & Extensions

**Limitations of Current Design**
- **In-Memory Store for LLD**: `ConcurrentHashMap` doesn't survive crashes. Production requires PostgreSQL/Cassandra + Kafka.
- **Stateful WebSocket Scaling**: Requires connection registry & routing layer. Complex to manage during node failures.
- **No E2E Encryption**: Server decrypts/stores plaintext. Adds compliance & privacy overhead.

**Improvements & Extensions**
1. **End-to-End Encryption**: Integrate Signal Protocol (Double Ratchet). Clients exchange keys. Server stores only ciphertext & metadata. Requires client-side decryption, breaking server-side search.
2. **Typing Indicators**: Ephemeral state published via Redis Pub/Sub (`typing:{conv_id}:{user_id}`). 3s TTL auto-expiration. Low overhead, high real-time.
3. **Message Reactions & Threads**: Extend `Message` with child `Reactions` table. Threads modeled as `parent_message_id`. Fan-out optimized via materialized view.
4. **Optimized Read Receipts**: Instead of instant DB writes, batch receipts in client (e.g., every 2s or on background). Server aggregates and updates in bulk. Reduces write amplification by 70%.
5. **Push Notification Deduplication**: FCM/APNs collapse key prevents notification spam. Local client sync resolves final state.
6. **Global Multi-Region Active-Active**: Geo-partition conversations by `conv_id hash`. Cross-region sync via CDC (Debezium) + Kafka. Conflict resolution via Lamport timestamps. Reduces P99 latency for global users.
