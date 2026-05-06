# Ticket Booking System
---

### 1. Requirement Clarification

**Functional Requirements**
- Search shows/events by city, venue, date, and category.
- Fetch interactive seat layout with real-time availability.
- Temporary seat reservation (lock) with configurable TTL (e.g., 5 mins).
- Complete booking: validate locks → process payment → confirm seats.
- Cancel booking with refund rules based on event policy.
- Track booking & payment history.

**Non-Functional Requirements**
- **High Concurrency**: Handle flash sales (100K+ concurrent users targeting same show).
- **Strong Consistency**: Zero double-booking under any race condition.
- **Low Latency**: Seat availability check < 20ms. Lock/confirm < 100ms.
- **High Availability**: 99.99% uptime; graceful degradation during peak traffic.

**Assumptions**
- Fixed seat map per screen; seats categorized by `VIP`, `PREMIUM`, `REGULAR`.
- Lock timeout: 5 minutes. Auto-release on expiry.
- Single payment attempt per booking; synchronous confirmation for LLD (async in prod).
- No seat swapping or adjacent-seat constraints in V1.
- Payment failures trigger automatic seat release.

---

### 2. Core Entities & Responsibilities

| Entity | Responsibility |
|--------|----------------|
| `TicketBookingSystem` | Singleton facade. Coordinates search, booking lifecycle, and external integrations. |
| `User` | Customer identity, payment methods, booking history. |
| `Event` / `Show` | `Show` binds an `Event` to a `Screen`, `Venue`, and time slot. Owns seat inventory. |
| `Venue` / `Screen` | Physical location & auditorium. Defines layout capacity. |
| `Seat` | Physical seat unit. Tracks row, col, type, current status, and version for concurrency. |
| `SeatLock` | Temporary reservation holding. Stores `userId`, `showId`, `expiresAt`, and locked `Seat` list. |
| `Booking` | Domain aggregate. Tracks seats, pricing, lifecycle state, and linked payment. |
| `Payment` | Transaction record. Stores amount, status, gateway reference, and refund metadata. |

---

### 3. Class Design (Java Implementation)

```java
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

// ================= ENUMS =================
enum SeatType { VIP, PREMIUM, REGULAR }
enum SeatStatus { AVAILABLE, LOCKED, BOOKED }
enum BookingStateType { CREATED, LOCKED, CONFIRMED, CANCELLED, EXPIRED }
enum PaymentStatus { PENDING, SUCCESS, FAILED, REFUNDED }

// ================= DOMAIN MODELS =================
class Seat {
    private final String id;
    private final int row, col;
    private final SeatType type;
    private volatile SeatStatus status;
    private volatile int version; // Optimistic locking

    public Seat(String id, int row, int col, SeatType type) {
        this.id = id; this.row = row; this.col = col; this.type = type; this.status = SeatStatus.AVAILABLE; this.version = 1;
    }
    public synchronized boolean lock(int expectedVersion) {
        if (status != SeatStatus.AVAILABLE || version != expectedVersion) return false;
        status = SeatStatus.LOCKED; version++; return true;
    }
    public synchronized boolean book(int expectedVersion) {
        if (status != SeatStatus.LOCKED || version != expectedVersion) return false;
        status = SeatStatus.BOOKED; version++; return true;
    }
    public synchronized void release(int expectedVersion) {
        if (version == expectedVersion) { status = SeatStatus.AVAILABLE; version++; }
    }
    public String getId() { return id; }
    public SeatType getType() { return type; }
    public int getVersion() { return version; }
    public SeatStatus getStatus() { return status; }
}

class Show {
    private final String id;
    private final String eventId;
    private final Instant startTime;
    private final Map<String, Seat> seats;
    private final ReentrantReadWriteLock layoutLock = new ReentrantReadWriteLock();

    public Show(String id, String eventId, Instant startTime, Map<String, Seat> seats) {
        this.id = id; this.eventId = eventId; this.startTime = startTime; this.seats = seats;
    }
    public Optional<Seat> getSeat(String seatId) {
        layoutLock.readLock().lock(); try { return Optional.ofNullable(seats.get(seatId)); } finally { layoutLock.readLock().unlock(); }
    }
    public Collection<Seat> getAllSeats() { return seats.values(); }
    public String getId() { return id; }
    public Instant getStartTime() { return startTime; }
}

class SeatLock {
    private final String lockId;
    private final String userId;
    private final String showId;
    private final List<Seat> seats;
    private final Instant expiresAt;

    public SeatLock(String userId, String showId, List<Seat> seats, Duration ttl) {
        this.lockId = UUID.randomUUID().toString().substring(0, 8);
        this.userId = userId; this.showId = showId; this.seats = seats;
        this.expiresAt = Instant.now().plus(ttl);
    }
    public boolean isExpired() { return Instant.now().isAfter(expiresAt); }
    public String getUserId() { return userId; }
    public String getShowId() { return showId; }
    public List<Seat> getSeats() { return seats; }
    public Instant getExpiresAt() { return expiresAt; }
}

class Payment {
    private final String transactionId;
    private final double amount;
    private PaymentStatus status;
    public Payment(String txnId, double amount) { this.transactionId = txnId; this.amount = amount; this.status = PaymentStatus.PENDING; }
    public void success() { status = PaymentStatus.SUCCESS; }
    public void fail() { status = PaymentStatus.FAILED; }
    public PaymentStatus getStatus() { return status; }
}

// ================= STATE PATTERN =================
abstract class BookingState {
    protected Booking context;
    BookingState(Booking ctx) { this.context = ctx; }
    abstract void lockSeats();
    abstract void confirmPayment(Payment p);
    abstract void cancel();
    abstract void expire();
    void transition(BookingState next) { context.setState(next); }
}

class CreatedState extends BookingState {
    CreatedState(Booking ctx) { super(ctx); }
    public void lockSeats() { transition(new LockedState(context)); }
    public void confirmPayment(Payment p) { fail("Payment before locking seats"); }
    public void cancel() { transition(new CancelledState(context)); }
    public void expire() { transition(new ExpiredState(context)); }
}
class LockedState extends BookingState {
    LockedState(Booking ctx) { super(ctx); }
    public void lockSeats() { fail("Already locked"); }
    public void confirmPayment(Payment p) {
        if (p.getStatus() == PaymentStatus.SUCCESS) transition(new ConfirmedState(context));
        else transition(new CancelledState(context));
    }
    public void cancel() { transition(new CancelledState(context)); }
    public void expire() { transition(new ExpiredState(context)); }
}
class ConfirmedState extends BookingState {
    ConfirmedState(Booking ctx) { super(ctx); }
    public void lockSeats() { fail("Already confirmed"); }
    public void confirmPayment(Payment p) { fail("Already confirmed"); }
    public void cancel() { fail("Cannot cancel after confirmation"); }
    public void expire() { fail("Terminal state"); }
}
class CancelledState extends BookingState {
    CancelledState(Booking ctx) { super(ctx); }
    public void lockSeats() { fail("Cancelled"); } public void confirmPayment(Payment p) { fail("Cancelled"); }
    public void cancel() { fail("Already cancelled"); } public void expire() { fail("Cancelled"); }
}
class ExpiredState extends BookingState {
    ExpiredState(Booking ctx) { super(ctx); }
    public void lockSeats() { fail("Expired"); } public void confirmPayment(Payment p) { fail("Expired"); }
    public void cancel() { fail("Expired"); } public void expire() { fail("Already expired"); }
}

class Booking {
    private final String id;
    private final String userId;
    private final Show show;
    private final List<Seat> seats;
    private BookingStateType stateType;
    private BookingState currentState;
    private Payment payment;
    private double totalPrice;

    public Booking(String id, String userId, Show show, List<Seat> seats) {
        this.id = id; this.userId = userId; this.show = show; this.seats = seats;
        this.currentState = new CreatedState(this); this.stateType = BookingStateType.CREATED;
    }
    void setState(BookingState s) { stateType = getStateType(s); currentState = s; }
    private BookingStateType getStateType(BookingState s) {
        if (s instanceof LockedState) return BookingStateType.LOCKED;
        if (s instanceof ConfirmedState) return BookingStateType.CONFIRMED;
        if (s instanceof CancelledState) return BookingStateType.CANCELLED;
        if (s instanceof ExpiredState) return BookingStateType.EXPIRED;
        return BookingStateType.CREATED;
    }
    public void lockSeats() { currentState.lockSeats(); }
    public void confirmPayment(Payment p) { currentState.confirmPayment(p); this.payment = p; }
    public void cancel() { currentState.cancel(); }
    public void expire() { currentState.expire(); }
    public BookingStateType getStateType() { return stateType; }
    public List<Seat> getSeats() { return seats; }
    public String getId() { return id; }
}

// ================= STRATEGY PATTERN =================
interface PricingStrategy { double calculate(Seat s); }
class CategoryPricing implements PricingStrategy {
    private final Map<SeatType, Double> rates = Map.of(SeatType.VIP, 50.0, SeatType.PREMIUM, 30.0, SeatType.REGULAR, 15.0);
    public double calculate(Seat s) { return rates.getOrDefault(s.getType(), 0.0); }
}

// ================= FACTORY =================
class SeatFactory {
    public static Seat create(String id, int row, int col, SeatType type) { return new Seat(id, row, col, type); }
}

// ================= OBSERVER =================
interface BookingEventListener { void onBookingStatusChange(String bookingId, BookingStateType status); }

// ================= MANAGERS & SERVICES =================
class SeatLockManager {
    private final Map<String, SeatLock> activeLocks = new ConcurrentHashMap<>();
    private final Map<String, List<String>> userLocks = new ConcurrentHashMap<>();
    private final List<BookingEventListener> listeners = new CopyOnWriteArrayList<>();

    public void registerListener(BookingEventListener l) { listeners.add(l); }

    public SeatLock acquire(String userId, Show show, List<String> seatIds, Duration ttl) {
        List<Seat> targetSeats = new ArrayList<>();
        for (String sid : seatIds) {
            Seat s = show.getSeat(sid).orElseThrow(() -> new IllegalArgumentException("Seat " + sid + " not found"));
            if (!s.lock(s.getVersion())) throw new IllegalStateException("Seat " + sid + " not available");
            targetSeats.add(s);
        }
        SeatLock lock = new SeatLock(userId, show.getId(), targetSeats, ttl);
        activeLocks.put(lock.getLockId(), lock);
        userLocks.computeIfAbsent(userId, k -> new ArrayList<>()).add(lock.getLockId());
        return lock;
    }

    public void release(String lockId) {
        SeatLock lock = activeLocks.remove(lockId);
        if (lock != null) {
            lock.getSeats().forEach(s -> s.release(s.getVersion() - 1));
        }
    }

    public void sweepExpired() {
        Instant now = Instant.now();
        activeLocks.values().removeIf(lock -> {
            if (now.isAfter(lock.getExpiresAt())) {
                release(lock.getLockId());
                return true;
            }
            return false;
        });
    }
}

class BookingService {
    private final PricingStrategy pricing;
    private final SeatLockManager lockManager;
    private final List<BookingEventListener> listeners = new CopyOnWriteArrayList<>();

    public BookingService(PricingStrategy pricing, SeatLockManager lockMgr) {
        this.pricing = pricing; this.lockManager = lockMgr;
    }
    public void registerListener(BookingEventListener l) { listeners.add(l); }

    public Booking createAndLock(String userId, Show show, List<String> seatIds) {
        SeatLock lock = lockManager.acquire(userId, show, seatIds, Duration.ofMinutes(5));
        Booking booking = new Booking(UUID.randomUUID().toString().substring(0,8), userId, show, lock.getSeats());
        booking.lockSeats();
        calculateTotal(booking);
        notifyListeners(booking.getId(), booking.getStateType());
        return booking;
    }

    public Booking confirmBooking(Booking booking, Payment payment) {
        if (payment.getStatus() != PaymentStatus.SUCCESS) {
            lockManager.release(booking.getSeats().stream().map(Seat::getId).collect(Collectors.toList()).toString()); // Simplified release
            booking.cancel();
        } else {
            // Atomically book seats in show inventory
            for (Seat s : booking.getSeats()) {
                if (!s.book(s.getVersion() - 1)) {
                    throw new IllegalStateException("Seat " + s.getId() + " state conflict during booking");
                }
            }
            booking.confirmPayment(payment);
        }
        notifyListeners(booking.getId(), booking.getStateType());
        return booking;
    }

    private void calculateTotal(Booking b) { b.totalPrice = b.getSeats().stream().mapToDouble(pricing::calculate).sum(); }
    private void notifyListeners(String id, BookingStateType type) {
        for (BookingEventListener l : listeners) l.onBookingStatusChange(id, type);
    }
}

// ================= SINGLETON =================
class TicketBookingSystem {
    private static volatile TicketBookingSystem instance;
    private final Map<String, Show> showRegistry = new ConcurrentHashMap<>();
    private final SeatLockManager lockManager;
    private final BookingService bookingService;

    private TicketBookingSystem() {
        lockManager = new SeatLockManager();
        bookingService = new BookingService(new CategoryPricing(), lockManager);
    }
    public static TicketBookingSystem getInstance() {
        if (instance == null) { synchronized(TicketBookingSystem.class) { if(instance==null) instance = new TicketBookingSystem(); } }
        return instance;
    }
    public BookingService getBookingService() { return bookingService; }
    public void addShow(Show s) { showRegistry.put(s.getId(), s); }
    public Show getShow(String id) { return showRegistry.get(id); }
}
```

---

### 4. Relationships & Class Diagram (Textual UML)

```
[TicketBookingSystem] 1 <1..> 1 [BookingService]       (Composition)
[BookingService] 1 --> 1 [SeatLockManager]             (Composition)
[BookingService] 1 --> 1 [PricingStrategy]             (Dependency Injection)

[Show] 1 *--> * [Seat]                                 (Composition: Seats belong to a specific show/screen instance)
[Seat] 1 --> 1 [SeatStatus]                            (Dependency)

[Booking] 1 --> 1 [BookingState]                       (Composition/Context: State Pattern)
[BookingState] <|-- [CreatedState]                     (Inheritance)
[BookingState] <|-- [LockedState]
[BookingState] <|-- [ConfirmedState]
[BookingState] <|-- [CancelledState]
[BookingState] <|-- [ExpiredState]

[SeatLockManager] 1 o--> * [SeatLock]                  (Aggregation: Temporary holds)
[SeatLock] 1 *--> * [Seat]                             (Association: References locked seats)

[BookingService] ..> [BookingEventListener]            (Observer Pattern)
[SeatFactory] ..> [Seat]                               (Creation)
```

---

### 5. Design Patterns Used

| Pattern | Implementation in Code |
|---------|------------------------|
| **Singleton** | `TicketBookingSystem` uses double-checked locking. Guarantees single registry for shows & orchestrates core flow. |
| **Factory Method** | `SeatFactory.create()` abstracts seat instantiation. Allows extending to `RecliningSeat` or `WheelchairAccessibleSeat` without touching booking logic. |
| **Strategy** | `PricingStrategy` interface decouples pricing algorithms from `BookingService`. `CategoryPricing` can be swapped for `DynamicSurgePricing` or `PromoPricing` at runtime. |
| **State Pattern** | `Booking` delegates operations to `BookingState`. Concrete states enforce valid transitions (e.g., cannot confirm without locking). Prevents illegal state jumps and encapsulates transition side-effects. |
| **Observer** | `BookingEventListener` interface + `CopyOnWriteArrayList` in `BookingService`/`SeatLockManager`. Fires on status change. Decouples core booking from UI updates, analytics, or notification dispatch. |

---

### 6. Core Flow (Very Important)

#### Search Flow
1. **Request**: User filters by city, date, event.
2. **Aggregation**: System queries `Show` registry/DB, joins with `Venue` capacity & `Screen` layout.
3. **Availability Pre-calc**: Computes `availableSeatsCount` from cached counters. Returns lightweight `ShowDTO` list.

#### Seat Selection Flow
1. **Layout Fetch**: Client requests `getSeatLayout(showId)`.
2. **Status Projection**: System reads `Seat` status & version from DB/Redis. Maps to `AVAILABLE`, `LOCKED`, `BOOKED`.
3. **Render**: Frontend displays color-coded grid. User selects seats.

#### Seat Locking Flow
1. **Lock Request**: Client submits `seatIds` → `BookingService.createAndLock()`.
2. **Atomic Acquisition**: `SeatLockManager` iterates seats, calls `seat.lock(expectedVersion)`. Uses version check (optimistic) or row-level lock (pessimistic).
3. **Fail Fast**: If any seat fails (already taken/locked), transaction rolls back all acquired seats immediately.
4. **Create Lock**: If successful, `SeatLock` created with `expiresAt = now + 5m`. Booking state → `LOCKED`.
5. **Response**: Returns `lockId`, `expiresAt`, `totalPrice`. Timer starts client-side.

#### Booking & Payment Flow
1. **Validate**: Payment service called with `bookingId` & amount.
2. **State Check**: Ensures `state == LOCKED` and `expiresAt > now`.
3. **Process Payment**: Synchronous/async gateway call. If `FAILED` → booking transitions to `CANCELLED`, seats released.
4. **Confirm**: If `SUCCESS`, `Booking.confirmBooking()` called. `Seat.book()` transitions to `BOOKED`. State → `CONFIRMED`.
5. **Acknowledge**: Ticket PDF/QR generated. Event published.

#### Seat Release Flow (Timeout)
1. **Background Sweeper**: Cron job or Redis TTL expiry event triggers `SeatLockManager.sweepExpired()`.
2. **Release Loop**: Iterates expired locks, calls `seat.release()` for each seat.
3. **State Update**: Booking transitions to `EXPIRED`. Observer notifies client to refresh seat map.
4. **Cleanup**: Expired lock removed from active registry.

---

### 7. API Design (Conceptual Only)

| Operation | Method | Request Payload | Response | Status Codes |
|-----------|--------|-----------------|----------|--------------|
| `searchShows` | `POST /shows/search` | `{ city: "NYC", date: "2026-05-10", genre: "Action" }` | `[ { showId, eventId, venue, time, availableCount } ]` | 200 OK, 400 Invalid Params |
| `getSeatLayout` | `GET /shows/{showId}/seats` | N/A | `{ layout: { rows: 10, cols: 20 }, seats: [{id, row, col, status, price}] }` | 200 OK, 404 Show |
| `lockSeats` | `POST /bookings/lock` | `{ userId, showId, seatIds: ["S-12", "S-13"] }` | `{ bookingId: "B-9A2F", status: "LOCKED", total: 60.0, expiresAt: "...", lockId: "..." }` | 201 Created, 409 Seat Unavailable |
| `confirmBooking` | `POST /bookings/{id}/confirm` | `{ paymentIntentId: "pi_xyz", idempotencyKey: "ik_123" }` | `{ bookingId, status: "CONFIRMED", tickets: [{qrCode, seat}] }` | 200 OK, 400 Expired/State Error, 502 Payment Failed |
| `cancelBooking` | `POST /bookings/{id}/cancel` | N/A | `{ status: "CANCELLED", refundAmount: 60.0, processedAt: "..." }` | 200 OK, 409 Already Confirmed, 404 Not Found |

---

### 8. Database Schema Design

**Tables & Columns**
- `shows` (`id` PK VARCHAR, `event_id` FK, `screen_id` FK, `start_time` TIMESTAMP, `status` ENUM)
- `seats` (`id` PK VARCHAR, `show_id` FK, `row_num` INT, `col_num` INT, `type` ENUM, `status` ENUM, `version` INT)
- `bookings` (`id` PK VARCHAR, `user_id` FK, `show_id` FK, `total_price` DECIMAL, `status` ENUM, `expires_at` TIMESTAMP, `created_at` TIMESTAMP)
- `booking_seats` (`booking_id` FK, `seat_id` FK, PRIMARY KEY(booking_id, seat_id))
- `seat_locks` (`lock_id` PK VARCHAR, `user_id` FK, `show_id` FK, `seat_ids` JSONB, `expires_at` TIMESTAMP)
- `payments` (`txn_id` PK VARCHAR, `booking_id` FK UNIQUE, `amount` DECIMAL, `status` ENUM, `gateway_ref` VARCHAR)

**Indexing Strategy**
- `seats(show_id, status)` → Composite index. Critical for `WHERE show_id = ? AND status = 'AVAILABLE'` queries.
- `bookings(user_id, created_at DESC)` → Fast order history.
- `seat_locks(show_id, expires_at)` → Partial index `WHERE expires_at < NOW()`. Powers efficient sweeper queries.
- `seats(version)` → Not indexed; used inline for optimistic locking.
- `payments(gateway_ref)` → For reconciliation & webhook idempotency.

---

### 9. Concurrency & Consistency

- **Double Booking Prevention**: 
  - *Application*: `synchronized` + version check in LLD. 
  - *Production*: DB `SELECT status, version FROM seats WHERE id = ? AND status = 'AVAILABLE' FOR UPDATE SKIP LOCKED` ensures exclusive row access during lock. Only one transaction succeeds.
- **Optimistic Locking**: `version` column incremented on every state change. `UPDATE seats SET status='LOCKED', version=version+1 WHERE id=? AND version=?`. Zero rows affected → conflict → retry/fail fast.
- **Distributed Locking**: Redis `SETNX seat:{show}:{seatId} {lockId} EX 300`. Lua script validates status & sets lock atomically. Prevents race conditions across microservice instances.
- **Idempotent Booking**: `POST /confirm` requires `idempotencyKey`. DB stores `(booking_id, idempotency_key)` unique constraint. Retries return cached result without double-charging.
- **Sweep Consistency**: Expired locks processed by distributed worker pool using Redis `ZSET` sorted by `expiresAt`. Workers claim ranges with `ZPOPMIN`, ensuring no overlap.

---

### 10. Edge Cases

| Scenario | Handling |
|----------|----------|
| **Seat Lock Expiration** | Redis TTL expires or cron sweeps. `EXPIRED` state set. Seats atomically reverted to `AVAILABLE`. Client UI refreshes. |
| **Payment Failure After Lock** | Booking transitions to `CANCELLED`. `Seat.release()` executed. User sees payment error prompt. No seat held. |
| **Partial Booking Failure** | If `seat.book()` fails for 1 of 4 seats (e.g., concurrent edge), entire booking fails. Saga compensation releases all 3 successfully locked seats. Atomicity maintained. |
| **Concurrent Selection** | Two users click same seats simultaneously. First hits DB/Redis lock succeeds. Second gets `409 Conflict` with updated layout. |
| **Network Timeout on Confirm** | Client retries with same `idempotencyKey`. Gateway checks payment status. If paid, confirms booking. If not, releases seats. |

---

### 11. Sample SQL Queries

**Fetch available seats for a show:**
```sql
SELECT id, row_num, col_num, type, price 
FROM seats 
WHERE show_id = $1 AND status = 'AVAILABLE' 
ORDER BY row_num ASC, col_num ASC;
```

**Get user bookings with status:**
```sql
SELECT b.id, b.total_price, b.status, b.created_at, s.name AS movie_name
FROM bookings b
JOIN shows sh ON b.show_id = sh.id
JOIN events s ON sh.event_id = s.id
WHERE b.user_id = $1 AND b.created_at > NOW() - INTERVAL '90 days'
ORDER BY b.created_at DESC;
```

**Find locked vs booked seats (capacity planning):**
```sql
SELECT s.type, 
       COUNT(CASE WHEN s.status = 'AVAILABLE' THEN 1 END) AS avail,
       COUNT(CASE WHEN s.status = 'LOCKED' THEN 1 END) AS locked,
       COUNT(CASE WHEN s.status = 'BOOKED' THEN 1 END) AS booked
FROM seats s 
WHERE s.show_id = $1 
GROUP BY s.type;
```

---

### 12. Trade-offs & Extensions

**Limitations of Current Design**
- **In-Memory State**: `SeatLockManager` & `showRegistry` won't survive JVM restarts. Requires Redis/DB persistence.
- **Synchronous Payment**: Blocks user flow. Network issues at gateway cause poor UX.
- **Global Seat Locking**: Sweeping all expired locks in memory scales poorly. Requires partitioned Redis `ZSET`.

**Improvements & Extensions**
1. **Distributed Redis Architecture**: Use `ZSET` keyed by `expires_at` for O(logN) expiry processing. Lua scripts for atomic lock/unlock. `SETNX` with NX flag prevents race conditions.
2. **Event-Driven Async Booking**: Replace sync payment with Kafka. `SeatsLocked` → `PaymentProcessed` → `SeatsBooked`. Decouples gateway latency from user experience. DLQ handles failures.
3. **Waitlist System**: If show sold out, users join queue. When `CANCELLED`/`NO-SHOW` occurs, next in line gets priority lock window. Implemented via Redis `LIST` + WebSockets.
4. **Real-Time Seat Map Cache**: Push seat status changes via Server-Sent Events (SSE) or WebSockets. Redis Pub/Sub broadcasts `SEAT_STATUS_CHANGED`. Frontend updates without polling.
5. **Dynamic Pricing & Surge**: Inject `SurgePricingStrategy` that adjusts prices based on `LOCKED+BOOKED / TOTAL` ratio. Updates reflected in `getSeatLayout()` via cache bust.
6. **Anti-Bot & Fairness**: Implement CAPTCHA + rate limiting per IP during flash sales. Shuffle queue order or use cryptographic lotteries for high-demand events.
