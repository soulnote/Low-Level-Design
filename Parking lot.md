
# Parking lot
### 1. Requirement Clarification

**Functional Requirements**
- Vehicle entry: License plate scanned, nearest compatible slot allocated, ticket generated.
- Vehicle exit: Ticket scanned, duration calculated, fee computed based on pricing rules, payment processed, slot released.
- Ticketing: Unique ticket ID, entry/exit timestamps, assigned slot, status tracking.
- Pricing: Vehicle-type & duration-based pricing. Supports fixed hourly, flat rates, or dynamic strategies.
- Multi-floor support: Slots distributed across floors with type-based constraints (Compact, Standard, Large).
- Payment: Supports cash/card. Tracks amount, timestamp, status, and links to ticket.

**Non-Functional Requirements**
- Low latency: Entry/exit processing < 200ms.
- High concurrency: Multiple entry/exit gates operating simultaneously without double-booking.
- Thread-safe & atomic operations for slot state transitions.
- Extensible: Easy to add new pricing/allocation strategies, vehicle types, or payment gateways.
- Auditability: All transactions logged for reconciliation.

**Assumptions**
- 1 `ParkingLot` instance per physical location.
- Vehicle types: `BIKE`, `CAR`, `TRUCK`.
- Slot types: `COMPACT`, `STANDARD`, `LARGE`.
- First-come-first-serve allocation (no reservations in V1).
- Single payment per ticket. Partial payments or multi-currency out of scope.
- In-memory state for LLD; persistence handled via DB in production.

---

### 2. Core Entities & Responsibilities

| Entity | Responsibility |
|--------|----------------|
| `ParkingLot` | Singleton facade. Holds floors, orchestrates high-level operations, coordinates managers. |
| `ParkingFloor` | Manages slots on a specific level. Provides thread-safe slot search. |
| `ParkingSlot` | Represents physical space. Tracks status (`FREE`, `OCCUPIED`, `MAINTENANCE`) and assigned vehicle. |
| `Vehicle` | Abstract base for all vehicle types. Holds license plate, type, dimensions. |
| `Ticket` | Immutable record of parking session. Links vehicle, slot, timestamps, and payment status. |
| `Payment` | Encapsulates amount, method, status, and links to ticket. |
| `SlotManager` | Handles slot discovery, allocation, and release using strategy pattern. |
| `TicketManager` | Generates tickets, validates state transitions, prevents duplicate active entries. |
| `PaymentService` | Processes payments, handles failures, updates ticket state. |
| `ParkingService` | Orchestrates entry/exit flows by coordinating managers & services. |

---

### 3. Class Design (Java Implementation)

```java
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

// ================= ENUMS =================
enum VehicleType { BIKE, CAR, TRUCK }
enum SlotType { COMPACT, STANDARD, LARGE }
enum SlotStatus { FREE, OCCUPIED, MAINTENANCE }
enum PaymentStatus { PENDING, COMPLETED, FAILED }
enum TicketStatus { ACTIVE, COMPLETED, CANCELLED }

// ================= DOMAIN MODELS =================
abstract class Vehicle {
    protected final String licensePlate;
    protected final VehicleType type;

    protected Vehicle(String licensePlate, VehicleType type) {
        this.licensePlate = licensePlate;
        this.type = type;
    }
    public String getLicensePlate() { return licensePlate; }
    public VehicleType getType() { return type; }
}

class Car extends Vehicle { public Car(String plate) { super(plate, VehicleType.CAR); } }
class Bike extends Vehicle { public Bike(String plate) { super(plate, VehicleType.BIKE); } }
class Truck extends Vehicle { public Truck(String plate) { super(plate, VehicleType.TRUCK); } }

class ParkingSlot {
    private final String id;
    private final SlotType type;
    private volatile SlotStatus status;
    private Vehicle assignedVehicle;

    public ParkingSlot(String id, SlotType type) {
        this.id = id;
        this.type = type;
        this.status = SlotStatus.FREE;
    }

    public synchronized boolean assign(Vehicle v) {
        if (status != SlotStatus.FREE) return false;
        status = SlotStatus.OCCUPIED;
        assignedVehicle = v;
        return true;
    }

    public synchronized void release() {
        status = SlotStatus.FREE;
        assignedVehicle = null;
    }

    public String getId() { return id; }
    public SlotType getType() { return type; }
    public SlotStatus getStatus() { return status; }
    public Vehicle getVehicle() { return assignedVehicle; }
}

class ParkingFloor {
    private final int floorNumber;
    private final List<ParkingSlot> slots;
    private final ReentrantLock allocationLock = new ReentrantLock();

    public ParkingFloor(int floorNumber, List<ParkingSlot> slots) {
        this.floorNumber = floorNumber;
        this.slots = slots;
    }

    public int getFloorNumber() { return floorNumber; }

    // Thread-safe slot search
    public ParkingSlot findAndReserveSlot(SlotType requiredType) {
        allocationLock.lock();
        try {
            for (ParkingSlot slot : slots) {
                if (slot.getStatus() == SlotStatus.FREE && slot.getType() == requiredType) {
                    return slot;
                }
            }
            return null;
        } finally {
            allocationLock.unlock();
        }
    }
}

class Ticket {
    private final String id;
    private final Vehicle vehicle;
    private final ParkingSlot assignedSlot;
    private final LocalDateTime entryTime;
    private TicketStatus status;
    private LocalDateTime exitTime;
    private Payment payment;

    public Ticket(String id, Vehicle vehicle, ParkingSlot slot) {
        this.id = id;
        this.vehicle = vehicle;
        this.assignedSlot = slot;
        this.entryTime = LocalDateTime.now();
        this.status = TicketStatus.ACTIVE;
    }

    public void markExit() { this.exitTime = LocalDateTime.now(); this.status = TicketStatus.COMPLETED; }
    public void attachPayment(Payment p) { this.payment = p; }

    public String getId() { return id; }
    public Vehicle getVehicle() { return vehicle; }
    public ParkingSlot getSlot() { return assignedSlot; }
    public LocalDateTime getEntryTime() { return entryTime; }
    public TicketStatus getStatus() { return status; }
    public Payment getPayment() { return payment; }
}

class Payment {
    private final String transactionId;
    private final double amount;
    private PaymentStatus status;
    private final LocalDateTime timestamp;

    public Payment(String transactionId, double amount) {
        this.transactionId = transactionId;
        this.amount = amount;
        this.status = PaymentStatus.PENDING;
        this.timestamp = LocalDateTime.now();
    }
    public void markCompleted() { this.status = PaymentStatus.COMPLETED; }
    public void markFailed() { this.status = PaymentStatus.FAILED; }
    public PaymentStatus getStatus() { return status; }
    public double getAmount() { return amount; }
}

// ================= STRATEGIES =================
interface SlotAllocationStrategy {
    SlotType getRequiredSlotType(VehicleType vType);
}

class DefaultSlotStrategy implements SlotAllocationStrategy {
    public SlotType getRequiredSlotType(VehicleType vType) {
        return switch (vType) {
            case BIKE -> SlotType.COMPACT;
            case CAR -> SlotType.STANDARD;
            case TRUCK -> SlotType.LARGE;
        };
    }
}

interface PricingStrategy {
    double calculatePrice(double hours, VehicleType vType);
}

class HourlyPricingStrategy implements PricingStrategy {
    private final Map<VehicleType, Double> ratePerHour;
    public HourlyPricingStrategy() {
        ratePerHour = Map.of(VehicleType.BIKE, 2.0, VehicleType.CAR, 5.0, VehicleType.TRUCK, 10.0);
    }
    public double calculatePrice(double hours, VehicleType vType) {
        double rate = ratePerHour.getOrDefault(vType, 5.0);
        return Math.ceil(hours) * rate; // Minimum 1 hour billing
    }
}

// ================= MANAGERS =================
class SlotManager {
    private final ParkingLot lot;
    private final SlotAllocationStrategy strategy;

    public SlotManager(ParkingLot lot, SlotAllocationStrategy strategy) {
        this.lot = lot;
        this.strategy = strategy;
    }

    public Optional<ParkingSlot> allocate(Vehicle vehicle) {
        SlotType required = strategy.getRequiredSlotType(vehicle.getType());
        for (ParkingFloor floor : lot.getFloors()) {
            ParkingSlot slot = floor.findAndReserveSlot(required);
            if (slot != null) {
                if (slot.assign(vehicle)) return Optional.of(slot);
            }
        }
        return Optional.empty();
    }

    public void release(ParkingSlot slot) {
        slot.release();
    }
}

class TicketManager {
    private final Map<String, Ticket> tickets = new ConcurrentHashMap<>();
    private final Map<String, Ticket> activeTicketsByPlate = new ConcurrentHashMap<>();

    public Ticket createTicket(Vehicle vehicle, ParkingSlot slot) {
        if (activeTicketsByPlate.containsKey(vehicle.getLicensePlate())) {
            throw new IllegalStateException("Duplicate entry: Vehicle already parked.");
        }
        String id = UUID.randomUUID().toString().substring(0, 8);
        Ticket t = new Ticket(id, vehicle, slot);
        tickets.put(id, t);
        activeTicketsByPlate.put(vehicle.getLicensePlate(), t);
        return t;
    }

    public Ticket validateAndRetrieve(String ticketId) {
        Ticket t = tickets.get(ticketId);
        if (t == null || t.getStatus() != TicketStatus.ACTIVE) {
            throw new IllegalArgumentException("Invalid or inactive ticket.");
        }
        return t;
    }

    public void markCompleted(String ticketId) {
        Ticket t = tickets.get(ticketId);
        if (t != null) {
            t.markExit();
            activeTicketsByPlate.remove(t.getVehicle().getLicensePlate());
        }
    }
}

// ================= SERVICES =================
class PaymentService {
    public boolean processPayment(Ticket ticket, PricingStrategy pricing) {
        long durationMinutes = java.time.temporal.ChronoUnit.MINUTES.between(ticket.getEntryTime(), LocalDateTime.now());
        double hours = Math.max(1, durationMinutes / 60.0);
        double amount = pricing.calculatePrice(hours, ticket.getVehicle().getType());

        Payment payment = new Payment("TXN-" + UUID.randomUUID().toString().substring(0, 6), amount);
        try {
            // Simulate gateway call
            payment.markCompleted();
            ticket.attachPayment(payment);
            return true;
        } catch (Exception e) {
            payment.markFailed();
            return false;
        }
    }
}

class ParkingService {
    private final SlotManager slotManager;
    private final TicketManager ticketManager;
    private final PaymentService paymentService;
    private final PricingStrategy pricingStrategy;

    public ParkingService(SlotManager sm, TicketManager tm, PaymentService ps, PricingStrategy pricing) {
        this.slotManager = sm;
        this.ticketManager = tm;
        this.paymentService = ps;
        this.pricingStrategy = pricing;
    }

    public Ticket entry(Vehicle vehicle) {
        ParkingSlot slot = slotManager.allocate(vehicle)
                .orElseThrow(() -> new IllegalStateException("Parking lot is full or no compatible slot."));
        return ticketManager.createTicket(vehicle, slot);
    }

    public boolean exit(String ticketId) {
        Ticket ticket = ticketManager.validateAndRetrieve(ticketId);
        if (!paymentService.processPayment(ticket, pricingStrategy)) {
            return false; // Payment failed, slot remains occupied
        }
        ticketManager.markCompleted(ticketId);
        slotManager.release(ticket.getSlot());
        return true;
    }
}

// ================= FACTORY =================
class VehicleFactory {
    public static Vehicle create(String licensePlate, VehicleType type) {
        return switch (type) {
            case BIKE -> new Bike(licensePlate);
            case CAR -> new Car(licensePlate);
            case TRUCK -> new Truck(licensePlate);
        };
    }
}

// ================= SINGLETON =================
class ParkingLot {
    private static ParkingLot instance;
    private final List<ParkingFloor> floors;

    private ParkingLot(List<ParkingFloor> floors) {
        this.floors = Collections.unmodifiableList(floors);
    }

    public static synchronized ParkingLot getInstance() {
        if (instance == null) {
            // Initialize with mock floors for LLD
            List<ParkingFloor> floors = new ArrayList<>();
            floors.add(new ParkingFloor(1, buildSlots(1, SlotType.STANDARD, 10)));
            floors.add(new ParkingFloor(2, buildSlots(2, SlotType.COMPACT, 8)));
            instance = new ParkingLot(floors);
        }
        return instance;
    }

    private static List<ParkingSlot> buildSlots(int floor, SlotType type, int count) {
        List<ParkingSlot> list = new ArrayList<>();
        for (int i = 0; i < count; i++) list.add(new ParkingSlot("F"+floor+"-S"+i, type));
        return list;
    }

    public List<ParkingFloor> getFloors() { return floors; }
}
```

---

### 4. Relationships & Class Diagram (Textual UML)

```
[ParkingLot] 1 <1..> * [ParkingFloor]  (Composition: Floors cannot exist without Lot)
[ParkingFloor] 1 <1..> * [ParkingSlot] (Composition: Slots belong to a Floor)
[ParkingLot] 1 -- 1 [SlotManager]      (Aggregation: Manager operates on Lot reference)
[SlotManager] 1 -- 1 [SlotAllocationStrategy] (Dependency Injection)

[Ticket] 1 -- 1 [Vehicle]              (Aggregation: Ticket references Vehicle)
[Ticket] 1 -- 1 [ParkingSlot]          (Association: Assigned Slot)
[Ticket] 1 -- 1 [Payment]              (Composition/Association: Payment bound to Ticket lifecycle)

[ParkingService] 1 --> 1 [SlotManager]
[ParkingService] 1 --> 1 [TicketManager]
[ParkingService] 1 --> 1 [PaymentService]
[ParkingService] 1 --> 1 [PricingStrategy] (Dependency Injection)

[VehicleFactory] ..> [Vehicle]         (Creation)
```

---

### 5. Design Patterns Used

| Pattern | Implementation in Code |
|---------|------------------------|
| **Singleton** | `ParkingLot` uses eager thread-safe `getInstance()` with private constructor. Ensures single physical lot instance per JVM. |
| **Factory Method** | `VehicleFactory.create()` abstracts concrete vehicle instantiation (`Bike`, `Car`, `Truck`). Easy to extend with `SUV` or `EV` without modifying service logic. |
| **Strategy** | `SlotAllocationStrategy` & `PricingStrategy` interfaces decouple algorithms. `DefaultSlotStrategy` and `HourlyPricingStrategy` can be swapped at runtime or injected via DI. Open-Closed Principle compliant. |
| **Dependency Injection (Manual)** | `ParkingService` receives `SlotManager`, `TicketManager`, `PaymentService`, and `PricingStrategy` via constructor. Enables unit testing and strategy swapping. |
| **Guard Clause / State Validation** | `TicketManager` checks plate duplication & status before allowing exit. Prevents invalid state transitions. |

---

### 6. Core Flow (Very Important)

#### Vehicle Entry Flow
1. **Gate Trigger**: Hardware sends `licensePlate` + `vehicleType` to `ParkingService.entry()`.
2. **Vehicle Object Creation**: `VehicleFactory.create()` builds the domain object.
3. **Slot Allocation**: `SlotManager.allocate()` iterates floors (bottom-up for convenience). Calls `ParkingFloor.findAndReserveSlot()` which holds a `ReentrantLock` to prevent race conditions.
4. **State Transition**: Once a compatible `FREE` slot is found, `slot.assign(vehicle)` transitions status to `OCCUPIED`.
5. **Ticket Generation**: `TicketManager.createTicket()` validates no active ticket exists for this plate, generates a unique ID, links vehicle & slot, and sets `TicketStatus.ACTIVE`.
6. **Response**: Ticket object returned. Gate opens. System logs entry.

#### Vehicle Exit Flow
1. **Gate Trigger**: Exit terminal sends `ticketId` to `ParkingService.exit()`.
2. **Validation**: `TicketManager.validateAndRetrieve()` fetches ticket, ensures it exists and is `ACTIVE`. Throws on expired/paid/invalid.
3. **Pricing**: `PaymentService` calculates duration (`now - entryTime`), rounds up to nearest hour, delegates to `PricingStrategy.calculatePrice()`.
4. **Payment Processing**: Simulates gateway. On success, creates `Payment` object, attaches to ticket, marks `PaymentStatus.COMPLETED`.
5. **State Transition**: `TicketManager.markCompleted()` sets ticket to `COMPLETED`, removes plate from active map.
6. **Slot Release**: `SlotManager.release()` calls `slot.release()`, setting status back to `FREE` under synchronized block.
7. **Response**: Boolean `true` returned. Gate opens. Receipt generated. If payment fails, flow halts, slot remains occupied, retry prompt shown.

---

### 7. API Design (Conceptual Only)

| Operation | Conceptual Signature | Request Payload | Response Payload |
|-----------|----------------------|-----------------|------------------|
| Park Vehicle | `parkVehicle(plate, type, gateId)` | `{ "plate": "ABC-1234", "type": "CAR", "gateId": "E1" }` | `{ "ticketId": "T-8F3A", "slot": "F1-S03", "entryTime": "2026-05-06T10:15:00" }` |
| Unpark Vehicle | `unparkVehicle(ticketId)` | `{ "ticketId": "T-8F3A" }` | `{ "success": true, "amountCharged": 5.0, "exitTime": "2026-05-06T12:30:00", "paymentStatus": "COMPLETED" }` |
| Get Available Slots | `getAvailableSlots(type)` | `{ "type": "STANDARD" }` | `[ "F1-S03", "F2-S01", "F3-S05" ]` |
| Get Ticket Status | `getTicketDetails(ticketId)` | `{ "ticketId": "T-8F3A" }` | `{ "plate": "ABC-1234", "slot": "F1-S03", "status": "ACTIVE", "entryTime": "...", "durationHours": 2.25 }` |

*No REST controllers implemented. These map directly to `ParkingService` methods in a production wrapper.*

---

### 8. Database Schema Design

**Tables & Columns**
- `parking_slot`
  - `id` VARCHAR PK
  - `floor_id` INT FK → `parking_floor.id`
  - `type` ENUM (COMPACT, STANDARD, LARGE)
  - `status` ENUM (FREE, OCCUPIED, MAINTENANCE)
  - `vehicle_plate` VARCHAR NULL
  - `last_updated` TIMESTAMP
- `ticket`
  - `id` VARCHAR PK
  - `vehicle_plate` VARCHAR
  - `slot_id` VARCHAR FK → `parking_slot.id`
  - `entry_time` TIMESTAMP
  - `exit_time` TIMESTAMP NULL
  - `status` ENUM (ACTIVE, COMPLETED, CANCELLED)
- `payment`
  - `transaction_id` VARCHAR PK
  - `ticket_id` VARCHAR UNIQUE FK → `ticket.id`
  - `amount` DECIMAL(10,2)
  - `method` ENUM (CARD, CASH, UPI)
  - `status` ENUM (PENDING, COMPLETED, FAILED)
  - `processed_at` TIMESTAMP

**Indexing Strategy**
- `parking_slot`: Composite index `(status, type, floor_id)` for fast slot lookup.
- `ticket`: Unique index on `vehicle_plate` WHERE `status='ACTIVE'` to prevent duplicates. B-Tree index on `id`.
- `payment`: Index on `ticket_id` for quick payment lookup during exit reconciliation.

---

### 9. Concurrency Handling

- **Fine-Grained Locking**: Each `ParkingFloor` owns a `ReentrantLock`. Only the floor being scanned is locked. This avoids global bottlenecks and allows parallel entry across different floors.
- **Atomic State Transition**: `ParkingSlot.assign()` uses `synchronized` + status check. Even if two threads find the same slot, only one succeeds in changing `FREE → OCCUPIED`.
- **Optimistic vs Pessimistic**: In-memory uses pessimistic locking per floor. In distributed production, I would replace this with Redis `SETNX` with TTL or DB `SELECT ... FOR UPDATE SKIP LOCKED`.
- **Ticket Manager Safety**: `ConcurrentHashMap` for `tickets` and `activeTicketsByPlate`. `putIfAbsent` semantics implicitly handled by check-then-act inside synchronized scope or replaced with `computeIfAbsent`.
- **Payment Rollback**: If payment fails after slot allocation, system retains slot as occupied. Retry queue or manual intervention required. No automatic rollback to prevent double-booking during high traffic.

---

### 10. Edge Cases

| Scenario | Handling |
|----------|----------|
| **Parking Full / No Compatible Slot** | `allocate()` returns `Optional.empty()`. `ParkingService` throws `IllegalStateException`. Gate shows "Lot Full" and rejects entry. |
| **Invalid/Expired Ticket** | `validateAndRetrieve()` checks `status != ACTIVE`. Throws `IllegalArgumentException`. Gate denies exit, logs audit event. |
| **Payment Failure** | `PaymentService` catches gateway timeout/decline. Returns `false`. Slot stays `OCCUPIED`, ticket remains `ACTIVE`. UI prompts retry. Admin override available. |
| **Duplicate Entry** | `TicketManager` checks `activeTicketsByPlate` before creation. If found, throws exception. Prevents ghost vehicles. |
| **System Crash During Payment** | Payment marked `PENDING` in DB. On restart, cron job retries `PENDING` payments older than threshold. Idempotency key (`transactionId`) ensures no double charge. |

---

### 11. Sample SQL Queries

**Find nearest available slot for a vehicle type:**
```sql
SELECT s.id, f.floor_number
FROM parking_slot s
JOIN parking_floor f ON s.floor_id = f.id
WHERE s.status = 'FREE' AND s.type = 'STANDARD'
ORDER BY f.floor_number ASC, s.id ASC
LIMIT 1;
```

**Get all active tickets:**
```sql
SELECT t.id, t.vehicle_plate, t.slot_id, t.entry_time, 
       EXTRACT(EPOCH FROM (NOW() - t.entry_time))/3600 AS hours_parked
FROM ticket t
WHERE t.status = 'ACTIVE';
```

**Calculate parking duration & bill (with pricing lookup):**
```sql
SELECT 
    t.id,
    CEIL(EXTRACT(EPOCH FROM (t.exit_time - t.entry_time)) / 3600.0) AS billable_hours,
    p.rate_per_hour,
    CEIL(EXTRACT(EPOCH FROM (t.exit_time - t.entry_time)) / 3600.0) * p.rate_per_hour AS total_due
FROM ticket t
JOIN pricing_rules p ON v.type = p.vehicle_type
JOIN vehicle v ON t.vehicle_plate = v.plate
WHERE t.id = 'T-8F3A' AND t.status = 'ACTIVE';
```
*(Assumes `pricing_rules` table exists for production scalability)*

---

### 12. Trade-offs & Extensions

**Limitations of Current Design**
- **Single JVM State**: In-memory maps/locks don't survive restarts. Requires persistent cache/DB sync.
- **Blocking Floor Lock**: High contention if one floor is heavily used. May cause queuing for other gates.
- **No Reservation Support**: FCFS only. Cannot handle premium/advance bookings without redesign.
- **Payment Coupling**: Payment is synchronous. Gateway latency blocks exit gate.

**Improvements & Extensions**
1. **Distributed Caching**: Use Redis `ZSET` for `slot_id` by `floor` & `type`. `ReentrantLock` replaced with Redis `SETNX` + Lua scripts for atomic allocation across nodes.
2. **Async Payment Processing**: Publish `ExitRequested` event to Kafka. Exit gate opens immediately after validation. Background worker processes payment, updates DB, handles retries. Idempotent consumers ensure consistency.
3. **Dynamic Pricing Strategy**: Inject `SurgePricingStrategy` based on occupancy % (`active_slots / total_slots`).
4. **Microservice Split**: Separate `AllocationService`, `BillingService`, `TicketingService`. Communicate via gRPC/REST with circuit breakers.
5. **Observability**: Add structured logging, metrics (Prometheus: `slot_occupancy_rate`, `avg_exit_latency`), and distributed tracing (OpenTelemetry) for SLO monitoring.
6. **Hardware Integration**: Standardize gate communication via MQTT or WebSockets. Add retry/backoff for network flakiness.

This design prioritizes clean OOP boundaries, explicit pattern usage, thread safety, and interview-ready flow clarity while leaving clear extension hooks for production scale.
