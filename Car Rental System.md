# Car Rental System
### 1. Requirement Clarification

**Functional Requirements**
- **Search**: Filter vehicles by location, date/time range, type (SUV, Sedan, etc.), features (GPS, auto, child seat).
- **Book/Reserve**: Lock vehicle for a specific window, calculate upfront price, process payment.
- **Cancel**: Refund based on cancellation policy (time-to-start delta).
- **Pick-up & Return**: Verify identity, validate booking, mark vehicle status, generate final invoice with actual usage.
- **Billing**: Base rate + taxes + late penalties + fuel/extra charges.
- **Availability Tracking**: Real-time sync across locations, prevent overlapping bookings.

**Non-Functional Requirements**
- **High Availability**: 99.9% uptime, degraded search mode if booking service is down.
- **Scalability**: Multi-city architecture, partitioned inventory by location.
- **Low Latency**: Search < 150ms, Booking confirmation < 500ms.
- **Concurrency**: Handle peak traffic (weekends/holidays), strict double-booking prevention.

**Assumptions**
- Vehicle types: `HATCHBACK`, `SEDAN`, `SUV`, `LUXURY`
- Pricing: Daily base rate + hourly overage, dynamic surge during high demand.
- Multi-location: Each branch manages its own fleet, but global search aggregates availability.
- Single booking per vehicle per active window.
- Upfront payment required; refund on cancellation.
- 15-minute grace period for pick-up before auto-cancellation.

---

### 2. Core Entities & Responsibilities

| Entity | Responsibility |
|--------|----------------|
| `CarRentalSystem` | Singleton entry point. Orchestrates search, booking, and lifecycle coordination. |
| `User` | Holds profile, driving license, contact, payment methods. Validates eligibility. |
| `Vehicle` | Abstract base. Tracks plate, model, type, current status, location, maintenance flag. |
| `VehicleInventory` | Manages fleet per location. Maintains availability state, handles reservations. |
| `Booking` | Reservation record. Tracks time window, pricing, state transitions, linked payment/invoice. |
| `Payment` | Transaction details, status, gateway reference, refund tracking. |
| `Invoice` | Final bill post-return. Combines base fare, taxes, penalties, payment status. |
| `Location` | Branch/city identifier, coordinates, associated inventory. |
| `PricingStrategy` | Calculates cost based on duration, vehicle type, and demand. |
| `SearchService` | Filters inventory by criteria, checks time-window availability, returns ranked list. |

---

### 3. Class Design (Java Implementation)

```java
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

// ================= ENUMS =================
enum VehicleType { HATCHBACK, SEDAN, SUV, LUXURY }
enum VehicleStatus { AVAILABLE, RESERVED, RENTED, MAINTENANCE }
enum BookingStatus { CREATED, CONFIRMED, ACTIVE, COMPLETED, CANCELLED }
enum PaymentStatus { PENDING, COMPLETED, FAILED, REFUNDED }

// ================= DOMAIN MODELS =================
class User {
    private final String id;
    private final String name;
    private final String licenseNumber;
    public User(String id, String name, String licenseNumber) {
        this.id = id; this.name = name; this.licenseNumber = licenseNumber;
    }
    public String getId() { return id; }
}

abstract class Vehicle {
    protected final String id;
    protected final VehicleType type;
    protected final String model;
    protected final String plate;
    protected final Location location;
    protected VehicleStatus status;

    public Vehicle(String id, String model, String plate, VehicleType type, Location location) {
        this.id = id; this.model = model; this.plate = plate;
        this.type = type; this.location = location; this.status = VehicleStatus.AVAILABLE;
    }
    public synchronized void setStatus(VehicleStatus status) { this.status = status; }
    public String getId() { return id; }
    public VehicleType getType() { return type; }
    public VehicleStatus getStatus() { return status; }
    public Location getLocation() { return location; }
}

class Car extends Vehicle {
    private final int seats;
    public Car(String id, String model, String plate, VehicleType type, Location loc, int seats) {
        super(id, model, plate, type, loc);
        this.seats = seats;
    }
}

class Location {
    private final String id;
    private final String city;
    private final String address;
    public Location(String id, String city, String address) {
        this.id = id; this.city = city; this.address = address;
    }
    public String getId() { return id; }
    public String getCity() { return city; }
}

class Booking {
    private final String id;
    private final User user;
    private final Vehicle vehicle;
    private LocalDateTime startTime;
    private LocalDateTime endTime;
    private BookingStatus status;
    private Payment payment;
    private Invoice invoice;

    // State Pattern Context
    private BookingState state;

    public Booking(String id, User user, Vehicle vehicle, LocalDateTime start, LocalDateTime end) {
        this.id = id; this.user = user; this.vehicle = vehicle;
        this.startTime = start; this.endTime = end;
        this.state = new CreatedState(this);
        this.status = BookingStatus.CREATED;
    }

    public void confirm() { state.confirm(); }
    public void pickup() { state.pickup(); }
    public void complete(double actualHours) { state.complete(actualHours); }
    public void cancel() { state.cancel(); }

    public void setState(BookingState state) { this.state = state; }
    public void setStatus(BookingStatus status) { this.status = status; }
    public BookingStatus getStatus() { return status; }
    public Vehicle getVehicle() { return vehicle; }
    public LocalDateTime getStartTime() { return startTime; }
    public LocalDateTime getEndTime() { return endTime; }
    public String getId() { return id; }
    public Payment getPayment() { return payment; }
    public void setPayment(Payment p) { this.payment = p; }
    public void setInvoice(Invoice inv) { this.invoice = inv; }
    public Invoice getInvoice() { return invoice; }
}

abstract class BookingState {
    protected Booking context;
    public BookingState(Booking context) { this.context = context; }
    public abstract void confirm();
    public abstract void pickup();
    public abstract void complete(double actualHours);
    public abstract void cancel();
    protected void transition(BookingStatus next, BookingState newState) {
        context.setStatus(next);
        context.setState(newState);
    }
}
class CreatedState extends BookingState {
    public CreatedState(Booking ctx) { super(ctx); }
    public void confirm() { transition(BookingStatus.CONFIRMED, new ConfirmedState(context)); }
    public void pickup() { throw new IllegalStateException("Must confirm booking first."); }
    public void complete(double h) { throw new IllegalStateException("Rental not started."); }
    public void cancel() { transition(BookingStatus.CANCELLED, new CancelledState(context)); }
}
class ConfirmedState extends BookingState {
    public ConfirmedState(Booking ctx) { super(ctx); }
    public void confirm() { throw new IllegalStateException("Already confirmed."); }
    public void pickup() { transition(BookingStatus.ACTIVE, new ActiveState(context)); }
    public void complete(double h) { throw new IllegalStateException("Rental not started."); }
    public void cancel() { transition(BookingStatus.CANCELLED, new CancelledState(context)); }
}
class ActiveState extends BookingState {
    public ActiveState(Booking ctx) { super(ctx); }
    public void confirm() { throw new IllegalStateException("Already active."); }
    public void pickup() { throw new IllegalStateException("Already picked up."); }
    public void complete(double h) { transition(BookingStatus.COMPLETED, new CompletedState(context)); }
    public void cancel() { throw new IllegalStateException("Cannot cancel during active rental."); }
}
class CompletedState extends BookingState {
    public CompletedState(Booking ctx) { super(ctx); }
    public void confirm() { throw new IllegalStateException("Booking completed."); }
    public void pickup() { throw new IllegalStateException("Booking completed."); }
    public void complete(double h) { throw new IllegalStateException("Already completed."); }
    public void cancel() { throw new IllegalStateException("Booking completed."); }
}
class CancelledState extends BookingState {
    public CancelledState(Booking ctx) { super(ctx); }
    public void confirm() { throw new IllegalStateException("Booking cancelled."); }
    public void pickup() { throw new IllegalStateException("Booking cancelled."); }
    public void complete(double h) { throw new IllegalStateException("Booking cancelled."); }
    public void cancel() { throw new IllegalStateException("Already cancelled."); }
}

class Payment {
    private final String transactionId;
    private final double amount;
    private PaymentStatus status;
    public Payment(String txId, double amount) { this.transactionId = txId; this.amount = amount; this.status = PaymentStatus.PENDING; }
    public void setStatus(PaymentStatus s) { this.status = s; }
    public PaymentStatus getStatus() { return status; }
    public double getAmount() { return amount; }
}

class Invoice {
    private final String id;
    private final double baseFare;
    private final double penalty;
    private final double taxes;
    public Invoice(String id, double base, double penalty, double taxes) {
        this.id = id; this.baseFare = base; this.penalty = penalty; this.taxes = taxes;
    }
    public double getTotal() { return baseFare + penalty + taxes; }
}

// ================= STRATEGIES =================
interface PricingStrategy {
    double calculatePrice(VehicleType type, double hours);
}

class StandardPricingStrategy implements PricingStrategy {
    private final Map<VehicleType, Double> baseRate = Map.of(
        VehicleType.HATCHBACK, 20.0, VehicleType.SEDAN, 35.0, VehicleType.SUV, 50.0, VehicleType.LUXURY, 100.0);
    public double calculatePrice(VehicleType type, double hours) {
        return baseRate.getOrDefault(type, 30.0) * hours;
    }
}

// ================= MANAGERS =================
class InventoryManager {
    private final Map<String, List<Vehicle>> locationVehicles = new ConcurrentHashMap<>();
    private final Map<String, ReentrantLock> vehicleLocks = new ConcurrentHashMap<>();

    public List<Vehicle> search(Location loc, VehicleType type, LocalDateTime start, LocalDateTime end) {
        List<Vehicle> pool = locationVehicles.getOrDefault(loc.getId(), Collections.emptyList());
        List<Vehicle> available = new ArrayList<>();
        for (Vehicle v : pool) {
            if (v.getType() == type && v.getStatus() == VehicleStatus.AVAILABLE) {
                // In-memory availability check
                if (isAvailableInWindow(v.getId(), start, end)) available.add(v);
            }
        }
        return available;
    }

    public boolean reserve(String vehicleId, LocalDateTime start, LocalDateTime end) {
        ReentrantLock lock = vehicleLocks.computeIfAbsent(vehicleId, k -> new ReentrantLock());
        lock.lock();
        try {
            if (!isAvailableInWindow(vehicleId, start, end)) return false;
            // In production: mark reservation in DB/Redis
            return true;
        } finally { lock.unlock(); }
    }

    private boolean isAvailableInWindow(String vid, LocalDateTime start, LocalDateTime end) {
        // Simulates checking active bookings. Returns true if no overlap.
        return true; // Placeholder for actual booking overlap logic
    }

    public void addVehicle(Location loc, Vehicle v) {
        locationVehicles.computeIfAbsent(loc.getId(), k -> new ArrayList<>()).add(v);
    }
}

// ================= OBSERVER =================
interface BookingEventListener {
    void onBookingStateChange(String bookingId, BookingStatus status);
}
class NotificationService {
    private final List<BookingEventListener> listeners = new ArrayList<>();
    public void register(BookingEventListener l) { listeners.add(l); }
    public void publish(String bookingId, BookingStatus status) {
        for (BookingEventListener l : listeners) l.onBookingStateChange(bookingId, status);
    }
}

// ================= SERVICES =================
class PaymentService {
    public boolean process(Booking booking, double amount) {
        Payment payment = new Payment("TXN-" + UUID.randomUUID().toString().substring(0, 6), amount);
        try {
            // Simulate gateway
            payment.setStatus(PaymentStatus.COMPLETED);
            booking.setPayment(payment);
            return true;
        } catch (Exception e) {
            payment.setStatus(PaymentStatus.FAILED);
            return false;
        }
    }
}

class SearchService {
    private final InventoryManager inventory;
    public SearchService(InventoryManager inventory) { this.inventory = inventory; }

    public List<Vehicle> findCars(Location loc, VehicleType type, LocalDateTime start, LocalDateTime end) {
        return inventory.search(loc, type, start, end);
    }
}

class BookingService {
    private final InventoryManager inventory;
    private final PricingStrategy pricing;
    private final PaymentService paymentService;
    private final NotificationService notifier;

    public BookingService(InventoryManager inventory, PricingStrategy pricing, PaymentService ps, NotificationService n) {
        this.inventory = inventory; this.pricing = pricing; this.paymentService = ps; this.notifier = n;
    }

    public Booking createBooking(User user, Vehicle vehicle, LocalDateTime start, LocalDateTime end) {
        String id = "BK-" + UUID.randomUUID().toString().substring(0, 6);
        return new Booking(id, user, vehicle, start, end);
    }

    public boolean confirmBooking(Booking booking) {
        double hours = ChronoUnit.HOURS.between(booking.getStartTime(), booking.getEndTime());
        double estimatedCost = pricing.calculatePrice(booking.getVehicle().getType(), hours);
        
        boolean paid = paymentService.process(booking, estimatedCost);
        if (!paid) {
            booking.cancel();
            notifier.publish(booking.getId(), booking.getStatus());
            return false;
        }

        if (!inventory.reserve(booking.getVehicle().getId(), booking.getStartTime(), booking.getEndTime())) {
            booking.cancel();
            notifier.publish(booking.getId(), booking.getStatus());
            return false;
        }

        booking.confirm();
        booking.getVehicle().setStatus(VehicleStatus.RESERVED);
        notifier.publish(booking.getId(), BookingStatus.CONFIRMED);
        return true;
    }

    public boolean pickupCar(Booking booking) {
        booking.pickup();
        booking.getVehicle().setStatus(VehicleStatus.RENTED);
        notifier.publish(booking.getId(), BookingStatus.ACTIVE);
        return true;
    }

    public Invoice returnCar(Booking booking, LocalDateTime actualReturnTime) {
        double bookedHours = ChronoUnit.HOURS.between(booking.getStartTime(), booking.getEndTime());
        double actualHours = ChronoUnit.HOURS.between(booking.getStartTime(), actualReturnTime);
        
        double penalty = actualHours > bookedHours + 1.0 ? (actualHours - bookedHours) * 15.0 : 0.0; // Late penalty
        double finalCost = pricing.calculatePrice(booking.getVehicle().getType(), actualHours) + penalty;
        double taxes = finalCost * 0.12;
        
        booking.complete(actualHours);
        booking.getVehicle().setStatus(VehicleStatus.AVAILABLE);
        booking.getPayment().setStatus(PaymentStatus.REFUNDED); // Refund overcharge if prepaid
        // In real system: charge difference or settle final payment
        
        Invoice invoice = new Invoice("INV-" + UUID.randomUUID().toString().substring(0, 6), 
            finalCost - penalty, penalty, taxes);
        booking.setInvoice(invoice);
        notifier.publish(booking.getId(), BookingStatus.COMPLETED);
        return invoice;
    }

    public boolean cancelBooking(Booking booking) {
        long hoursToStart = ChronoUnit.HOURS.between(LocalDateTime.now(), booking.getStartTime());
        boolean fullRefund = hoursToStart > 24;
        // Process refund logic here
        booking.cancel();
        booking.getVehicle().setStatus(VehicleStatus.AVAILABLE);
        notifier.publish(booking.getId(), BookingStatus.CANCELLED);
        return fullRefund;
    }
}

// ================= SINGLETON =================
class CarRentalSystem {
    private static volatile CarRentalSystem instance;
    private final InventoryManager inventory;
    private final SearchService searchService;
    private final BookingService bookingService;
    private final NotificationService notifier;

    private CarRentalSystem() {
        inventory = new InventoryManager();
        PricingStrategy pricing = new StandardPricingStrategy();
        PaymentService ps = new PaymentService();
        notifier = new NotificationService();
        searchService = new SearchService(inventory);
        bookingService = new BookingService(inventory, pricing, ps, notifier);
    }

    public static CarRentalSystem getInstance() {
        if (instance == null) {
            synchronized (CarRentalSystem.class) {
                if (instance == null) instance = new CarRentalSystem();
            }
        }
        return instance;
    }

    public SearchService getSearchService() { return searchService; }
    public BookingService getBookingService() { return bookingService; }
    public InventoryManager getInventory() { return inventory; }
}

// ================= FACTORY =================
class VehicleFactory {
    public static Vehicle createCar(String id, String model, String plate, VehicleType type, Location loc, int seats) {
        return new Car(id, model, plate, type, loc, seats);
    }
}
```

---

### 4. Relationships & Class Diagram (Textual UML)

```
[CarRentalSystem] 1 <..> * [BookingService], [SearchService], [InventoryManager] (Composition/Aggregation)

[Location] 1 <1..> * [Vehicle]          (Aggregation: Vehicles can be reassigned/moved)
[User] 1 <0..> * [Booking]              (Association: User owns multiple bookings)

[Booking] 1 --> 1 [Vehicle]             (Association: Points to reserved car)
[Booking] 1 --> 1 [Payment]             (Composition: Payment lifecycle bound to booking)
[Booking] 1 --> 1 [Invoice]             (Composition: Invoice generated on completion)

[Booking] 1 --> * [BookingState]        (Composition/State Pattern: Context delegates to State)
[BookingService] --> [PricingStrategy]  (Dependency: Strategy injection)
[Booking] ..> [NotificationService]     (Dependency: Observer pattern publisher)
```

---

### 5. Design Patterns Used

| Pattern | Implementation in Code |
|---------|------------------------|
| **Singleton** | `CarRentalSystem.getInstance()` uses double-checked locking. Ensures single orchestration point per deployment. |
| **Factory Method** | `VehicleFactory.createCar()` abstracts `Car` instantiation. Allows adding `ElectricCar` or `LuxuryVan` without touching service logic. |
| **Strategy** | `PricingStrategy` interface decouples pricing algorithms from `BookingService`. `StandardPricingStrategy` can be swapped for `DynamicSurgePricing` at runtime. |
| **State Pattern** | `Booking` holds a `BookingState` reference. Concrete states (`CreatedState`, `ConfirmedState`, etc.) enforce valid transitions. Prevents illegal operations (e.g., picking up an unconfirmed car). |
| **Observer** | `NotificationService` maintains listener registry. `BookingService` publishes state changes. Enables async email/SMS, logging, or telemetry without coupling domain to notification providers. |

---

### 6. Core Flow (Very Important)

#### Car Search Flow
1. **Input**: User provides `locationId`, `dateRange`, `vehicleType`.
2. **Filter**: `SearchService` delegates to `InventoryManager`.
3. **Availability Check**: Iterates vehicles at location. Filters by type & `AVAILABLE` status. Checks in-memory/DB booking table for time-window overlaps.
4. **Rank & Return**: Sorts by price/relevance, returns list to caller.

#### Booking Flow
1. **Select & Lock**: User selects vehicle. `BookingService` creates `Booking(CREATED)`.
2. **Reserve Window**: Calls `InventoryManager.reserve()` to acquire distributed lock & block time slot.
3. **Price & Pay**: `PricingStrategy` calculates estimated cost. `PaymentService` processes upfront payment.
4. **Confirm**: If payment succeeds, `Booking.confirm()` transitions state to `CONFIRMED`. Vehicle status updates to `RESERVED`. Observer triggers confirmation email.
5. **Failure Rollback**: If payment fails or reserve blocks timeout, booking auto-cancels, lock released, vehicle remains `AVAILABLE`.

#### Pickup Flow
1. **Validation**: Staff/customer scans QR or enters `bookingId`. System verifies `CONFIRMED` status & matches user ID/license.
2. **Activate**: `BookingService.pickupCar()` called. State transitions to `ACTIVE`.
3. **Update Vehicle**: Status changes to `RENTED`. Telemetry/logging captures actual departure time.
4. **Grace Period**: If no pickup within 15 mins of `startTime`, cron auto-cancels booking.

#### Return Flow
1. **Scan & Inspect**: Staff scans `bookingId`, checks vehicle condition, fuel level, mileage.
2. **Calculate**: `returnCar()` computes actual duration vs booked duration.
3. **Penalties & Finalize**: Late return penalty applied. Taxes calculated. `Invoice` generated.
4. **Settle**: Final payment charged/refunded. State transitions to `COMPLETED`.
5. **Release**: Vehicle status set to `AVAILABLE`. Observer triggers receipt & feedback request.

---

### 7. API Design (Conceptual Only)

| Endpoint | Method | Request Payload | Response | Status Codes |
|----------|--------|-----------------|----------|--------------|
| `/search` | `POST` | `{ locationId, startDate, endDate, type, filters }` | `{ vehicles: [{id, model, plate, rate, features}] }` | 200 OK, 400 Bad Request |
| `/bookings` | `POST` | `{ userId, vehicleId, startTime, endTime }` | `{ bookingId, status: CONFIRMED, paymentLink: "...", total: 120.0 }` | 201 Created, 409 Conflict, 502 Payment Failed |
| `/bookings/{id}/cancel` | `POST` | `{ refundPolicy: "STANDARD" }` | `{ status: CANCELLED, refundAmount: 95.0, processedAt: "..." }` | 200 OK, 403 Policy Violation, 404 Not Found |
| `/bookings/{id}/pickup` | `POST` | `{ userId, otp: "1234" }` | `{ status: ACTIVE, vehiclePlate: "...", endTime: "..." }` | 200 OK, 401 Unauthorized, 409 Invalid State |
| `/bookings/{id}/return` | `POST` | `{ returnTime, fuelLevel, mileage, condition: "GOOD" }` | `{ status: COMPLETED, invoice: {base, penalty, tax, total} }` | 200 OK, 400 Missing Data, 500 Settlement Failed |

---

### 8. Database Schema Design

**Tables & Columns**
- `users` (`id` PK VARCHAR, `name` VARCHAR, `license_number` VARCHAR UNIQUE, `created_at` TIMESTAMP)
- `locations` (`id` PK VARCHAR, `city` VARCHAR, `address` VARCHAR, `timezone` VARCHAR)
- `vehicles` (`id` PK VARCHAR, `location_id` FK, `type` ENUM, `model` VARCHAR, `plate` VARCHAR UNIQUE, `status` ENUM, `is_active` BOOLEAN)
- `bookings` (`id` PK VARCHAR, `user_id` FK, `vehicle_id` FK, `status` ENUM, `start_time` TIMESTAMP, `end_time` TIMESTAMP, `actual_end_time` TIMESTAMP, `created_at` TIMESTAMP)
- `payments` (`transaction_id` PK VARCHAR, `booking_id` FK UNIQUE, `amount` DECIMAL, `status` ENUM, `gateway_ref` VARCHAR, `processed_at` TIMESTAMP)
- `invoices` (`id` PK VARCHAR, `booking_id` FK UNIQUE, `base_fare` DECIMAL, `penalty` DECIMAL, `taxes` DECIMAL, `total` DECIMAL, `issued_at` TIMESTAMP)

**Indexing Strategy**
- `vehicles(location_id, type, status)` → Composite B-Tree for fast search filtering.
- `bookings(vehicle_id)` + `bookings(user_id)` → FK lookups.
- **Time-window exclusion**: `bookings(vehicle_id)` + GiST index on `tsrange(start_time, end_time)` to prevent overlaps natively (PostgreSQL).
- `payments(status)` → Partial index `WHERE status = 'PENDING'` for reconciliation jobs.

---

### 9. Concurrency Handling

- **Double Booking Prevention**: 
  - *Application Layer*: `ReentrantLock` per `vehicleId` during `reserve()` call.
  - *Database Layer*: `SELECT id FROM vehicles WHERE id = ? FOR UPDATE SKIP LOCKED` + `EXCLUDE` constraint on time range. If two transactions try to book same car, second fails fast.
- **Optimistic Locking**: `bookings` table has `version` column. State transitions check `WHERE version = ?`. If mismatched, retry or fail.
- **Reservation TTL**: Unconfirmed bookings hold a temporary lock (Redis `SETNX` with 15-min TTL). Auto-expiration frees slot without manual rollback.
- **Idempotency**: Payment & return endpoints require `Idempotency-Key` header. Prevents duplicate charges on network retries.

---

### 10. Edge Cases

| Scenario | Handling |
|----------|----------|
| **No Cars Available** | Search returns empty. `SearchService` queries neighboring `locationId`s, suggests alternatives with ETA. |
| **Payment Failure** | `PaymentService` catches exception. Booking state reverts/gets cancelled. `InventoryManager` releases lock. User notified to retry. |
| **Booking Expiration** | Scheduled job scans `bookings WHERE status='CREATED' AND start_time < NOW() - 15m`. Auto-transitions to `CANCELLED`, frees vehicle. |
| **Late Return Penalties** | `returnCar()` compares `actualReturnTime` vs `endTime`. Adds hourly overcharge. Invoice updated. Charged to saved payment method. |
| **Vehicle Breakdown** | Staff flags `status = MAINTENANCE`. Active booking auto-reassigned if possible. Customer refunded + compensation applied. Telemetry logs fault. |

---

### 11. Sample SQL Queries

**Find available cars in location for date range:**
```sql
SELECT v.id, v.model, v.plate, v.type, p.rate_per_hour
FROM vehicles v
JOIN pricing p ON v.type = p.vehicle_type
WHERE v.location_id = $1 
  AND v.status = 'AVAILABLE'
  AND NOT EXISTS (
      SELECT 1 FROM bookings b 
      WHERE b.vehicle_id = v.id 
      AND b.status IN ('CONFIRMED', 'ACTIVE')
      AND NOT (b.end_time <= $2 OR b.start_time >= $3)
  )
ORDER BY p.rate_per_hour ASC;
```

**Get active bookings for a user:**
```sql
SELECT b.id, v.plate, v.model, b.start_time, b.end_time, b.status
FROM bookings b
JOIN vehicles v ON b.vehicle_id = v.id
WHERE b.user_id = $1 AND b.status = 'ACTIVE';
```

**Calculate total revenue (completed bookings in month):**
```sql
SELECT SUM(i.total) AS monthly_revenue, COUNT(i.id) AS completed_bookings
FROM invoices i
JOIN bookings b ON i.booking_id = b.id
WHERE b.status = 'COMPLETED' 
  AND b.actual_end_time >= DATE_TRUNC('month', CURRENT_DATE)
  AND b.actual_end_time < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month';
```

---

### 12. Trade-offs & Extensions

**Limitations**
- **In-memory state in LLD**: Real system requires Redis/DB synchronization for state & locks.
- **Synchronous payments**: Blocks booking flow. Gateway timeouts can degrade UX.
- **Monolithic inventory**: Single `InventoryManager` won't scale globally. Requires sharding by location.

**Improvements & Extensions**
1. **Distributed Caching**: Redis `ZSET` for vehicle availability by location & type. `search` hits cache first, falls back to DB. Cache invalidated on booking/return.
2. **Event-Driven Architecture**: Replace direct service calls with Kafka. `BookingRequested` → `PaymentSaga` → `InventoryReserved`. Enables async retries, dead-letter queues, and audit trails.
3. **Microservice Split**: `Search-Service` (read-heavy, cached), `Booking-Service` (transactional), `Billing-Service` (state machines, invoicing). Communicate via gRPC.
4. **Dynamic Pricing ML**: Inject `DynamicPricingStrategy` that consumes real-time occupancy, seasonality, and competitor rates to adjust `baseRate`.
5. **IoT & Telemetry Integration**: Vehicle telematics report GPS, fuel, mileage. Auto-triggers return workflow via geofence detection. Smart locks enable keyless pickup via mobile app.

This design emphasizes clean boundaries, state safety, concurrent access control, and clear extension points. It aligns with SDE2 expectations for robust OOP, scalable architecture thinking, and production-ready workflow handling.
