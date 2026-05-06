# Ride-Sharing App



### 1. Requirement Clarification

**Functional Requirements**
- Rider requests ride with pickup & drop-off coordinates, selects vehicle type.
- Real-time driver matching based on proximity, availability, and rating.
- Driver accepts/rejects/ignores request within timeout.
- Full ride lifecycle: `REQUESTED → ACCEPTED → DRIVER_ARRIVED → IN_PROGRESS → COMPLETED → CANCELLED`.
- Dynamic fare calculation (base + distance + time + surge multiplier).
- Secure payment processing & receipt generation.
- Real-time GPS location tracking & ETA updates.

**Non-Functional Requirements**
- **Low Latency**: Match & dispatch < 500ms. Location updates < 1s.
- **High Scalability**: Support millions of concurrent rides & live location streams.
- **High Availability**: Multi-AZ deployment, graceful degradation during matching outages.
- **Consistency**: Exactly-once ride acceptance, accurate billing, zero double-booking.

**Assumptions**
- GPS coordinates provided by client SDK with ~10m accuracy. Smoothing applied server-side.
- Surge pricing adjusts dynamically based on supply/demand heatmaps.
- Driver timeout for acceptance: 15s. Auto-retry next nearest driver.
- Payment handled via external gateway; retries with idempotency keys.
- Single currency & region for LLD scope.

---

### 2. Core Entities & Responsibilities

| Entity | Responsibility |
|--------|----------------|
| `RideSharingSystem` | Singleton facade. Bootstraps services, manages lifecycle, routes requests. |
| `User` / `Rider` / `Driver` | Base identity. Driver tracks status, location, vehicle. Rider tracks preferences, payment methods. |
| `Location` | Lat/Lng coordinates with distance calculation utility. |
| `Vehicle` | Type, plate, capacity, comfort tier. |
| `Ride` | Aggregate root. Tracks state, participants, route, fare, and events. |
| `Fare` | Immutable breakdown: base, distance, time, surge, tolls, final total. |
| `Payment` | Transaction record with gateway reference, status, retry logic. |
| `MatchingService` | Finds optimal available driver using geo-indexing & ranking. |
| `LocationService` | Ingests GPS streams, updates driver state, pushes real-time updates to observers. |

---

### 3. Class Design (Java Implementation)

```java
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

// ================= ENUMS =================
enum RideStatus { REQUESTED, ACCEPTED, DRIVER_ARRIVED, IN_PROGRESS, COMPLETED, CANCELLED }
enum VehicleType { SEDAN, SUV, MOTORBIKE }
enum DriverStatus { AVAILABLE, BUSY, OFFLINE }

// ================= DOMAIN MODELS =================
class Location {
    private final double lat, lng;
    public Location(double lat, double lng) { this.lat = lat; this.lng = lng; }
    public double distanceTo(Location other) {
        return Math.sqrt(Math.pow(this.lat - other.lat, 2) + Math.pow(this.lng - other.lng, 2)) * 111.0; // Mock km calc
    }
    public double getLat() { return lat; } public double getLng() { return lng; }
}

abstract class User {
    protected final String id;
    protected final String name;
    protected User(String id, String name) { this.id = id; this.name = name; }
    public String getId() { return id; } public String getName() { return name; }
}

class Rider extends User {
    public Rider(String id, String name) { super(id, name); }
}

class Vehicle {
    private final String plate;
    private final VehicleType type;
    private final String model;
    public Vehicle(String plate, VehicleType type, String model) { this.plate = plate; this.type = type; this.model = model; }
    public VehicleType getType() { return type; }
    public String getPlate() { return plate; }
}

class Driver extends User {
    private Location currentLocation;
    private DriverStatus status;
    private final Vehicle vehicle;
    private final ReentrantLock rideLock = new ReentrantLock();
    private final List<Observer> observers = new CopyOnWriteArrayList<>();

    public Driver(String id, String name, Vehicle vehicle) {
        super(id, name); this.vehicle = vehicle; this.status = DriverStatus.AVAILABLE;
    }
    public synchronized void updateLocation(Location loc) { this.currentLocation = loc; notifyLocation(); }
    public Location getLocation() { return currentLocation; }
    public DriverStatus getStatus() { return status; }
    public Vehicle getVehicle() { return vehicle; }
    public void setStatus(DriverStatus s) { this.status = s; }
    public boolean tryLockRide() { return rideLock.tryLock(); }
    public void unlockRide() { if (rideLock.isHeldByCurrentThread()) rideLock.unlock(); }
    public void addObserver(Observer o) { observers.add(o); }
    private void notifyLocation() { observers.forEach(o -> o.onLocationUpdate(this)); }
}

class Fare {
    private final double base, distance, surge, total;
    public Fare(double base, double dist, double surge) {
        this.base = base; this.distance = dist; this.surge = surge; this.total = Math.round((base + dist + surge) * 100.0) / 100.0;
    }
    public double getTotal() { return total; }
}

class Payment {
    private final String id;
    private final double amount;
    private volatile boolean completed;
    public Payment(String id, double amount) { this.id = id; this.amount = amount; }
    public void complete() { this.completed = true; }
    public boolean isCompleted() { return completed; }
}

// ================= STATE PATTERN =================
interface RideState {
    void accept(Driver d, Ride ctx); void start(Ride ctx); void driverArrived(Ride ctx);
    void complete(Ride ctx); void cancel(String by, Ride ctx); RideStatus getState();
}

class RequestedState implements RideState {
    public void accept(Driver d, Ride c) { c.setDriver(d); c.setState(new AcceptedState()); c.notify("Ride accepted"); }
    public void start(Ride c) { throw new IllegalStateException("Cannot start before accepted"); }
    public void driverArrived(Ride c) { throw new IllegalStateException("Driver not assigned"); }
    public void complete(Ride c) { throw new IllegalStateException("Not started"); }
    public void cancel(String by, Ride c) { c.setState(new CancelledState()); c.notify("Cancelled by " + by); }
    public RideStatus getState() { return RideStatus.REQUESTED; }
}
class AcceptedState implements RideState {
    public void accept(Driver d, Ride c) { throw new IllegalStateException("Already accepted"); }
    public void start(Ride c) { c.setState(new InProgressState()); c.notify("Ride started"); }
    public void driverArrived(Ride c) { c.setState(new DriverArrivedState()); c.notify("Driver arrived"); }
    public void complete(Ride c) { throw new IllegalStateException("Not started"); }
    public void cancel(String by, Ride c) { c.setState(new CancelledState()); if(c.getDriver()!=null) c.getDriver().setStatus(DriverStatus.AVAILABLE); c.notify("Cancelled by " + by); }
    public RideStatus getState() { return RideStatus.ACCEPTED; }
}
class DriverArrivedState implements RideState {
    public void accept(Driver d, Ride c) { throw new IllegalStateException("Driver arrived"); }
    public void start(Ride c) { c.setState(new InProgressState()); c.notify("Ride started"); }
    public void driverArrived(Ride c) { throw new IllegalStateException("Already arrived"); }
    public void complete(Ride c) { throw new IllegalStateException("Not started"); }
    public void cancel(String by, Ride c) { throw new IllegalStateException("Cancellation fee applies"); }
    public RideStatus getState() { return RideStatus.DRIVER_ARRIVED; }
}
class InProgressState implements RideState {
    public void accept(Driver d, Ride c) { throw new IllegalStateException("In progress"); }
    public void start(Ride c) { throw new IllegalStateException("Already started"); }
    public void driverArrived(Ride c) { throw new IllegalStateException("In progress"); }
    public void complete(Ride c) { c.setState(new CompletedState()); c.notify("Ride completed"); }
    public void cancel(String by, Ride c) { throw new IllegalStateException("Cannot cancel. Use emergency stop."); }
    public RideStatus getState() { return RideStatus.IN_PROGRESS; }
}
class CompletedState implements RideState {
    public void accept(Driver d, Ride c) { throw new IllegalStateException("Completed"); }
    public void start(Ride c) { throw new IllegalStateException("Completed"); }
    public void driverArrived(Ride c) { throw new IllegalStateException("Completed"); }
    public void complete(Ride c) { throw new IllegalStateException("Already completed"); }
    public void cancel(String by, Ride c) { throw new IllegalStateException("Completed"); }
    public RideStatus getState() { return RideStatus.COMPLETED; }
}
class CancelledState implements RideState {
    public void accept(Driver d, Ride c) { throw new IllegalStateException("Cancelled"); }
    public void start(Ride c) { throw new IllegalStateException("Cancelled"); }
    public void driverArrived(Ride c) { throw new IllegalStateException("Cancelled"); }
    public void complete(Ride c) { throw new IllegalStateException("Cancelled"); }
    public void cancel(String by, Ride c) { throw new IllegalStateException("Already cancelled"); }
    public RideStatus getState() { return RideStatus.CANCELLED; }
}

class Ride {
    private final String id;
    private final Rider rider;
    private final Location pickup, dropoff;
    private Driver driver;
    private RideState state;
    private Fare fare;
    private Payment payment;
    private final List<Observer> observers = new CopyOnWriteArrayList<>();

    public Ride(String id, Rider rider, Location pickup, Location dropoff) {
        this.id = id; this.rider = rider; this.pickup = pickup; this.dropoff = dropoff;
        this.state = new RequestedState();
    }
    public void setState(RideState s) { this.state = s; }
    public void accept(Driver d) { state.accept(d, this); }
    public void start() { state.start(this); }
    public void driverArrived() { state.driverArrived(this); }
    public void complete() { state.complete(this); }
    public void cancel(String by) { state.cancel(by, this); }
    public RideStatus getStatus() { return state.getState(); }
    public Driver getDriver() { return driver; }
    public void setDriver(Driver d) { this.driver = d; }
    public Rider getRider() { return rider; }
    public Location getPickup() { return pickup; }
    public Location getDropoff() { return dropoff; }
    public void addObserver(Observer o) { observers.add(o); }
    public void notify(String msg) { observers.forEach(o -> o.onStatusUpdate(this, msg)); }
    public void setFare(Fare f) { this.fare = f; }
    public Fare getFare() { return fare; }
    public void setPayment(Payment p) { this.payment = p; }
}

// ================= OBSERVER PATTERN =================
interface Observer { void onLocationUpdate(Driver d); void onStatusUpdate(Ride r, String msg); }
class ClientObserver implements Observer {
    public void onLocationUpdate(Driver d) { /* Push to rider app */ }
    public void onStatusUpdate(Ride r, String msg) { /* Push to rider app */ }
}

// ================= STRATEGY PATTERN =================
interface PricingStrategy { double calculate(Location pickup, Location dropoff, VehicleType type); }
class BasePricing implements PricingStrategy {
    private final Map<VehicleType, Double> ratePerKm = Map.of(VehicleType.SEDAN, 1.5, VehicleType.SUV, 2.2, VehicleType.MOTORBIKE, 1.0);
    public double calculate(Location p1, Location p2, VehicleType t) { return p1.distanceTo(p2) * ratePerKm.get(t); }
}
class SurgePricing implements PricingStrategy {
    private final PricingStrategy base; private final double multiplier;
    public SurgePricing(PricingStrategy base, double m) { this.base = base; this.multiplier = m; }
    public double calculate(Location p1, Location p2, VehicleType t) { return base.calculate(p1, p2, t) * multiplier; }
}

// ================= FACTORY PATTERN =================
class RideFactory {
    public static Ride create(String id, Rider rider, Location p1, Location p2) {
        return new Ride(id, rider, p1, p2);
    }
}

// ================= SERVICES & MANAGERS =================
class LocationService {
    private final Map<String, Driver> trackedDrivers = new ConcurrentHashMap<>();
    public void track(Driver d) { trackedDrivers.put(d.getId(), d); }
    public void update(String driverId, Location loc) {
        Driver d = trackedDrivers.get(driverId);
        if (d != null) d.updateLocation(loc);
    }
}

class DriverManager {
    private static volatile DriverManager instance;
    private final Map<String, Driver> registry = new ConcurrentHashMap<>();
    private DriverManager() {}
    public static DriverManager getInstance() {
        if (instance == null) { synchronized(DriverManager.class) { if(instance==null) instance=new DriverManager(); } }
        return instance;
    }
    public void register(Driver d) { registry.put(d.getId(), d); }
    public Driver get(String id) { return registry.get(id); }
    public Collection<Driver> getAvailableNear(Location loc, double radiusKm) {
        return registry.values().stream().filter(d -> d.getStatus() == DriverStatus.AVAILABLE && loc.distanceTo(d.getLocation()) <= radiusKm).toList();
    }
}

class MatchingService {
    private static volatile MatchingService instance;
    private final DriverManager driverMgr = DriverManager.getInstance();
    private MatchingService() {}
    public static MatchingService getInstance() {
        if (instance == null) { synchronized(MatchingService.class) { if(instance==null) instance=new MatchingService(); } }
        return instance;
    }
    public Driver findNearest(Ride ride) {
        Collection<Driver> candidates = driverMgr.getAvailableNear(ride.getPickup(), 5.0);
        return candidates.stream().min(Comparator.comparingDouble(d -> ride.getPickup().distanceTo(d.getLocation()))).orElse(null);
    }
}

class PricingService {
    private PricingStrategy strategy = new BasePricing();
    public void setStrategy(PricingStrategy s) { this.strategy = s; }
    public double estimate(Location p1, Location p2, VehicleType type) { return strategy.calculate(p1, p2, type); }
    public Fare calculate(Ride ride) {
        double distCost = strategy.calculate(ride.getPickup(), ride.getDropoff(), ride.getDriver().getVehicle().getType());
        return new Fare(5.0, distCost, 0.0);
    }
}

class PaymentService {
    public boolean process(Ride ride) {
        Payment p = new Payment("PAY-" + System.currentTimeMillis(), ride.getFare().getTotal());
        p.complete(); // Mock gateway
        ride.setPayment(p);
        return true;
    }
}

class RideService {
    private final MatchingService matcher = MatchingService.getInstance();
    private final PricingService pricing = new PricingService();
    private final PaymentService payment = new PaymentService();
    private final Map<String, Ride> rideStore = new ConcurrentHashMap<>();

    public Ride requestRide(String rideId, Rider rider, Location p1, Location p2, VehicleType type) {
        Ride ride = RideFactory.create(rideId, rider, p1, p2);
        ride.addObserver(new ClientObserver());
        rideStore.put(rideId, ride);

        Driver driver = matcher.findNearest(ride);
        if (driver != null && driver.tryLockRide()) {
            try {
                ride.accept(driver);
            } finally { driver.unlockRide(); }
        } else {
            ride.cancel("No available driver");
        }
        return ride;
    }

    public void start(String rideId) { rideStore.get(rideId).start(); }
    public void complete(String rideId) {
        Ride r = rideStore.get(rideId);
        r.complete();
        r.setFare(pricing.calculate(r));
        payment.process(r);
        r.getDriver().setStatus(DriverStatus.AVAILABLE);
    }
    public void cancel(String rideId, String userId) { rideStore.get(rideId).cancel(userId); }
}

// ================= SINGLETON FACADE =================
class RideSharingSystem {
    private static volatile RideSharingSystem instance;
    private final RideService rideService;
    private final LocationService locationService;

    private RideSharingSystem() {
        rideService = new RideService();
        locationService = new LocationService();
    }
    public static RideSharingSystem getInstance() {
        if (instance == null) { synchronized(RideSharingSystem.class) { if(instance==null) instance=new RideSharingSystem(); } }
        return instance;
    }
    public RideService getRideService() { return rideService; }
    public LocationService getLocationService() { return locationService; }
}
```

---

### 4. Relationships & Class Diagram (Textual UML)

```
[RideSharingSystem] 1 <1..> 1 [RideService]          (Composition: Facade delegation)
[RideSharingSystem] 1 --> 1 [LocationService]        (Dependency: Real-time tracking)

[Ride] 1 *--> 1 [RideState]                          (Composition: State Context)
[RideState] <|-- [RequestedState]                    (Inheritance)
[RideState] <|-- [AcceptedState], [InProgressState]  (Inheritance)
[RideState] <|-- [CompletedState], [CancelledState]  (Inheritance)

[Ride] 1 o--> 1 [Driver]                             (Aggregation: Assigned during lifecycle)
[Ride] 1 *--> 1 [Fare]                               (Composition: Bound to ride completion)
[Ride] 1 *--> 1 [Payment]                            (Composition: Transaction record)

[Driver] 1 --> 1 [Vehicle]                           (Aggregation: Driver owns vehicle)
[Driver] 1 --> 1 [Observer]                          (Dependency: Location broadcasts)

[PricingStrategy] <|-- [BasePricing]                 (Strategy: Normal rates)
[PricingStrategy] <|-- [SurgePricing]                (Strategy: Dynamic multiplier)

[RideFactory] ..> [Ride]                             (Creation)
[Observer] <|-- [ClientObserver]                     (Implementation)
```

---

### 5. Design Patterns Used

| Pattern | Implementation in Code |
|---------|------------------------|
| **Singleton** | `RideSharingSystem`, `MatchingService`, `DriverManager`. Ensures centralized driver registry, consistent matching logic, and thread-safe global state. |
| **Factory** | `RideFactory.create()` abstracts ride instantiation. Decouples service layer from constructor complexity & validation. Extensible for `PoolRide` or `ScheduledRide`. |
| **Strategy** | `PricingStrategy` interface with `BasePricing` & `SurgePricing`. `PricingService` injects strategy at runtime. Allows dynamic pricing algorithm swaps without modifying ride flow. |
| **State** | `RideState` controls lifecycle transitions. Concrete states validate allowed operations (e.g., reject `start()` in `REQUESTED`). Prevents illegal jumps & encapsulates side-effects. |
| **Observer** | `Observer` interface + `CopyOnWriteArrayList` in `Ride`/`Driver`. Fires on location updates & state changes. Decouples core logic from push notifications, WebSocket streaming, & analytics. |

---

### 6. Core Flow (Very Important)

#### Ride Request Flow
1. **Input**: Rider submits pickup/drop, vehicle type.
2. **Estimate**: `PricingService.estimate()` returns upfront fare.
3. **Match**: `MatchingService.findNearest()` queries available drivers within 5km, sorted by distance & rating.
4. **Dispatch**: System sends request to nearest driver. Starts 15s timer.
5. **Timeout/Fallback**: If no accept, retries next candidate up to max attempts. Cancels if pool exhausted.

#### Driver Matching Flow
1. **Filter**: `DriverManager.getAvailableNear()` uses spatial index to fetch candidates.
2. **Rank**: Scores by `(proximity_weight * dist) + (rating_weight * score) - (surge_penalty)`.
3. **Lock**: `tryLockRide()` acquires distributed lock on driver. Prevents double-assignment.
4. **Notify**: Push notification & in-app ping sent to driver. State → `REQUESTED`.

#### Ride Acceptance Flow
1. **Driver Action**: Accepts request. Lock released.
2. **State Transition**: `ride.accept(driver)` → `ACCEPTED`.
3. **Routing**: Navigation SDK launched for driver. ETA pushed to rider.
4. **Revert on Ignore**: If driver times out/declines, lock released, state reverts to `REQUESTED`, matching retries.

#### Ride Start & Tracking Flow
1. **Arrival**: Driver reaches pickup, taps "Arrived". State → `DRIVER_ARRIVED`.
2. **Start**: Rider enters car, taps "Start Ride" or driver starts. State → `IN_PROGRESS`.
3. **Tracking**: `LocationService` ingests GPS pings (1Hz). `LocationService.update()` broadcasts via Observer to rider UI.
4. **Smoothing**: Server applies Kalman filter to remove GPS jitter before pushing.

#### Ride Completion Flow
1. **Drop-off**: Driver arrives at destination, taps "End Ride".
2. **State**: `ride.complete()` → `COMPLETED`. Driver status → `AVAILABLE`.
3. **Fare Calc**: `PricingService.calculate()` computes exact fare based on actual distance/time.
4. **Payment**: `PaymentService.process()` charges rider. Receipt generated. Observer notifies both parties.

#### Cancellation Flow
1. **Rider Cancel**: If `REQUESTED/ACCEPTED`, `ride.cancel("rider")` → `CANCELLED`. Driver freed, no charge (or fee if late).
2. **Driver Cancel**: After `ACCEPTED`, driver cancels. State → `CANCELLED`. Penalty applied to driver account. Matching retries immediately.
3. **Mid-Ride Emergency**: `InProgressState` blocks normal cancel. Requires manual safety intervention via app support.

---

### 7. API Design (Conceptual Only)

| Endpoint | Method | Request Payload | Response | Status Codes |
|----------|--------|-----------------|----------|--------------|
| `requestRide` | `POST /rides` | `{ pickup: {lat,lng}, dropoff: {lat,lng}, vehicleType: "SEDAN", paymentMethodId: "pm_123" }` | `{ rideId: "r_8a2", status: "REQUESTED", fare: 12.50, estimatedPickupMins: 4 }` | 201 Created, 400 Invalid Route, 402 No Drivers |
| `acceptRide` | `POST /rides/{id}/accept` | `{ driverId: "d_45", action: "ACCEPT" }` | `{ status: "ACCEPTED", driver: {name, plate}, eta: 3 }` | 200 OK, 409 Already Accepted, 410 Expired |
| `cancelRide` | `POST /rides/{id}/cancel` | `{ cancelBy: "RIDER", reason: "WAIT_TOO_LONG" }` | `{ status: "CANCELLED", feeCharged: 0.00, refundAmount: 0.00 }` | 200 OK, 409 Already Completed |
| `updateDriverLocation` | `PATCH /drivers/{id}/location` | `{ lat: 37.77, lng: -122.41, heading: 90 }` | `{ status: "OK", nextPingInMs: 1000 }` | 200 OK, 401 Unauthorized |
| `getRideStatus` | `GET /rides/{id}` | N/A | `{ status: "IN_PROGRESS", driver: {name, liveLocation}, fareSoFar: 8.20, etaToDest: 5 }` | 200 OK, 404 Not Found |

---

### 8. Database Schema Design

**Tables & Columns**
- `users` (`id` PK VARCHAR, `name` VARCHAR, `role` ENUM, `email` VARCHAR, `created_at` TIMESTAMP)
- `drivers` (`user_id` PK FK, `status` ENUM, `rating` DECIMAL, `total_trips` INT)
- `vehicles` (`id` PK VARCHAR, `driver_id` FK, `type` ENUM, `plate` VARCHAR, `model` VARCHAR)
- `rides` (`id` PK UUID, `rider_id` FK, `driver_id` FK, `pickup_lat` DOUBLE, `pickup_lng` DOUBLE, `drop_lat` DOUBLE, `drop_lng` DOUBLE, `status` ENUM, `base_fare` DECIMAL, `total_fare` DECIMAL, `created_at` TIMESTAMP)
- `ride_events` (`id` PK BIGINT, `ride_id` FK, `type` ENUM, `payload` JSONB, `timestamp` TIMESTAMP)
- `payments` (`id` PK VARCHAR, `ride_id` FK UNIQUE, `amount` DECIMAL, `status` ENUM, `gateway_ref` VARCHAR)

**Indexing Strategy**
- `rides(rider_id, created_at DESC)` → Fast ride history lookup.
- `rides(driver_id, created_at DESC)` → Driver earnings & history.
- `rides(status, created_at)` → Filtered index for active/abandoned ride cleanup jobs.
- **Spatial Index**: `drivers` table uses PostGIS `GEOGRAPHY(Point, 4326)` or Redis `GEOADD`. `ST_DWithin` for O(log N) proximity queries.
- `ride_events(ride_id, timestamp)` → Ordered log for replay & dispute resolution.

---

### 9. Concurrency & Scalability

- **Distributed Locking**: Ride acceptance uses Redis `SETNX driver:lock:{id} {rideId} EX 15`. Ensures only one driver claims a ride. Prevents race conditions under high concurrency.
- **Geo-Partitioning**: Location data sharded by city/region. Matching runs against local shards to reduce cross-region latency.
- **Event-Driven Architecture**: Kafka topics `ride.state.change`, `driver.location.update`. Consumers handle async tasks: billing, notifications, analytics. Decouples real-time matching from side effects.
- **Idempotency**: All critical endpoints require `Idempotency-Key` header. DB stores `(key, response)` to safely retry network drops without double-charging or duplicate assignments.
- **Stateless Matching Workers**: Horizonally scaled behind ALB. Share driver state via Redis Cluster. Can process 50K matches/sec.

---

### 10. Edge Cases

| Scenario | Handling |
|----------|----------|
| **No Drivers Available** | Matching retries with expanding radius. After 60s, cancels ride, refunds pre-auth, suggests scheduled ride. |
| **Driver Cancels Post-Accept** | `AcceptedState` → `CANCELLED`. Penalty logged. System immediately triggers fallback matching to nearest next driver. |
| **Rider Cancels Mid-Ride** | `InProgressState` blocks standard cancel. Routes to safety/support workflow. Billing continues until driver officially ends trip. |
| **GPS Inaccuracies/Drift** | Server applies moving-average smoothing & map-matching. Invalid jumps (>100m/s) discarded. ETA recalculated on corrected position. |
| **Payment Failure** | Ride completes normally. Payment marked `PENDING`. Retry queue charges saved method every 5m for 24h. Rider blocked until resolved. |

---

### 11. Sample SQL Queries

**Find nearby drivers (PostGIS syntax):**
```sql
SELECT d.user_id, d.rating, ST_Distance(d.location, ST_MakePoint($1, $2)::geography) AS dist_km
FROM drivers d
JOIN users u ON d.user_id = u.id
WHERE d.status = 'AVAILABLE' 
  AND ST_DWithin(d.location, ST_MakePoint($1, $2)::geography, 5000)
ORDER BY dist_km ASC, d.rating DESC
LIMIT 10;
```

**Get active rides for a driver:**
```sql
SELECT r.id, r.rider_id, r.pickup_lat, r.pickup_lng, r.status, r.total_fare
FROM rides r
WHERE r.driver_id = $1 
  AND r.status IN ('ACCEPTED', 'DRIVER_ARRIVED', 'IN_PROGRESS')
ORDER BY r.created_at DESC;
```

**Fetch rider trip history (last 30 days):**
```sql
SELECT r.id, r.driver_id, r.total_fare, r.status, r.created_at
FROM rides r
WHERE r.rider_id = $1 
  AND r.created_at >= NOW() - INTERVAL '30 days'
  AND r.status = 'COMPLETED'
ORDER BY r.created_at DESC;
```

---

### 12. Trade-offs & Extensions

**Limitations of Current Design**
- **In-Memory Registry**: `DriverManager` uses `ConcurrentHashMap`. Requires Redis/DB persistence for multi-node resilience.
- **Synchronous Matching**: Blocks caller until nearest driver responds. High contention in dense areas.
- **Basic Pricing**: Linear distance formula ignores traffic, tolls, & dynamic supply/demand curves.

**Improvements & Extensions**
1. **Distributed Matching Engine**: Kafka-based event stream. `RideRequested` → `MatchWorker` → `DriverOffered`. Async fan-out with circuit breakers for high availability.
2. **Surge Pricing Algorithm**: Real-time heatmap of demand/supply ratio. Injects `SurgePricingStrategy` dynamically. ML models predict demand spikes 15-30 mins ahead.
3. **Pool Rides (Shared)**: Extend `Ride` to `SharedRide`. Graph-matching algorithm clusters riders with overlapping routes. Fare split logic + detour optimization (OSRM/GraphHopper).
4. **Driver Ranking & Reputation**: Multi-dimensional scoring: acceptance rate, rating, cancellation frequency, safety incidents. Influences match priority & incentives.
5. **Real-Time Route Optimization**: Integrate traffic APIs & turn-by-turn SDK. Dynamic ETA adjustments pushed to rider. Rerouting on congestion detected.
6. **Multi-Region Active-Active**: Global traffic routing (Route53/Cloudflare). Region-scoped ride state. Cross-region payment settlement via async reconciliation.
