# Inventory management system
### 1. Requirement Clarification

**Functional Requirements**
- **Product/SKU Management**: Create base products, generate variant SKUs (size/color), update metadata.
- **Inventory Tracking**: Maintain `physical`, `available`, and `reserved` quantities per warehouse.
- **Stock Operations**: `STOCK_IN` (receiving), `STOCK_OUT` (shipping), `RESERVE` (order creation), `RELEASE` (cancellation).
- **Reservation Management**: Hold stock temporarily for pending orders; auto-release on timeout.
- **Low-Stock Alerts**: Configurable thresholds per SKU/warehouse; trigger notifications.
- **Transaction History**: Immutable audit trail of every inventory movement.

**Non-Functional Requirements**
- **Consistency**: Strong consistency on stock updates; zero overselling allowed.
- **High Availability**: 99.99% uptime; read replicas for stock checks.
- **Scalability**: Horizontal scaling across warehouses/regions; partitioned data.
- **Low Latency**: < 50ms for availability checks, < 100ms for reservation commits.

**Assumptions**
- SKU granularity: Each variant has its own stock count.
- Inventory is warehouse-specific; no cross-warehouse pooling in V1.
- ACID compliance required for reservation/deduction transactions.
- Read-heavy (stock checks), write-heavy during flash sales/events.
- Alerts sent via async queue to external notification service.

---

### 2. Core Entities & Responsibilities

| Entity | Responsibility |
|--------|----------------|
| `Product` | Base catalog item. Contains metadata, category, and variant rules. |
| `SKU` | Stock Keeping Unit. Unique per variant. Links to product and inventory rules. |
| `Warehouse` | Physical/digital storage location. Tracks capacity, operational status, and region. |
| `InventoryItem` | Junction entity linking `SKU` + `Warehouse`. Manages `physicalQty`, `reservedQty`, `availableQty`, and versioning for concurrency. |
| `StockTransaction` | Immutable ledger record of inventory movement (type, quantity, timestamp, reference ID). |
| `Reservation` | Temporary stock hold. Linked to order, SKU, warehouse, and expiry timestamp. |
| `AlertThreshold` | Configurable rule for triggering low-stock notifications per SKU/warehouse. |
| `InventorySystem` | Facade/Singleton orchestrating services, enforcing consistency boundaries, and routing commands. |

---

### 3. Class Design (Java Implementation)

```java
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

// ================= ENUMS =================
enum StockAction { IN, OUT, RESERVE, RELEASE, ADJUST }
enum TransactionStatus { COMPLETED, FAILED, PENDING, REVERSED }
enum AlertLevel { LOW, CRITICAL, OUT_OF_STOCK }

// ================= DOMAIN MODELS =================
class Product {
    private final String id;
    private final String name;
    private final String category;

    public Product(String id, String name, String category) {
        this.id = id; this.name = name; this.category = category;
    }
    public String getId() { return id; }
    public String getName() { return name; }
}

class SKU {
    private final String id;
    private final String productId;
    private final Map<String, String> attributes; // e.g., {"color": "red", "size": "L"}

    public SKU(String id, String productId, Map<String, String> attributes) {
        this.id = id; this.productId = productId; this.attributes = attributes;
    }
    public String getId() { return id; }
    public String getProductId() { return productId; }
}

class Warehouse {
    private final String id;
    private final String name;
    private final String region;
    private final int maxCapacity;
    private int currentOccupancy;

    public Warehouse(String id, String name, String region, int maxCapacity) {
        this.id = id; this.name = name; this.region = region; this.maxCapacity = maxCapacity;
    }
    public String getId() { return id; }
    public String getRegion() { return region; }
    public boolean hasCapacity(int qty) { return (currentOccupancy + qty) <= maxCapacity; }
}

class InventoryItem {
    private final String skuId;
    private final String warehouseId;
    private int physicalQty;
    private int reservedQty;
    private int availableQty;
    private int version; // Optimistic locking / Row version
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private AlertThreshold threshold;

    public InventoryItem(String skuId, String warehouseId, int initialQty, AlertThreshold threshold) {
        this.skuId = skuId; this.warehouseId = warehouseId;
        this.physicalQty = initialQty; this.availableQty = initialQty;
        this.version = 1; this.threshold = threshold;
    }

    public int getAvailableQty() { rwLock.readLock().lock(); try { return availableQty; } finally { rwLock.readLock().unlock(); } }

    // Atomic reservation attempt
    public synchronized boolean tryReserve(int qty) {
        if (availableQty < qty) return false;
        availableQty -= qty;
        reservedQty += qty;
        version++;
        return true;
    }

    // Atomic release
    public synchronized void release(int qty) {
        if (reservedQty < qty) throw new IllegalStateException("Release exceeds reserved quantity");
        reservedQty -= qty;
        availableQty += qty;
        version++;
    }

    // Stock in/out adjustment
    public synchronized void adjust(StockAction action, int qty) {
        if (action == StockAction.IN) {
            physicalQty += qty;
            availableQty += qty;
        } else if (action == StockAction.OUT) {
            if (physicalQty < qty) throw new IllegalStateException("Insufficient physical stock");
            physicalQty -= qty;
            availableQty -= qty; // Physical and available move together on fulfillment
        } else if (action == StockAction.ADJUST) {
            int diff = qty - physicalQty;
            physicalQty = qty;
            availableQty += diff;
        }
        version++;
    }

    public boolean shouldAlert() { return threshold != null && availableQty <= threshold.getLimit(); }
    public AlertLevel getAlertLevel() { return availableQty == 0 ? AlertLevel.OUT_OF_STOCK : (availableQty <= threshold.getCriticalLimit() ? AlertLevel.CRITICAL : AlertLevel.LOW); }
    public String getSkuId() { return skuId; }
    public String getWarehouseId() { return warehouseId; }
}

class AlertThreshold {
    private final int limit;
    private final int criticalLimit;
    public AlertThreshold(int limit, int criticalLimit) { this.limit = limit; this.criticalLimit = criticalLimit; }
    public int getLimit() { return limit; }
    public int getCriticalLimit() { return criticalLimit; }
}

class StockTransaction {
    private final String id;
    private final String referenceId; // Order ID / Receipt ID
    private final String skuId;
    private final String warehouseId;
    private final StockAction action;
    private final int quantity;
    private final LocalDateTime timestamp;

    public StockTransaction(String refId, String skuId, String warehouseId, StockAction action, int qty) {
        this.id = UUID.randomUUID().toString().substring(0, 8);
        this.referenceId = refId; this.skuId = skuId; this.warehouseId = warehouseId;
        this.action = action; this.quantity = qty; this.timestamp = LocalDateTime.now();
    }
    public String getId() { return id; }
    public String getSkuId() { return skuId; }
}

class Reservation {
    private final String id;
    private final String orderId;
    private final String skuId;
    private final String warehouseId;
    private final int quantity;
    private final LocalDateTime expiresAt;
    private boolean isActive;

    public Reservation(String orderId, String skuId, String warehouseId, int qty, int ttlMinutes) {
        this.id = UUID.randomUUID().toString().substring(0, 8);
        this.orderId = orderId; this.skuId = skuId; this.warehouseId = warehouseId;
        this.quantity = qty; this.expiresAt = LocalDateTime.now().plusMinutes(ttlMinutes);
        this.isActive = true;
    }
    public boolean isExpired() { return LocalDateTime.now().isAfter(expiresAt) && isActive; }
    public void cancel() { this.isActive = false; }
    public String getSkuId() { return skuId; }
    public String getWarehouseId() { return warehouseId; }
    public int getQuantity() { return quantity; }
    public String getId() { return id; }
}

// ================= COMMAND PATTERN =================
interface InventoryCommand { void execute(StockManager manager); }

class ReserveStockCommand implements InventoryCommand {
    private final String orderId, skuId, warehouseId;
    private final int qty;
    public ReserveStockCommand(String orderId, String skuId, String warehouseId, int qty) {
        this.orderId = orderId; this.skuId = skuId; this.warehouseId = warehouseId; this.qty = qty;
    }
    public void execute(StockManager mgr) { mgr.reserve(orderId, skuId, warehouseId, qty); }
}

// ================= STRATEGY PATTERN =================
interface AllocationStrategy { String selectWarehouse(String skuId, int qty, List<String> warehouseIds); }
class NearestWarehouseStrategy implements AllocationStrategy {
    public String selectWarehouse(String skuId, int qty, List<String> warehouseIds) {
        return warehouseIds.isEmpty() ? null : warehouseIds.get(0); // Simplified
    }
}

// ================= OBSERVER PATTERN =================
interface StockAlertObserver { void onThresholdBreached(String skuId, String warehouseId, AlertLevel level, int qty); }
class AlertService implements StockAlertObserver {
    private final List<StockAlertObserver> downstreamConsumers = new ArrayList<>();
    public void register(StockAlertObserver consumer) { downstreamConsumers.add(consumer); }
    public void onThresholdBreached(String skuId, String warehouseId, AlertLevel level, int qty) {
        // In prod: publish to Kafka/SQS, trigger email/webhook
        for (var c : downstreamConsumers) c.onThresholdBreached(skuId, warehouseId, level, qty);
    }
}

// ================= SERVICES & MANAGERS =================
class SKUFactory {
    public static SKU create(String productId, String variantSuffix, Map<String, String> attrs) {
        return new SKU(productId + "-" + variantSuffix, productId, attrs);
    }
}

class StockManager {
    private final Map<String, InventoryItem> inventory = new ConcurrentHashMap<>();
    private final Map<String, Reservation> reservations = new ConcurrentHashMap<>();
    private final AlertService alertService;

    public StockManager(AlertService alertService) { this.alertService = alertService; }

    public InventoryItem addItem(InventoryItem item) {
        inventory.put(item.getSkuId() + "::" + item.getWarehouseId(), item);
        return item;
    }

    public InventoryItem getItem(String skuId, String warehouseId) {
        return inventory.get(skuId + "::" + warehouseId);
    }

    // Core reservation logic with atomicity
    public boolean reserve(String orderId, String skuId, String warehouseId, int qty) {
        String key = skuId + "::" + warehouseId;
        InventoryItem item = inventory.get(key);
        if (item == null || !item.tryReserve(qty)) return false;
        
        Reservation res = new Reservation(orderId, skuId, warehouseId, qty, 30);
        reservations.put(orderId + "::" + skuId, res);
        
        if (item.shouldAlert()) alertService.onThresholdBreached(skuId, warehouseId, item.getAlertLevel(), item.getAvailableQty());
        return true;
    }

    public void releaseReservation(String orderId, String skuId) {
        String key = orderId + "::" + skuId;
        Reservation res = reservations.get(key);
        if (res != null && res.isActive) {
            res.cancel();
            InventoryItem item = inventory.get(res.getSkuId() + "::" + res.getWarehouseId());
            if (item != null) {
                item.release(res.getQuantity());
                reservations.remove(key);
            }
        }
    }

    public void processStockIn(String skuId, String warehouseId, int qty) {
        InventoryItem item = getItem(skuId, warehouseId);
        if (item != null) item.adjust(StockAction.IN, qty);
    }
}

class InventoryService {
    private final StockManager stockManager;
    private final AllocationStrategy allocationStrategy;

    public InventoryService(StockManager stockManager, AllocationStrategy strategy) {
        this.stockManager = stockManager;
        this.allocationStrategy = strategy;
    }

    public boolean execute(InventoryCommand command) {
        command.execute(stockManager);
        return true;
    }

    public String getAvailableStock(String skuId) {
        // In prod: query read-replica or Redis cache
        return "Total available across warehouses";
    }
}

// ================= SINGLETON =================
class InventorySystem {
    private static volatile InventorySystem instance;
    private final InventoryService inventoryService;
    private final AlertService alertService;
    private final StockManager stockManager;

    private InventorySystem() {
        alertService = new AlertService();
        stockManager = new StockManager(alertService);
        inventoryService = new InventoryService(stockManager, new NearestWarehouseStrategy());
    }

    public static InventorySystem getInstance() {
        if (instance == null) {
            synchronized (InventorySystem.class) {
                if (instance == null) instance = new InventorySystem();
            }
        }
        return instance;
    }

    public InventoryService getService() { return inventoryService; }
    public StockManager getManager() { return stockManager; }
    public AlertService getAlerts() { return alertService; }
}
```

---

### 4. Relationships & Class Diagram (Textual UML)

```
[InventorySystem] 1 <..> 1 [InventoryService]      (Composition)
[InventoryService] 1 --> 1 [StockManager]          (Composition)
[InventoryService] 1 --> 1 [AllocationStrategy]    (Dependency/Strategy Injection)

[Warehouse] 1 o--> * [InventoryItem]               (Aggregation: Items belong to warehouse)
[SKU] 1 o--> * [InventoryItem]                     (Aggregation: Items track SKU stock)
[InventoryItem] 1 --> 1 [AlertThreshold]           (Composition: Config bound to item)

[StockTransaction] 1 --> 1 [InventoryItem]         (Association: Ledger entry)
[Reservation] 1 --> 1 [InventoryItem]              (Association: Points to reserved stock)
[Reservation] 1 --> 1 [Order]                      (External dependency)

[InventoryCommand] <|-- [ReserveStockCommand]      (Inheritance: Command Pattern)
[StockAlertObserver] <|-- [AlertService]           (Interface implementation)
[InventoryItem] ..> [StockAlertObserver]           (Dependency: Publishes breaches)
```

---

### 5. Design Patterns Used

| Pattern | Implementation in Code |
|---------|------------------------|
| **Singleton** | `InventorySystem` uses double-checked locking. Guarantees single orchestration point and shared state across the JVM. |
| **Factory Method** | `SKUFactory.create()` encapsulates variant ID generation logic. Decouples SKU creation from service layer. |
| **Command Pattern** | `InventoryCommand` interface + `ReserveStockCommand` encapsulates stock operations. Enables logging, retry queues, and future undo/compensation without exposing internals. |
| **Strategy Pattern** | `AllocationStrategy` interface decouples warehouse selection logic from reservation flow. `NearestWarehouseStrategy` can be swapped for `LeastStockStrategy` or `PriorityRegionStrategy`. |
| **Observer Pattern** | `AlertService` implements `StockAlertObserver`. `InventoryItem` triggers `onThresholdBreached()` on stock changes. Decouples alert routing from core inventory logic. |

---

### 6. Core Flow (Very Important)

#### Add/Update Product Flow
1. **Create Base**: `Product` entity instantiated with catalog metadata.
2. **Variant Generation**: `SKUFactory` creates unique SKUs per attribute combination.
3. **Warehouse Assignment**: System registers each SKU against active warehouses.
4. **Initialize Item**: `InventoryItem(0)` created. Version = 1. Thresholds applied. State: `AVAILABLE=0, PHYSICAL=0, RESERVED=0`.

#### Stock In Flow
1. **Receive Goods**: Supplier delivers `qty`. GRN (Goods Receipt Note) generated.
2. **Validate**: `StockManager` locates `InventoryItem` via `skuId` + `warehouseId`.
3. **Update**: `adjust(IN, qty)` increments `physicalQty` and `availableQty` atomically. Version increments.
4. **Audit**: `StockTransaction(IN)` persisted. Alert service checks threshold.

#### Stock Reservation Flow
1. **Order Placement**: Checkout triggers `ReserveStockCommand(orderId, skuId, warehouseId, qty)`.
2. **Acquire Lock**: `InventoryItem.tryReserve(qty)` acquires intrinsic lock (or DB row lock).
3. **Check & Decrement**: Verifies `availableQty >= qty`. If true: `available -= qty`, `reserved += qty`, `version++`.
4. **Create Reservation**: `Reservation` object stored with 30-min TTL.
5. **Return**: Success to order service. If fail, order service retries or cancels.

#### Stock Release Flow (Cancellation)
1. **Cancel Trigger**: User cancels order before confirmation.
2. **Locate Reservation**: `StockManager` fetches active `Reservation`.
3. **Release Lock**: `item.release(qty)` runs atomically: `reserved -= qty`, `available += qty`, `version++`.
4. **Cleanup**: Reservation marked `inactive`, removed from active map. Audit log created.

#### Stock Deduction Flow (Fulfillment)
1. **Payment Confirmed**: Order status → `PAID`.
2. **Pick & Pack**: Warehouse executes `adjust(OUT, qty)` (fulfillment). `physicalQty` decreases. `reservedQty` implicitly released as reservation is consumed.
3. **Finalize**: Reservation marked fulfilled, removed from active map. Shipping label generated.

#### Low Stock Alert Flow
1. **Transaction Trigger**: Any `adjust` or `tryReserve` modifies `availableQty`.
2. **Evaluate Threshold**: Post-update, `item.shouldAlert()` checks `available <= threshold.limit`.
3. **Fire Event**: `AlertService.onThresholdBreached()` publishes event.
4. **Consume**: Downstream consumers route to Slack/Email/Procurement System. Rate limiting prevents alert spam.

---

### 7. API Design (Conceptual Only)

| Operation | Conceptual Signature | Request Payload | Response | Status Codes |
|-----------|----------------------|-----------------|----------|--------------|
| `reserveStock` | `reserve(orderId, items[], warehouseId)` | `{ orderId: "ORD-101", items: [{sku: "SKU-A", qty: 2}] }` | `{ success: true, reservationId: "RES-99", expiryAt: "2026-05-06T12:30:00" }` | 200 OK, 409 Conflict (Oversell), 400 Invalid SKU |
| `releaseStock` | `release(reservationId)` | `{ reservationId: "RES-99", reason: "USER_CANCEL" }` | `{ success: true, releasedQty: 2, status: "CANCELLED" }` | 200 OK, 404 Not Found, 410 Gone (Already Fulfilled) |
| `updateStock` | `adjustStock(skuId, warehouseId, action, qty)` | `{ skuId: "SKU-A", wh: "WH-01", action: "IN", qty: 500 }` | `{ skuId: "SKU-A", newAvailable: 1502, version: 12 }` | 200 OK, 422 Insufficient Physical |
| `getAvailableInventory` | `checkAvailability(skuId)` | `{ skuId: "SKU-A", region: "US-EAST" }` | `{ warehouses: [{id: "WH-01", qty: 50}, {id: "WH-02", qty: 30}] }` | 200 OK, 404 SKU Not Found |

---

### 8. Database Schema Design

**Tables & Columns**
- `products` (`id` PK VARCHAR, `name` VARCHAR, `category` VARCHAR, `created_at` TIMESTAMP)
- `skus` (`id` PK VARCHAR, `product_id` FK, `variant_attrs` JSONB, `is_active` BOOLEAN)
- `warehouses` (`id` PK VARCHAR, `name` VARCHAR, `region` VARCHAR, `status` ENUM)
- `inventory` (`sku_id` VARCHAR, `warehouse_id` VARCHAR, `physical_qty` INT DEFAULT 0, `reserved_qty` INT DEFAULT 0, `available_qty` GENERATED ALWAYS AS (`physical_qty` - `reserved_qty`), `version` INT DEFAULT 1, `alert_limit` INT DEFAULT 10, PRIMARY KEY(sku_id, warehouse_id))
- `reservations` (`id` PK UUID, `order_id` VARCHAR, `sku_id` VARCHAR, `warehouse_id` VARCHAR, `qty` INT, `status` ENUM (ACTIVE, RELEASED, FULFILLED), `created_at` TIMESTAMP, `expires_at` TIMESTAMP)
- `stock_transactions` (`id` PK UUID, `reference_id` VARCHAR, `sku_id` VARCHAR, `warehouse_id` VARCHAR, `action` ENUM, `qty` INT, `timestamp` TIMESTAMP)

**Indexing Strategy**
- `inventory(sku_id, warehouse_id)` → Clustered index (Primary Key). Fast point lookup.
- `reservations(expires_at)` → Partial index `WHERE status = 'ACTIVE'`. Enables cron sweep for expired reservations.
- `stock_transactions(sku_id, timestamp DESC)` → B-Tree for audit trails and reconciliation.
- `inventory(available_qty, warehouse_id)` → Filtered index `WHERE available_qty <= alert_limit` for rapid alert polling.

---

### 9. Concurrency & Consistency

**Prevent Overselling**
- **Optimistic Locking**: `version` column in `inventory` table. Update query includes `WHERE version = current_version`. If 0 rows affected, retry or fail fast.
- **Pessimistic Locking**: For high-contention SKUs, use `SELECT ... FOR UPDATE` within a short transaction to serialize reservations.
- **Application-Level Guard**: `ReentrantReadWriteLock` per `InventoryItem` key in memory maps. `tryReserve()` is synchronized to prevent race conditions during concurrent API calls.

**Distributed Consistency**
- **Idempotency Keys**: Every reservation request carries `orderId + skuId`. DB `reservations` table has unique constraint to prevent duplicates on network retries.
- **Outbox Pattern**: After DB commit, stock transaction and reservation events are written to an `outbox_events` table in the same transaction. Background relay publishes to Kafka. Guarantees at-least-once delivery without tight coupling.
- **Saga Compensation**: If downstream payment/order service fails, inventory receives a `COMPENSATE_RESERVATION` command. `release()` logic executes. State remains consistent.

---

### 10. Edge Cases

| Scenario | Handling |
|----------|----------|
| **Out of Stock** | `tryReserve()` returns false immediately. Order service falls back to nearest warehouse or prompts backorder. |
| **Partial Availability** | System checks multiple warehouses. If total `qty` >= requested, splits reservation across locations (if supported). Otherwise, rejects. |
| **Duplicate Reservations** | Unique constraint on `(order_id, sku_id)` in DB. Application checks cache first. Idempotency ensures safe retries. |
| **Failed Transactions** | DB transaction rolls back on any step failure. `version` remains unchanged. Client receives `409/500`, prompts retry. |
| **Inventory Mismatch** | Nightly reconciliation job compares `inventory.available` with sum of active reservations + physical logs. Auto-adjusts discrepancies via `ADJUST` transaction. Alerts ops team. |

---

### 11. Sample SQL Queries

**Get available stock for a SKU across active warehouses:**
```sql
SELECT i.warehouse_id, i.available_qty, i.physical_qty, i.reserved_qty
FROM inventory i
JOIN warehouses w ON i.warehouse_id = w.id
WHERE i.sku_id = $1 AND w.status = 'ACTIVE' AND i.available_qty > 0;
```

**Find low-stock items requiring restocking:**
```sql
SELECT sku_id, warehouse_id, available_qty, alert_limit,
       (alert_limit - available_qty) AS deficit
FROM inventory
WHERE available_qty <= alert_limit
ORDER BY available_qty ASC;
```

**Fetch active reservations expiring soon (for auto-release):**
```sql
SELECT id, order_id, sku_id, warehouse_id, qty, expires_at
FROM reservations
WHERE status = 'ACTIVE' AND expires_at < NOW()
ORDER BY expires_at ASC;
```

**Inventory movement summary for a product (Last 24h):**
```sql
SELECT sku_id, SUM(CASE WHEN action = 'IN' THEN qty ELSE 0 END) AS stock_in,
               SUM(CASE WHEN action = 'OUT' THEN qty ELSE 0 END) AS stock_out
FROM stock_transactions
WHERE timestamp >= NOW() - INTERVAL '24 HOURS'
GROUP BY sku_id;
```

---

### 12. Trade-offs & Extensions

**Limitations of Current Design**
- **In-Memory State in LLD**: `ConcurrentHashMap` doesn't survive restarts. Real systems rely on PostgreSQL/MySQL or distributed KV stores.
- **Synchronous Locking**: Pessimistic DB locks can bottleneck flash sales. Retry storms may degrade latency.
- **Monolithic Alert Logic**: Direct observer calls can block inventory transactions if downstream systems are slow.

**Improvements & Extensions**
1. **Distributed Inventory Partitioning**: Shard `inventory` table by `warehouse_id` or `sku_id hash`. Route reads/writes to appropriate partitions. Reduces lock contention.
2. **Redis Cache for Hot SKUs**: Cache `available_qty` in Redis with short TTL. Use Lua scripts for atomic `DECR` on reservation. DB acts as source of truth. Cache invalidated on stock-in/adjustment.
3. **Event-Driven Architecture**: Replace direct `AlertService` calls with Kafka. `InventoryService` publishes `StockLevelChanged` events. Consumers independently handle alerts, analytics, and replenishment triggers.
4. **CQRS Separation**: Split `Command` (Reserve/Update) and `Query` (Check Stock) models. Command side uses strict ACID. Query side uses materialized views/Elasticsearch for fast, scalable lookups.
5. **Predictive Restocking**: Integrate ML model consuming `stock_transactions` and sales velocity. Auto-generate `PurchaseOrder` commands when projected stock drops below safety margin.
6. **Geospatial Routing**: Upgrade `AllocationStrategy` to use customer zip code + warehouse lat/long. Prioritize nearest node with sufficient stock, balancing shipping cost vs fulfillment speed.
