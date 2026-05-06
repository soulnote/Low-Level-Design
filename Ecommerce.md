# Ecommerce

### 1. Requirement Clarification

**Functional Requirements**
- **Cart CRUD**: Add, remove, update quantity of items.
- **View Cart**: Fetch cart contents, subtotals, discounts, taxes, and grand total.
- **Promotions**: Apply coupons/discounts (percentage, fixed amount, threshold-based).
- **Checkout**: Validate cart, reserve inventory, calculate final pricing, generate order draft, clear cart.
- **Save for Later**: Move items from cart to saved items without deleting.
- **Guest/Logged-in**: Merge guest cart into user cart on authentication.

**Non-Functional Requirements**
- **Low Latency**: Cart read/write < 50ms (p99).
- **High Availability**: 99.99% uptime, degraded read mode during write outages.
- **Consistency**: Strict price/inventory validation at checkout. No overselling.
- **Scalability**: Support millions of concurrent carts, partitioned by user/region.

**Assumptions**
- Single active cart per user (session-based for guests).
- Pricing is dynamic (tax, shipping, promo rules).
- Inventory is checked and reserved synchronously during checkout with a 10-min TTL.
- Guest carts stored in Redis, authenticated carts in DB. Merges on login.
- Max 100 items per cart. Max 1 coupon per cart.

---

### 2. Core Entities & Responsibilities

| Entity | Responsibility |
|--------|----------------|
| `ShoppingCartSystem` | Singleton facade. Routes requests, initializes managers, handles lifecycle. |
| `User` | Buyer identity (guest/authenticated). Links to cart & order history. |
| `Cart` | Container for items. Holds state, version (optimistic lock), total calculation context. |
| `CartItem` | Represents a product in cart. Tracks quantity, unit price, line total. |
| `Product` | Catalog item. SKU, base price, category, vendor ID. |
| `PricingDetail` | Immutable snapshot of cart breakdown: subtotal, discounts, tax, shipping, final total. |
| `Coupon` | Promo rule. Validates applicability, delegates to `DiscountStrategy`. |
| `InventoryService` | External dependency interface for stock checks & reservation. |
| `Order` | Checkout result. Captures cart state, payment intent, delivery details. |

---

### 3. Class Design (Java Implementation)

```java
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

// ================= ENUMS =================
enum DiscountType { PERCENTAGE, FIXED_AMOUNT }
enum OrderStatus { PENDING, CONFIRMED, CANCELLED }
enum CartEventType { PRICE_CHANGED, STOCK_DEPLETED, ITEM_REMOVED }

// ================= DOMAIN MODELS =================
class Product {
    private final String sku;
    private final String name;
    private final double basePrice;
    private final String categoryId;

    public Product(String sku, String name, double basePrice, String categoryId) {
        this.sku = sku; this.name = name; this.basePrice = basePrice; this.categoryId = categoryId;
    }
    public String getSku() { return sku; }
    public double getBasePrice() { return basePrice; }
}

class CartItem {
    private final String itemId;
    private final String productSku;
    private final double unitPrice;
    private int quantity;

    public CartItem(String itemId, String productSku, double unitPrice, int quantity) {
        this.itemId = itemId; this.productSku = productSku; this.unitPrice = unitPrice; this.quantity = quantity;
    }
    public void updateQuantity(int qty) { this.quantity = qty; }
    public double getLineTotal() { return unitPrice * quantity; }
    public String getItemId() { return itemId; }
    public String getProductSku() { return productSku; }
    public int getQuantity() { return quantity; }
    public double getUnitPrice() { return unitPrice; }
}

class PricingDetail {
    private final double subtotal;
    private final double discount;
    private final double tax;
    private final double shipping;
    private final double grandTotal;

    public PricingDetail(double subtotal, double discount, double tax, double shipping, double grandTotal) {
        this.subtotal = subtotal; this.discount = discount; this.tax = tax; this.shipping = shipping; this.grandTotal = grandTotal;
    }
    public double getSubtotal() { return subtotal; }
    public double getGrandTotal() { return grandTotal; }
    public double getDiscount() { return discount; }
}

class Cart {
    private final String cartId;
    private final String userId;
    private final List<CartItem> items;
    private volatile long version; // Optimistic locking
    private PricingDetail pricingDetail;
    private Coupon appliedCoupon;

    public Cart(String cartId, String userId) {
        this.cartId = cartId; this.userId = userId; this.items = new ArrayList<>();
        this.version = 1; this.pricingDetail = new PricingDetail(0,0,0,0,0);
    }

    public synchronized void addItem(CartItem item) {
        items.stream().filter(i -> i.getProductSku().equals(item.getProductSku())).findAny()
             .ifPresentOrElse(i -> i.updateQuantity(i.getQuantity() + item.getQuantity()), () -> items.add(item));
        version++;
    }

    public synchronized void updateItem(String itemId, int newQty) {
        items.stream().filter(i -> i.getItemId().equals(itemId)).findFirst()
             .ifPresent(i -> { i.updateQuantity(newQty); version++; });
    }

    public synchronized void removeItem(String itemId) {
        items.removeIf(i -> i.getItemId().equals(itemId));
        version++;
    }

    public void applyPricing(PricingDetail detail, Coupon coupon) {
        this.pricingDetail = detail;
        this.appliedCoupon = coupon;
    }

    public String getCartId() { return cartId; }
    public String getUserId() { return userId; }
    public List<CartItem> getItems() { return Collections.unmodifiableList(items); }
    public long getVersion() { return version; }
    public PricingDetail getPricingDetail() { return pricingDetail; }
    public Coupon getAppliedCoupon() { return appliedCoupon; }
}

class Coupon {
    private final String code;
    private final DiscountType type;
    private final double value;
    private final double minPurchaseThreshold;
    private final Instant expiry;

    public Coupon(String code, DiscountType type, double value, double minThreshold, Instant expiry) {
        this.code = code; this.type = type; this.value = value; this.minPurchaseThreshold = minThreshold; this.expiry = expiry;
    }
    public boolean isValid(double subtotal, Instant now) {
        return now.isBefore(expiry) && subtotal >= minPurchaseThreshold;
    }
    public String getCode() { return code; }
    public DiscountType getType() { return type; }
    public double getValue() { return value; }
}

class Order {
    private final String orderId;
    private final String userId;
    private final PricingDetail pricing;
    private final List<OrderLineItem> lineItems;
    private final OrderStatus status;

    public Order(String orderId, String userId, PricingDetail pricing, List<OrderLineItem> lineItems, OrderStatus status) {
        this.orderId = orderId; this.userId = userId; this.pricing = pricing; this.lineItems = lineItems; this.status = status;
    }
    public String getOrderId() { return orderId; }
    public String getUserId() { return userId; }
    public OrderStatus getStatus() { return status; }
}

class OrderLineItem {
    private final String productSku;
    private final int quantity;
    private final double pricePaid;
    public OrderLineItem(String sku, int qty, double price) { this.productSku = sku; this.quantity = qty; this.pricePaid = price; }
}

// ================= OBSERVER & STRATEGY =================
interface CartEventListener { void onEvent(Cart cart, CartEventType event); }

interface DiscountStrategy {
    double apply(double subtotal, Coupon coupon);
}
class PercentageDiscount implements DiscountStrategy {
    public double apply(double sub, Coupon c) { return sub * (c.getValue() / 100.0); }
}
class FixedAmountDiscount implements DiscountStrategy {
    public double apply(double sub, Coupon c) { return c.getValue(); }
}

// ================= FACTORY =================
class CouponFactory {
    public static Coupon create(String code, DiscountType type, double value, double threshold, Instant expiry) {
        return new Coupon(code, type, value, threshold, expiry);
    }
    public static DiscountStrategy getStrategy(Coupon c) {
        return c.getType() == DiscountType.PERCENTAGE ? new PercentageDiscount() : new FixedAmountDiscount();
    }
}

// ================= MANAGER / SINGLETON =================
class CartManager {
    private static volatile CartManager instance;
    private final Map<String, Cart> activeCarts = new ConcurrentHashMap<>();
    private final List<CartEventListener> listeners = new CopyOnWriteArrayList<>();

    private CartManager() { seedCarts(); }
    private void seedCarts() { activeCarts.put("user-1", new Cart("c-1", "user-1")); }

    public static CartManager getInstance() {
        if (instance == null) {
            synchronized (CartManager.class) {
                if (instance == null) instance = new CartManager();
            }
        }
        return instance;
    }

    public void registerListener(CartEventListener l) { listeners.add(l); }
    public void notifyListeners(Cart cart, CartEventType evt) {
        for (CartEventListener l : listeners) l.onEvent(cart, evt);
    }

    public Cart getCart(String userId) { return activeCarts.get(userId); }
    public void saveCart(String userId, Cart cart) { activeCarts.put(userId, cart); }
}

// ================= SERVICES =================
interface InventoryService {
    boolean isAvailable(String sku, int qty);
    boolean reserve(String sku, int qty, String orderId, int ttlMinutes);
    void release(String sku, int qty);
}

class PricingService {
    private final Map<String, Double> taxRates = Map.of("default", 0.12, "electronics", 0.08);
    public PricingDetail calculate(Cart cart, Coupon coupon, DiscountStrategy strategy) {
        double subtotal = cart.getItems().stream().mapToDouble(CartItem::getLineTotal).sum();
        double discount = (coupon != null && coupon.isValid(subtotal, Instant.now())) 
                          ? Math.min(strategy.apply(subtotal, coupon), subtotal) : 0.0;
        double taxable = subtotal - discount;
        double tax = taxable * taxRates.getOrDefault("default", 0.12);
        double shipping = subtotal > 100.0 ? 0.0 : 10.0;
        return new PricingDetail(subtotal, discount, tax, shipping, taxable + tax + shipping);
    }
}

class CheckoutService {
    private final PricingService pricingService;
    private final InventoryService inventoryService;

    public CheckoutService(PricingService ps, InventoryService inv) { this.pricingService = ps; this.inventoryService = inv; }

    public Order executeCheckout(String userId, String idempotencyKey) {
        CartManager mgr = CartManager.getInstance();
        Cart cart = mgr.getCart(userId);
        if (cart == null || cart.getItems().isEmpty()) throw new IllegalStateException("Cart empty");

        // 1. Validate & Reserve Inventory
        String orderId = "ORD-" + System.currentTimeMillis() + "-" + new AtomicLong().incrementAndGet();
        for (CartItem item : cart.getItems()) {
            if (!inventoryService.isAvailable(item.getProductSku(), item.getQuantity())) {
                throw new IllegalStateException("Insufficient stock for " + item.getProductSku());
            }
            if (!inventoryService.reserve(item.getProductSku(), item.getQuantity(), orderId, 10)) {
                throw new IllegalStateException("Reservation failed for " + item.getProductSku());
            }
        }

        // 2. Final Pricing
        DiscountStrategy strategy = cart.getAppliedCoupon() != null 
                ? CouponFactory.getStrategy(cart.getAppliedCoupon()) : null;
        PricingDetail finalPrice = pricingService.calculate(cart, cart.getAppliedCoupon(), strategy);

        // 3. Build Order
        Order order = new OrderBuilder(orderId, userId)
                .addLineItems(cart)
                .applyPricing(finalPrice)
                .setStatus(OrderStatus.CONFIRMED)
                .build();

        // 4. Clear Cart
        cart.getItems().clear();
        mgr.saveCart(userId, cart);
        mgr.notifyListeners(cart, CartEventType.ITEM_REMOVED);

        return order;
    }
}

// ================= BUILDER =================
class OrderBuilder {
    private final Order order;
    private final List<OrderLineItem> lines = new ArrayList<>();

    public OrderBuilder(String orderId, String userId) {
        this.order = null; // Placeholder, will build new instance
        this.orderId = orderId; this.userId = userId;
    }
    private String orderId, userId;
    private PricingDetail pricing;
    private OrderStatus status;

    public OrderBuilder addLineItems(Cart cart) {
        for (CartItem i : cart.getItems()) lines.add(new OrderLineItem(i.getProductSku(), i.getQuantity(), i.getUnitPrice()));
        return this;
    }
    public OrderBuilder applyPricing(PricingDetail p) { this.pricing = p; return this; }
    public OrderBuilder setStatus(OrderStatus s) { this.status = s; return this; }
    public Order build() {
        return new Order(orderId, userId, pricing, lines, status);
    }
}
```

---

### 4. Relationships & Class Diagram (Textual UML)

```
[CartManager] 1 <1..> * [Cart]                (Composition: Manages lifecycle of carts)
[CartManager] 1 o--> * [CartEventListener]    (Aggregation: Observer Pattern)

[Cart] 1 *--> * [CartItem]                    (Composition: Items cannot exist outside cart context)
[CartItem] 1 --> 1 [Product]                  (Association: References SKU & price snapshot)
[Cart] 1 --> 1 [PricingDetail]                (Composition: Pricing bound to cart version)
[Cart] 1 o--> 1 [Coupon]                      (Aggregation: Coupon applied to cart, reusable)

[CheckoutService] 1 --> 1 [PricingService]    (Dependency: Delegates pricing logic)
[CheckoutService] 1 --> 1 [InventoryService]  (Dependency: External contract)
[CheckoutService] 1 --> 1 [OrderBuilder]      (Creation/Builder Pattern)

[DiscountStrategy] <|-- [PercentageDiscount]  (Inheritance: Strategy Interface)
[DiscountStrategy] <|-- [FixedAmountDiscount] (Inheritance: Strategy Interface)
[Coupon] 1 --> 1 [DiscountStrategy]           (Dependency: Injected via Factory)

[CouponFactory] ..> [Coupon], [DiscountStrategy] (Factory Method)
```

---

### 5. Design Patterns Used

| Pattern | Implementation in Code |
|---------|------------------------|
| **Singleton** | `CartManager.getInstance()` uses double-checked locking. Guarantees single cart registry across JVM threads. Manages in-memory active carts & event listeners. |
| **Factory** | `CouponFactory.create()` & `CouponFactory.getStrategy()` abstract coupon & discount instantiation. Open-Closed compliant: add new coupon types without touching checkout flow. |
| **Strategy** | `DiscountStrategy` interface with `PercentageDiscount` and `FixedAmountDiscount`. Injected into `PricingService` or used dynamically via factory. Swappable at runtime. |
| **Builder** | `OrderBuilder` constructs `Order` step-by-step: `addLineItems()` → `applyPricing()` → `setStatus()` → `build()`. Handles complex object creation with immutable guarantees. |
| **Observer** | `CartEventListener` interface + `CartManager.listeners` list. `CartManager.notifyListeners()` triggers on cart mutations. Enables async side-effects (analytics, cache invalidation, UI updates) without tight coupling. |

---

### 6. Core Flow (Very Important)

#### Add to Cart Flow
1. **Request**: `addToCart(userId, sku, qty)` received.
2. **Product Fetch**: Validate SKU exists, fetch base price & tax category.
3. **Cart Load**: `CartManager.getCart(userId)` loads existing or creates new.
4. **Update/Insert**: If SKU exists in cart, increment quantity. Else, append new `CartItem` with price snapshot.
5. **Reprice**: `PricingService.calculate()` runs with current items & applied coupon. Updates `Cart.pricingDetail` & increments `version`.
6. **Persist & Notify**: Cart saved to DB/Redis. Observer fires `PRICE_CHANGED`. Response returns updated totals.

#### Update Quantity Flow
1. **Input**: `updateCartItem(itemId, newQty)`.
2. **Validation**: Check `newQty > 0` and `newQty <= max_limit`.
3. **Stock Check**: Optional pre-check against `InventoryService` for high-demand items.
4. **Mutation**: `cart.updateItem()` adjusts qty, bumps `version`.
5. **Reprice**: Recompute totals. If `newQty == 0`, treat as remove.
6. **Return**: Updated cart payload.

#### Remove Item Flow
1. **Locate**: Find `CartItem` by `itemId`.
2. **Delete**: `cart.removeItem()` executes, increments `version`.
3. **Reprice**: Recalculate. If coupon threshold no longer met, auto-remove coupon, repricing with discount=0.
4. **Notify**: Observer fires `ITEM_REMOVED`. Cache invalidated.

#### Apply Coupon Flow
1. **Validate**: `coupon.isValid()` checks expiry, min spend, usage limits.
2. **Check Conflicts**: Ensure no stacking rules violated (e.g., 1 promo per cart).
3. **Apply**: Set `cart.appliedCoupon`, run `PricingService`.
4. **Delta Check**: If new discount < old discount, reject. Else, persist new pricing & version.
5. **Response**: Return applied discount & new grand total.

#### Checkout Flow
1. **Freeze Cart**: Acquire cart lock using `version` (CAS). Snapshot items & pricing.
2. **Inventory Reserve**: Loop items → `InventoryService.reserve(sku, qty, orderId, 10m)`. If any fail → abort, release previous, return error.
3. **Final Pricing**: Recompute with latest tax/shipping rules. Lock price.
4. **Build Order**: `OrderBuilder` creates immutable `Order` with line items & financial breakdown.
5. **Clear Cart**: Remove all items, reset coupon, bump version. Observer notifies.
6. **Async Handoff**: Push order to payment queue. Return `orderId` & status `PENDING`.

---

### 7. API Design (Conceptual Only)

| Operation | Method | Request Payload | Response | Status Codes |
|-----------|--------|-----------------|----------|--------------|
| `addToCart` | `POST /carts/items` | `{ userId, sku, qty: 2, idempotencyKey: "k1" }` | `{ cartId: "c-1", version: 5, items: [...], pricing: {subtotal, discount, total} }` | 200 OK, 404 Product, 409 Cart Locked |
| `updateCartItem` | `PATCH /carts/items/{itemId}` | `{ qty: 4, version: 5 }` | `{ cartId: "c-1", version: 6, pricing: {total: 145.50} }` | 200 OK, 412 Version Mismatch |
| `removeFromCart` | `DELETE /carts/items/{itemId}` | N/A | `{ cartId: "c-1", version: 7, pricing: {total: 89.00} }` | 200 OK, 404 Item |
| `applyCoupon` | `POST /carts/coupons` | `{ userId, code: "SAVE20" }` | `{ success: true, discount: 20.00, newTotal: 69.00 }` | 200 OK, 400 Invalid/Expired |
| `checkout` | `POST /checkout` | `{ userId, shippingAddress, paymentIntentId }` | `{ orderId: "ORD-9A2F", status: "CONFIRMED", estimatedTotal: 81.20 }` | 201 Created, 422 OOS/Price Change, 500 Payment Failed |

---

### 8. Database Schema Design

**Tables & Columns**
- `users` (`id` PK UUID, `email` VARCHAR UNIQUE, `status` ENUM, `created_at` TIMESTAMP)
- `products` (`sku` PK VARCHAR, `name` VARCHAR, `category` VARCHAR, `base_price` DECIMAL, `is_active` BOOLEAN)
- `carts` (`id` PK VARCHAR, `user_id` FK, `version` INT DEFAULT 1, `applied_coupon` VARCHAR NULL, `created_at` TIMESTAMP)
- `cart_items` (`id` PK BIGINT, `cart_id` FK, `product_sku` FK, `quantity` INT, `unit_price_snapshot` DECIMAL)
- `coupons` (`code` PK VARCHAR, `type` ENUM, `value` DECIMAL, `min_spend` DECIMAL, `expires_at` TIMESTAMP, `usage_limit` INT)
- `orders` (`id` PK VARCHAR, `user_id` FK, `cart_snapshot` JSONB, `pricing` JSONB, `status` ENUM, `created_at` TIMESTAMP)

**Indexing Strategy**
- `cart_items(cart_id, product_sku)` → Composite index for fast cart hydration & duplicate checks.
- `carts(user_id)` → B-Tree for instant lookup on auth/guest-merge.
- `orders(user_id, created_at DESC)` → Paginated order history without sorting overhead.
- `coupons(code)` → Unique index for fast validation.
- `cart_items(cart_id, version)` → Optimistic lock verification index.

---

### 9. Concurrency & Consistency

- **Oversell Prevention**: `InventoryService.reserve()` uses `SELECT ... FOR UPDATE SKIP LOCKED` on stock rows. If reserved, decrement available. TTL ensures stale reservations auto-expire.
- **Concurrent Cart Updates**: `Cart.version` enables Optimistic Concurrency Control. `UPDATE carts SET version = v+1 WHERE id = ? AND version = v`. If 0 rows affected, client retries with fresh payload.
- **Idempotency**: Checkout requires `idempotency_key` header. DB stores `checkout_attempts(key, order_id, status)`. Replays return existing order if key seen.
- **Price Consistency**: `CartItem` stores `unit_price_snapshot` at add/update time. Checkout recalculates but flags delta if base price changed > threshold. User prompted to confirm before payment capture.
- **Distributed Locking**: Redis `SETNX cart:{userId}:lock` with 5s TTL during checkout. Prevents parallel checkout requests from double-reserving inventory.

---

### 10. Edge Cases

| Scenario | Handling |
|----------|----------|
| **Out-of-Stock During Checkout** | `reserve()` fails. Rollback previous reservations via compensation. Return `422 Insufficient Stock`. Suggest alternatives. |
| **Coupon Invalid/Expired** | Validation rejects at apply time. Auto-clears `appliedCoupon` at checkout if conditions no longer met. User sees updated total. |
| **Price Change After Add** | Checkout compares `base_price` in DB vs `unit_price_snapshot`. If delta > 5%, pause checkout, require user reconfirmation. Prevents silent overcharge. |
| **Concurrent Cart Updates** | Version mismatch triggers `412 Precondition Failed`. Frontend retries GET cart, merges UI changes, resubmits. |
| **Partial Checkout Failure** | Payment gateway timeout triggers async retry queue. If max retries exceeded, inventory auto-released, order marked `CANCELLED`. Cart restored. |

---

### 11. Sample SQL Queries

**Fetch cart items for a user:**
```sql
SELECT ci.product_sku, ci.quantity, ci.unit_price_snapshot, 
       ci.quantity * ci.unit_price_snapshot AS line_total
FROM cart_items ci
JOIN carts c ON ci.cart_id = c.id
WHERE c.user_id = $1
ORDER BY ci.id;
```

**Calculate total cart value (server-side verification):**
```sql
SELECT COALESCE(SUM(ci.quantity * ci.unit_price_snapshot), 0) AS subtotal,
       COUNT(ci.id) AS item_count
FROM cart_items ci
WHERE ci.cart_id = (SELECT id FROM carts WHERE user_id = $1);
```

**Get applicable coupons for current cart total:**
```sql
SELECT code, type, value, expires_at
FROM coupons
WHERE value > 0 
  AND expires_at > NOW() 
  AND usage_limit > (SELECT COUNT(*) FROM coupon_redemptions WHERE code = coupons.code)
  AND min_spend <= $1 -- $1 = cart subtotal
ORDER BY value DESC LIMIT 1;
```

**Validate product availability:**
```sql
SELECT i.sku, i.available_qty - COALESCE(SUM(r.qty), 0) AS effective_available
FROM inventory i
LEFT JOIN reservations r ON i.sku = r.sku AND r.expires_at > NOW()
WHERE i.sku = $1
GROUP BY i.sku, i.available_qty;
```

---

### 12. Trade-offs & Extensions

**Limitations of Current Design**
- **In-Memory Registry**: `CartManager` stores carts in `ConcurrentHashMap`. Production requires Redis/DB backing. Memory-bound under traffic spikes.
- **Synchronous Checkout**: Inventory reservation & pricing run in single thread. High latency if external pricing engine is slow.
- **Price Snapshot Staleness**: `unit_price_snapshot` can diverge from live catalog. Requires explicit sync or re-validation.

**Improvements & Extensions**
1. **Distributed Cart Storage (Redis)**: Use Redis Hashes `cart:{userId}` with TTL for guest carts. Pipeline writes. Lua scripts for atomic `INCR qty` & version bump.
2. **Event-Driven Architecture**: Publish `CartUpdated`, `InventoryReserved`, `CheckoutFailed` to Kafka. Async services handle notifications, analytics, & compensation. Decouples checkout from downstream side-effects.
3. **Multi-Device Sync**: WebSocket/Push-based invalidation. When cart updates on mobile, web client receives `CartVersionMismatch` event, prompts merge or overwrite.
4. **Real-Time Pricing Updates**: Replace synchronous tax/shipping calls with cached rules engine. Fallback to async recalculation with `PENDING` status. Update user via UI toast.
5. **CQRS Separation**: Command model (add/update) uses strict ACID. Query model (view cart) reads from materialized view/Redis. Scales read throughput 100x for browse-heavy traffic.
6. **Dynamic Threshold Coupons**: Introduce `SmartCouponManager` with ML-driven personalization. Apply targeted discounts based on cart abandonment probability & LTV.
