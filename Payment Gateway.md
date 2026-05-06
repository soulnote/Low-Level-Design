# Payment Gateway
### 1. Requirement Clarification

**Functional Requirements**
- Initiate payment with amount, currency, and selected payment method.
- Support multiple methods: Credit/Debit Card, UPI, Net Banking, Digital Wallets.
- Two-step flow: Authorization (hold funds) → Capture (transfer funds).
- Refunds: Full or partial, with status tracking and audit trail.
- Real-time status tracking with webhook/polling fallback.
- Strict idempotency: Duplicate requests return same result without double-charging.

**Non-Functional Requirements**
- **Reliability**: Zero financial loss/duplication. Exactly-once semantics for business logic.
- **Strong Consistency**: Payment state transitions are atomic and auditable.
- **High Availability**: 99.99% uptime, graceful degradation via circuit breakers & fallback processors.
- **Security & Compliance**: PCI-DSS Level 1 compliant, tokenization, AES-256 encryption, HMAC-verified webhooks.

**Assumptions**
- External processors (banks/card networks/UPI switch) handle actual fund movement.
- Single currency per payment (multi-currency handled via FX rate at initiation).
- Transaction limit: $10,000 per request. Daily merchant limit configurable.
- Idempotency window: 24 hours.
- Webhooks delivered at-least-once with exponential backoff.

---

### 2. Core Entities & Responsibilities

| Entity | Responsibility |
|--------|----------------|
| `Payment` | Core domain aggregate. Tracks amount, currency, method, lifecycle state, and idempotency key. |
| `PaymentRequest` | Input DTO carrying merchant ID, amount, method type, currency, and metadata. |
| `Transaction` | Immutable record of each fund movement attempt (AUTH, CAPTURE, REFUND). Links to processor response. |
| `PaymentMethod` | Abstract base for routing & method-specific validation (Card, UPI, Wallet). |
| `PaymentProcessor` | Contract for external bank/gateway integration. Handles API calls & response mapping. |
| `Refund` | Tracks refund requests, validates against captured amount, stores status & reason. |
| `IdempotencyKey` | Stores client-generated keys, request hashes, and resolved payment IDs to prevent duplication. |
| `PaymentGateway` | Singleton orchestrator. Routes requests, manages processors, enforces consistency. |
| `PaymentService`, `RefundService` | Service layer coordinating state transitions, processor calls, and audit logging. |

---

### 3. Class Design (Java Implementation Required)

```java
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

// ================= ENUMS =================
enum PaymentMethodType { CARD, UPI, WALLET }
enum TransactionType { AUTHORIZE, CAPTURE, REFUND }
enum PaymentStateType { INITIATED, AUTHORIZED, CAPTURED, FAILED, PARTIALLY_REFUNDED, FULLY_REFUNDED }

// ================= DOMAIN MODELS =================
abstract class PaymentMethod {
    protected final String id;
    protected final PaymentMethodType type;
    public PaymentMethod(String id, PaymentMethodType type) { this.id = id; this.type = type; }
    public String getId() { return id; }
    public PaymentMethodType getType() { return type; }
    public abstract void validate();
}
class CardPaymentMethod extends PaymentMethod {
    private final String last4;
    public CardPaymentMethod(String id, String last4) { super(id, PaymentMethodType.CARD); this.last4 = last4; }
    public void validate() { if (last4 == null || last4.length() != 4) throw new IllegalArgumentException("Invalid card token"); }
}
class UPIPaymentMethod extends PaymentMethod {
    private final String vpa;
    public UPIPaymentMethod(String id, String vpa) { super(id, PaymentMethodType.UPI); this.vpa = vpa; }
    public void validate() { if (!vpa.contains("@")) throw new IllegalArgumentException("Invalid UPI VPA"); }
}

class Transaction {
    private final String id;
    private final String paymentId;
    private final TransactionType type;
    private final double amount;
    private final String processorTxnId;
    private final String status;
    private final Instant createdAt;
    public Transaction(String paymentId, TransactionType type, double amount, String processorId, String status) {
        this.id = UUID.randomUUID().toString().substring(0, 12);
        this.paymentId = paymentId; this.type = type; this.amount = amount;
        this.processorTxnId = processorId; this.status = status; this.createdAt = Instant.now();
    }
    public double getAmount() { return amount; }
    public String getProcessorTxnId() { return processorTxnId; }
    public TransactionType getType() { return type; }
}

class Refund {
    private final String id;
    private final String paymentId;
    private final double amount;
    private final String reason;
    private String status;
    public Refund(String paymentId, double amount, String reason) {
        this.id = "RF-" + UUID.randomUUID().toString().substring(0, 8);
        this.paymentId = paymentId; this.amount = amount; this.reason = reason; this.status = "PENDING";
    }
    public void markSuccess() { this.status = "SUCCESS"; }
    public double getAmount() { return amount; }
    public String getStatus() { return status; }
}

// ================= STATE PATTERN =================
abstract class PaymentState {
    protected Payment context;
    PaymentState(Payment ctx) { this.context = ctx; }
    abstract void authorize(Transaction t);
    abstract void capture(Transaction t);
    abstract void refund(Transaction t);
    abstract void fail(String reason);
    void transition(PaymentState next) { context.setState(next); }
}
class InitiatedState extends PaymentState {
    InitiatedState(Payment ctx) { super(ctx); }
    public void authorize(Transaction t) { transition(new AuthorizedState(context)); }
    public void capture(Transaction t) { fail("Cannot capture before authorization"); }
    public void refund(Transaction t) { fail("Cannot refund before capture"); }
    public void fail(String r) { transition(new FailedState(context, r)); }
}
class AuthorizedState extends PaymentState {
    AuthorizedState(Payment ctx) { super(ctx); }
    public void authorize(Transaction t) { fail("Already authorized"); }
    public void capture(Transaction t) { transition(new CapturedState(context)); }
    public void refund(Transaction t) { fail("Cannot refund before capture"); }
    public void fail(String r) { transition(new FailedState(context, r)); }
}
class CapturedState extends PaymentState {
    private double totalRefunded = 0.0;
    CapturedState(Payment ctx) { super(ctx); }
    public void authorize(Transaction t) { fail("Already captured"); }
    public void capture(Transaction t) { fail("Already captured"); }
    public void refund(Transaction t) { 
        totalRefunded += t.getAmount(); 
        transition(totalRefunded >= context.getAmount() ? new FullyRefundedState(context) : new PartiallyRefundedState(context)); 
    }
    public void fail(String r) { context.markRefundFailure(); }
}
class PartiallyRefundedState extends CapturedState {
    PartiallyRefundedState(Payment ctx) { super(ctx); } // Inherits refund logic
}
class FullyRefundedState extends PaymentState {
    FullyRefundedState(Payment ctx) { super(ctx); }
    public void authorize(Transaction t) { fail("Payment fully refunded"); }
    public void capture(Transaction t) { fail("Payment fully refunded"); }
    public void refund(Transaction t) { fail("Already fully refunded"); }
    public void fail(String r) { context.markRefundFailure(); }
}
class FailedState extends PaymentState {
    private final String reason;
    FailedState(Payment ctx, String reason) { super(ctx); this.reason = reason; }
    public void authorize(Transaction t) { fail(reason); }
    public void capture(Transaction t) { fail(reason); }
    public void refund(Transaction t) { fail(reason); }
    public void fail(String r) { fail(this.reason); } // Terminal
}

class Payment {
    private final String id;
    private final String merchantId;
    private final double amount;
    private final String currency;
    private final PaymentMethod method;
    private final String idempotencyKey;
    private PaymentStateType stateType;
    private PaymentState currentState;
    private final List<Transaction> transactions = new ArrayList<>();
    private String failureReason;

    public Payment(String id, String merchantId, double amount, String currency, PaymentMethod method, String idempotencyKey) {
        this.id = id; this.merchantId = merchantId; this.amount = amount; this.currency = currency;
        this.method = method; this.idempotencyKey = idempotencyKey;
        this.currentState = new InitiatedState(this);
        this.stateType = PaymentStateType.INITIATED;
    }

    void setState(PaymentState newState) {
        if (newState instanceof FailedState) this.failureReason = ((FailedState) newState).toString().split("@")[1];
        this.stateType = getStateTypeFromInstance(newState);
        this.currentState = newState;
    }
    private PaymentStateType getStateTypeFromInstance(PaymentState s) {
        if (s instanceof InitiatedState) return PaymentStateType.INITIATED;
        if (s instanceof AuthorizedState) return PaymentStateType.AUTHORIZED;
        if (s instanceof CapturedState || s instanceof PartiallyRefundedState) return PaymentStateType.CAPTURED;
        if (s instanceof FullyRefundedState) return PaymentStateType.FULLY_REFUNDED;
        return PaymentStateType.FAILED;
    }

    public void authorize() { currentState.authorize(new Transaction(id, TransactionType.AUTHORIZE, amount, null, "PENDING")); }
    public void capture() { currentState.capture(new Transaction(id, TransactionType.CAPTURE, amount, null, "PENDING")); }
    public void refund(double amount) { currentState.refund(new Transaction(id, TransactionType.REFUND, amount, null, "PENDING")); }
    public void fail(String reason) { currentState.fail(reason); }
    public void markRefundFailure() { this.failureReason = "Refund failed at processor"; }

    public String getId() { return id; }
    public double getAmount() { return amount; }
    public PaymentStateType getStateType() { return stateType; }
    public PaymentMethod getMethod() { return method; }
    public String getIdempotencyKey() { return idempotencyKey; }
    public void addTransaction(Transaction t) { transactions.add(t); }
}

class IdempotencyRecord {
    private final String key;
    private final String paymentId;
    private final Instant expiresAt;
    public IdempotencyRecord(String key, String paymentId, Instant expiresAt) { this.key = key; this.paymentId = paymentId; this.expiresAt = expiresAt; }
    public String getPaymentId() { return paymentId; }
}

// ================= TEMPLATE METHOD & FACTORY =================
interface PaymentProcessor {
    TransactionResult authorize(Payment payment);
    TransactionResult capture(Payment payment, String authCode);
    TransactionResult refund(Payment payment, String captureCode, double amount);
}

abstract class AbstractProcessor implements PaymentProcessor {
    protected final String processorName;
    AbstractProcessor(String name) { this.processorName = name; }
    
    // Template Method: defines skeleton for processing
    public final TransactionResult processAuthorize(Payment p) {
        validateMethod(p.getMethod());
        try {
            return executeAuthorize(p);
        } catch (Exception e) {
            return new TransactionResult("AUTH_FAIL", e.getMessage(), false);
        }
    }
    protected abstract void validateMethod(PaymentMethod m);
    protected abstract TransactionResult executeAuthorize(Payment p);
    public abstract TransactionResult capture(Payment p, String authCode);
    public abstract TransactionResult refund(Payment p, String captureCode, double amount);
}

class CardProcessor extends AbstractProcessor {
    CardProcessor() { super("STRIPE_CARD"); }
    protected void validateMethod(PaymentMethod m) { if (!(m instanceof CardPaymentMethod)) throw new IllegalArgumentException(); }
    protected TransactionResult executeAuthorize(Payment p) { return new TransactionResult("AUTH_OK_" + p.getId(), null, true); }
    public TransactionResult capture(Payment p, String auth) { return new TransactionResult("CAP_OK_" + p.getId(), null, true); }
    public TransactionResult refund(Payment p, String cap, double amt) { return new TransactionResult("REF_OK_" + p.getId(), null, true); }
}
class UPIProcessor extends AbstractProcessor {
    UPIProcessor() { super("RAZORPAY_UPI"); }
    protected void validateMethod(PaymentMethod m) { if (!(m instanceof UPIPaymentMethod)) throw new IllegalArgumentException(); }
    protected TransactionResult executeAuthorize(Payment p) { return new TransactionResult("UPI_AUTH_" + p.getId(), null, true); }
    public TransactionResult capture(Payment p, String auth) { return new TransactionResult("UPI_CAP_" + p.getId(), null, true); }
    public TransactionResult refund(Payment p, String cap, double amt) { return new TransactionResult("UPI_REF_" + p.getId(), null, true); }
}

class ProcessorManager {
    private static volatile ProcessorManager instance;
    private final Map<PaymentMethodType, AbstractProcessor> processors = new EnumMap<>(PaymentMethodType.class);
    private ProcessorManager() {
        processors.put(PaymentMethodType.CARD, new CardProcessor());
        processors.put(PaymentMethodType.UPI, new UPIProcessor());
        // Wallet -> WalletProcessor
    }
    public static ProcessorManager getInstance() {
        if (instance == null) { synchronized(ProcessorManager.class) { if(instance==null) instance=new ProcessorManager(); } }
        return instance;
    }
    public AbstractProcessor get(PaymentMethodType type) {
        if (!processors.containsKey(type)) throw new UnsupportedOperationException("Unsupported method");
        return processors.get(type);
    }
}

class TransactionResult {
    private final String txnId;
    private final String errorMessage;
    private final boolean success;
    public TransactionResult(String txnId, String err, boolean success) { this.txnId = txnId; this.errorMessage = err; this.success = success; }
    public boolean isSuccess() { return success; }
    public String getTxnId() { return txnId; }
    public String getErrorMessage() { return errorMessage; }
}

// ================= SERVICES & MANAGERS =================
class IdempotencyManager {
    private final Map<String, IdempotencyRecord> cache = new ConcurrentHashMap<>();
    public IdempotencyRecord getOrSet(String key, String paymentId) {
        return cache.computeIfAbsent(key, k -> new IdempotencyRecord(k, paymentId, Instant.now().plusSeconds(86400)));
    }
}

class PaymentService {
    private final IdempotencyManager idempotencyMgr;
    private final Map<String, Payment> paymentStore = new ConcurrentHashMap<>();
    private final ProcessorManager procMgr;

    public PaymentService(IdempotencyManager im) { this.idempotencyMgr = im; this.procMgr = ProcessorManager.getInstance(); }

    public Payment initiate(Payment p) {
        // 1. Idempotency Check
        IdempotencyRecord rec = idempotencyMgr.getOrSet(p.getIdempotencyKey(), p.getId());
        if (!rec.getPaymentId().equals(p.getId())) {
            return paymentStore.get(rec.getPaymentId()); // Return existing
        }

        // 2. Validate Method
        p.getMethod().validate();

        // 3. Authorize
        p.authorize();
        AbstractProcessor processor = procMgr.get(p.getMethod().getType());
        TransactionResult authRes = processor.processAuthorize(p);
        p.addTransaction(new Transaction(p.getId(), TransactionType.AUTHORIZE, p.getAmount(), authRes.getTxnId(), authRes.isSuccess() ? "SUCCESS" : "FAILED"));

        if (!authRes.isSuccess()) { p.fail("Authorization failed: " + authRes.getErrorMessage()); }
        else { p.addTransaction(new Transaction(p.getId(), TransactionType.CAPTURE, p.getAmount(), null, "PENDING")); }

        paymentStore.put(p.getId(), p);
        return p;
    }

    public Payment capture(String paymentId) {
        Payment p = paymentStore.get(paymentId);
        if (p == null) throw new IllegalArgumentException("Payment not found");
        p.capture();
        AbstractProcessor proc = procMgr.get(p.getMethod().getType());
        TransactionResult capRes = proc.capture(p, "AUTH_TOKEN"); // Simplified token passing
        Transaction txn = new Transaction(p.getId(), TransactionType.CAPTURE, p.getAmount(), capRes.getTxnId(), capRes.isSuccess() ? "SUCCESS" : "FAILED");
        p.addTransaction(txn);
        if (!capRes.isSuccess()) p.fail("Capture failed: " + capRes.getErrorMessage());
        return p;
    }

    public Payment getPayment(String id) { return paymentStore.get(id); }
}

class RefundService {
    private final Map<String, Refund> refundStore = new ConcurrentHashMap<>();
    private final ProcessorManager procMgr;
    public RefundService() { this.procMgr = ProcessorManager.getInstance(); }

    public Refund process(Payment p, double amount, String reason) {
        if (amount > p.getAmount()) throw new IllegalArgumentException("Refund exceeds payment amount");
        Refund rf = new Refund(p.getId(), amount, reason);
        p.refund(amount);
        TransactionResult refRes = procMgr.get(p.getMethod().getType()).refund(p, "CAPTURE_TOKEN", amount);
        if (refRes.isSuccess()) rf.markSuccess();
        refundStore.put(rf.getId(), rf);
        return rf;
    }
}

// ================= SINGLETON FACADE =================
class PaymentGateway {
    private static volatile PaymentGateway instance;
    private final PaymentService paymentService;
    private final RefundService refundService;

    private PaymentGateway() {
        paymentService = new PaymentService(new IdempotencyManager());
        refundService = new RefundService();
    }
    public static PaymentGateway getInstance() {
        if (instance == null) { synchronized(PaymentGateway.class) { if(instance==null) instance=new PaymentGateway(); } }
        return instance;
    }
    public PaymentService getPaymentService() { return paymentService; }
    public RefundService getRefundService() { return refundService; }
}
```

---

### 4. Relationships & Class Diagram (Textual UML)

```
[PaymentGateway] 1 <1..> 1 [PaymentService]        (Composition)
[PaymentGateway] 1 <1..> 1 [RefundService]         (Composition)

[PaymentService] 1 --> 1 [IdempotencyManager]      (Dependency)
[PaymentService] 1 --> 1 [ProcessorManager]        (Dependency)

[ProcessorManager] 1 <1..> * [AbstractProcessor]   (Aggregation/Factory Map)
[AbstractProcessor] <|-- [CardProcessor]           (Inheritance: Template Method base)
[AbstractProcessor] <|-- [UPIProcessor]

[Payment] 1 *--> * [Transaction]                   (Composition: Transactions bound to Payment lifecycle)
[Payment] 1 o--> * [Refund]                        (Association: Refunds reference original Payment)
[Payment] 1 --> 1 [PaymentState]                   (Composition/Context: State Pattern delegation)
[PaymentState] <|-- [InitiatedState]               (Inheritance: Concrete States)
[PaymentState] <|-- [AuthorizedState]
[PaymentState] <|-- [CapturedState]
[PaymentState] <|-- [FailedState]

[Payment] 1 --> 1 [PaymentMethod]                  (Association: CardMethod, UPIMethod, etc.)
[PaymentMethod] <|-- [CardPaymentMethod]           (Inheritance)
[PaymentMethod] <|-- [UPIPaymentMethod]
```

---

### 5. Design Patterns Used

| Pattern | Implementation in Code |
|---------|------------------------|
| **Singleton** | `ProcessorManager` & `PaymentGateway` use double-checked locking. Ensures single processor registry and unified gateway entry point across threads. |
| **Factory** | `ProcessorManager` acts as a registry/factory. Routes to `CardProcessor` or `UPIProcessor` based on `PaymentMethodType`. Decouples service layer from concrete processor implementations. |
| **Strategy** | `AbstractProcessor` implementations act as interchangeable strategies for payment routing. Allows swapping providers (e.g., Stripe → Adyen) without modifying core payment flow. |
| **State Pattern** | `Payment` delegates behavior to `PaymentState`. Concrete states (`InitiatedState`, `AuthorizedState`, etc.) enforce valid transitions. Prevents illegal operations (e.g., refunding before capture). |
| **Template Method** | `AbstractProcessor.processAuthorize()` defines fixed skeleton: `validateMethod()` → `executeAuthorize()`. Subclasses override only provider-specific steps, ensuring consistent error handling & logging. |

---

### 6. Core Flow (Very Important)

#### Payment Initiation Flow
1. **Request Received**: Merchant calls `initiatePayment()` with amount, method, currency, `idempotency_key`.
2. **Idempotency Check**: `IdempotencyManager` verifies key. If seen, returns cached `Payment` ID. Prevents duplicate charges.
3. **Payment Creation**: `Payment(INITIATED)` instantiated. Method validated (e.g., VPA format, tokenized card check).
4. **Authorization**: State transitions to `AuthorizedState`. Processor called (`processAuthorize()`). Bank places hold.
5. **Record & Return**: `Transaction(AUTH)` recorded. Payment ID & status returned. Awaiting capture.

#### Authorization & Capture Flow
1. **Capture Trigger**: Merchant confirms fulfillment. Calls `capturePayment(paymentId)`.
2. **State Validation**: Checks `PaymentStateType.AUTHORIZED`. Transitions to `CapturedState`.
3. **Fund Transfer**: Processor `capture()` called with auth token. Bank releases hold & transfers funds.
4. **Finalize**: `Transaction(CAPTURE)` recorded with processor reference. State: `CAPTURED`. Ready for refund.

#### Failure & Retry Flow
1. **Failure Detection**: Processor returns timeout/decline. State → `FailedState`. Reason logged.
2. **Retry Policy**: If network timeout (idempotency key preserved), system retries with exponential backoff (30s, 2m, 5m).
3. **Deduplication**: Processor checks auth/capture idempotency headers. Returns same result if already processed.
4. **Reconciliation**: Async job polls pending payments. If processor confirms success despite timeout, state corrected.

#### Refund Flow
1. **Validate**: `RefundService` checks `amount <= captured_amount`. Verifies `CAPTURED` or `REFUNDED` state.
2. **Process**: State transitions to `PartialRefundedState`/`FullyRefundedState`. Processor `refund()` invoked.
3. **Record**: `Refund` entity created. `Transaction(REFUND)` logged.
4. **Completion**: Funds reversed to original payment method. Merchant notified.

#### Status Tracking Flow
1. **Webhook Primary**: Processor sends `payment.status.updated` event. System verifies HMAC signature.
2. **State Sync**: Webhook payload updates `Transaction` status & `PaymentState`. Idempotent handler ignores duplicates.
3. **Polling Fallback**: Scheduled cron queries processor for payments stuck in `PENDING` > 5 mins. Resolves state drift.

---

### 7. API Design (Conceptual Only)

| Operation | Method | Request Payload | Response | Status Codes |
|-----------|--------|-----------------|----------|--------------|
| `initiatePayment` | `POST /payments` | `{ amount: 100.00, currency: "USD", method_type: "CARD", token: "tok_123", idempotency_key: "ik_8a2f" }` | `{ payment_id: "pay_9x3b", status: "INITIATED", next_action: "CAPTURE" }` | 201 Created, 400 Validation, 409 Duplicate |
| `capturePayment` | `POST /payments/{id}/capture` | `{ amount: 100.00 }` | `{ payment_id: "pay_9x3b", status: "CAPTURED", transaction_id: "txn_cap_1" }` | 200 OK, 404 Not Found, 409 Invalid State |
| `refundPayment` | `POST /payments/{id}/refund` | `{ amount: 50.00, reason: "Customer cancellation" }` | `{ refund_id: "ref_k2m9", status: "PROCESSING", amount: 50.00 }` | 201 Created, 400 Exceeds Limit, 422 Not Captured |
| `getPaymentStatus` | `GET /payments/{id}` | N/A | `{ id: "pay_9x3b", status: "CAPTURED", amount: 100.00, transactions: [...] }` | 200 OK, 404 Not Found |

---

### 8. Database Schema Design

**Tables & Columns**
- `payments` (`id` PK VARCHAR, `merchant_id` VARCHAR, `amount` DECIMAL(15,2), `currency` VARCHAR(3), `status` VARCHAR, `method_type` ENUM, `idempotency_key` VARCHAR, `created_at` TIMESTAMP)
- `transactions` (`id` PK VARCHAR, `payment_id` FK, `type` ENUM(AUTH, CAPTURE, REFUND), `amount` DECIMAL(15,2), `processor_txn_id` VARCHAR, `status` VARCHAR, `created_at` TIMESTAMP)
- `refunds` (`id` PK VARCHAR, `payment_id` FK, `amount` DECIMAL(15,2), `status` VARCHAR, `reason` TEXT, `created_at` TIMESTAMP)
- `idempotency_keys` (`key` PK VARCHAR UNIQUE, `payment_id` FK, `request_hash` VARCHAR, `response_payload` JSONB, `expires_at` TIMESTAMP)
- `webhook_logs` (`id` PK BIGINT, `payment_id` FK, `event_type` VARCHAR, `signature` VARCHAR, `processed_at` TIMESTAMP, `status` VARCHAR)

**Indexing Strategy**
- `payments(merchant_id, status, created_at DESC)` → Fast merchant dashboard queries & pagination.
- `transactions(payment_id)` → B-Tree for rapid audit trail fetching.
- `idempotency_keys(key)` → Unique constraint. Powers O(1) deduplication.
- `refunds(payment_id, status)` → Filtered index for refund reconciliation.
- `webhook_logs(payment_id, processed_at)` → Enables idempotent webhook replay tracking.

---

### 9. Concurrency & Consistency

- **Idempotency Handling**: Unique DB constraint on `idempotency_keys.key`. `INSERT ... ON CONFLICT (key) DO UPDATE` guarantees exactly-once insertion. If concurrent requests hit same key, second thread reads cached result.
- **At-Least-Once → Exactly-Once**: Webhooks/events delivered at-least-once. Consumer uses `payment_id + txn_type` as composite unique key. Duplicates silently ignored.
- **Distributed Transactions**: Saga pattern with compensation. Auth succeeds → Capture fails → System auto-queues `Refund(Auth)` to reverse hold. No 2PC across banks.
- **Race Conditions**: `Payment.status` updates use optimistic locking (`version` column). `UPDATE payments SET status=?, version=version+1 WHERE id=? AND version=?`. If 0 rows, client retries.
- **State Drift Resolution**: Async reconciliation job compares gateway DB vs processor ledger. Mismatches trigger state correction or manual ops ticket.

---

### 10. Security Considerations

- **PCI-DSS Compliance**: Raw PAN/CVV never stored. Only network tokens (e.g., Stripe tokens) persisted. AES-256-GCM for sensitive metadata.
- **Tokenization**: Payment methods stored as opaque tokens. Actual card routing handled by certified PCI vault.
- **Secure Communication**: Mutual TLS (mTLS) for processor APIs. TLS 1.3 enforced. All internal microservices use service mesh (e.g., Istio) with mTLS.
- **Webhook Verification**: HMAC-SHA256 signature validated using rotating merchant secrets. Prevents forged status updates.
- **Data Minimization**: Logs redact sensitive fields. PII encrypted at rest. Access controlled via RBAC with audit logging.

---

### 11. Edge Cases

| Scenario | Handling |
|----------|----------|
| **Duplicate Payment Requests** | `idempotency_key` unique constraint. Second request returns cached `payment_id` & status immediately. No processor call. |
| **Partial Failure (Auth OK, Capture Fail)** | Capture fails → refund saga triggered for auth amount. State → `FAILED`. Merchant notified. Funds auto-released. |
| **Refund Exceeds Captured Amount** | `RefundService` validates against `captured_total`. Throws `400 Bad Request`. Prevents negative merchant balance. |
| **Network Timeout During Processor Call** | Retry with exponential backoff + idempotency headers. If unresolved, async poller reconciles with bank ledger. |
| **Inconsistent Gateway vs Bank State** | Scheduled reconciliation job runs hourly. Compares `status` vs processor API. Flags drift. Auto-corrects if idempotent. |

---

### 12. Sample SQL Queries

**Fetch payment status:**
```sql
SELECT id, merchant_id, amount, status, method_type, created_at
FROM payments
WHERE id = $1;
```

**Get transactions for a payment:**
```sql
SELECT id, type, amount, processor_txn_id, status, created_at
FROM transactions
WHERE payment_id = $1
ORDER BY created_at ASC;
```

**Find failed payments (last 24h):**
```sql
SELECT id, merchant_id, amount, status, created_at
FROM payments
WHERE status = 'FAILED' 
  AND created_at >= NOW() - INTERVAL '24 hours'
ORDER BY created_at DESC
LIMIT 100;
```

**Refund history with status breakdown:**
```sql
SELECT p.id AS payment_id, SUM(CASE WHEN r.status = 'SUCCESS' THEN r.amount ELSE 0 END) AS total_refunded,
       COUNT(r.id) AS refund_count
FROM refunds r
JOIN payments p ON r.payment_id = p.id
WHERE p.merchant_id = $1
GROUP BY p.id;
```

---

### 13. Trade-offs & Extensions

**Limitations of Current Design**
- **In-Memory Stores**: `ConcurrentHashMap` for LLD. Production requires PostgreSQL + Redis for idempotency & caching.
- **Synchronous Capture**: Blocks merchant confirmation. High latency if bank is slow.
- **Monolithic Processor Map**: Hardcoded routing. Lacks dynamic failover or cost-based routing.

**Improvements & Extensions**
1. **Multi-Region Active-Active**: Deploy primary/secondary payment clusters per region. Async replication via CDC (Debezium). Conflict resolution via Lamport timestamps & idempotency keys.
2. **Event-Driven Architecture**: Replace direct service calls with Kafka. `PaymentInitiated` → `AuthSaga` → `CaptureSaga` → `Settlement`. Enables async retries, DLQ, and auditability.
3. **Reconciliation Engine**: Nightly batch job ingests processor settlement files (CSV/JSON). Matches with internal ledger. Auto-flags mismatches, triggers alerts.
4. **Fraud Detection Integration**: Inject `RiskScoringStrategy` during `initiate()`. ML model evaluates device fingerprint, velocity, IP reputation. Blocks high-risk payments before auth.
5. **Dynamic Least-Cost Routing**: Implement `ProcessorRouter` strategy. Routes transactions based on success rate, fees, & region. Fallback to secondary processor on degradation.
6. **PCI-Compliant Vault Decoupling**: Move token storage to dedicated HSM-backed service. Payment gateway only handles routing & state. Reduces audit scope & breach impact.
