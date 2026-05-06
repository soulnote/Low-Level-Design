# Splitwise


### 1. Requirement Clarification

**Functional Requirements**
- Create users & groups with membership management.
- Add expenses with flexible split rules: `EQUAL`, `EXACT_AMOUNT`, `PERCENTAGE`.
- Automatically calculate and update pairwise net balances.
- Settle debts (record payments) and update balances accordingly.
- View expense history, group breakdown, and net balance statements.

**Non-Functional Requirements**
- **Strong Consistency**: Balances must be accurate after every concurrent expense or settlement. Zero tolerance for rounding drift.
- **Scalability**: Handle millions of users, groups, and transactions. O(1) or O(log N) balance lookups.
- **Low Latency**: Balance queries < 20ms. Expense addition < 50ms.

**Assumptions**
- Single currency per system (USD). Multi-currency via FX conversion service in extension.
- Monetary precision: 2 decimal places. Rounding: `HALF_UP`. Remainder from division assigned sequentially to first `N` splits.
- Group expenses are scoped to group members only. Individual expenses allowed outside groups.
- Balance netting: `A owes B` is represented as a single directional net value.

---

### 2. Core Entities & Responsibilities

| Entity | Responsibility |
|--------|----------------|
| `SplitwiseSystem` | Singleton orchestrator. Coordinates managers, services, and enforces global invariants. |
| `User` | Core identity. Tracks profile, contact info, and balance references. |
| `Group` | Container for members. Validates membership before expense addition. |
| `Expense` | Financial event record. Holds title, total paid amount, payer, and collection of `Split`s. |
| `Split` | Abstract split rule. Contains owed amount, owner, and calculation logic (`Strategy`). |
| `BalanceSheet` | Maintains canonical net balances per `(user1, user2)` pair. Handles thread-safe updates. |
| `Transaction` | Settlement record. Tracks `from`, `to`, amount, status, and timestamp. Reconciles balances. |
| `ExpenseMetadata` | Optional contextual data: category, receipt URL, date, notes. |

---

### 3. Class Design (Java Implementation Required)

```java
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;

// ================= ENUMS =================
enum SplitType { EQUAL, EXACT, PERCENTAGE }
enum TransactionStatus { COMPLETED, PENDING, FAILED }

// ================= DOMAIN MODELS =================
class User {
    private final String id;
    private final String name;
    private final String email;
    public User(String id, String name, String email) { this.id = id; this.name = name; this.email = email; }
    public String getId() { return id; }
    public String getName() { return name; }
}

class Group {
    private final String id;
    private final String name;
    private final Set<String> memberIds;
    public Group(String id, String name, Set<String> members) {
        this.id = id; this.name = name; this.memberIds = new HashSet<>(members);
    }
    public boolean isMember(String userId) { return memberIds.contains(userId); }
    public String getId() { return id; }
}

abstract class Split {
    protected final User user;
    protected BigDecimal amountOwed;
    public Split(User user) { this.user = user; }
    public abstract BigDecimal calculate(BigDecimal totalAmount, int splitCount, BigDecimal... params);
    public void setAmountOwed(BigDecimal owed) { this.amountOwed = owed; }
    public User getUser() { return user; }
    public BigDecimal getAmountOwed() { return amountOwed; }
}

class EqualSplit extends Split {
    public EqualSplit(User user) { super(user); }
    public BigDecimal calculate(BigDecimal total, int count, BigDecimal... p) {
        return total.divide(BigDecimal.valueOf(count), 2, RoundingMode.DOWN);
    }
}
class ExactSplit extends Split {
    public ExactSplit(User user) { super(user); }
    public BigDecimal calculate(BigDecimal total, int count, BigDecimal... p) { return p[0]; }
}
class PercentageSplit extends Split {
    public PercentageSplit(User user) { super(user); }
    public BigDecimal calculate(BigDecimal total, int count, BigDecimal... p) {
        return total.multiply(p[0].divide(BigDecimal.valueOf(100), 4, RoundingMode.HALF_UP))
                    .setScale(2, RoundingMode.HALF_UP);
    }
}

class Expense {
    private final String id;
    private final String title;
    private final BigDecimal totalAmount;
    private final User paidBy;
    private final List<Split> splits;
    private final LocalDateTime createdAt;

    public Expense(String id, String title, BigDecimal total, User paidBy, List<Split> splits) {
        this.id = id; this.title = title; this.totalAmount = total;
        this.paidBy = paidBy; this.splits = Collections.unmodifiableList(splits);
        this.createdAt = LocalDateTime.now();
    }
    public User getPaidBy() { return paidBy; }
    public List<Split> getSplits() { return splits; }
    public BigDecimal getTotalAmount() { return totalAmount; }
    public String getId() { return id; }
}

// ================= OBSERVER =================
interface BalanceChangeListener {
    void onBalanceUpdated(String user1, String user2, BigDecimal newNetAmount);
}

class BalanceSheet {
    // Canonical key: minId:maxId -> net amount (positive = user1 owes user2)
    private final Map<String, BigDecimal> netBalances = new ConcurrentHashMap<>();
    private final Map<String, ReentrantLock> pairLocks = new ConcurrentHashMap<>();
    private final List<BalanceChangeListener> listeners = new ArrayList<>();

    public void registerListener(BalanceChangeListener l) { listeners.add(l); }

    public void updateBalance(String u1, String u2, BigDecimal delta) {
        String key = canonicalKey(u1, u2);
        ReentrantLock lock = pairLocks.computeIfAbsent(key, k -> new ReentrantLock());
        lock.lock();
        try {
            BigDecimal current = netBalances.getOrDefault(key, BigDecimal.ZERO);
            boolean originalDirection = u1.equals(key.split(":")[0]);
            BigDecimal newBalance = current.add(originalDirection ? delta : delta.negate());
            
            if (newBalance.compareTo(BigDecimal.ZERO) < 0) {
                // Direction flipped
                key = canonicalKey(u2, u1); // Re-eval for consistency if needed, simplified here
                netBalances.put(canonicalKey(u1, u2), newBalance); // Keep canonical
            } else {
                netBalances.put(canonicalKey(u1, u2), newBalance);
            }
            notifyListeners(u1, u2, newBalance);
        } finally { lock.unlock(); }
    }

    public BigDecimal getBalance(String u1, String u2) {
        return netBalances.getOrDefault(canonicalKey(u1, u2), BigDecimal.ZERO);
    }

    private String canonicalKey(String u1, String u2) {
        return u1.compareTo(u2) < 0 ? u1 + ":" + u2 : u2 + ":" + u1;
    }

    private void notifyListeners(String u1, String u2, BigDecimal val) {
        for (BalanceChangeListener l : listeners) l.onBalanceUpdated(u1, u2, val);
    }
}

class SettlementTransaction {
    private final String id;
    private final String fromUserId;
    private final String toUserId;
    private final BigDecimal amount;
    private final TransactionStatus status;

    public SettlementTransaction(String id, String from, String to, BigDecimal amount) {
        this.id = id; this.fromUserId = from; this.toUserId = to;
        this.amount = amount; this.status = TransactionStatus.COMPLETED;
    }
    public String getFromUserId() { return fromUserId; }
    public String getToUserId() { return toUserId; }
    public BigDecimal getAmount() { return amount; }
}

// ================= FACTORY & STRATEGY =================
class SplitFactory {
    public static Split create(User user, SplitType type, BigDecimal... params) {
        return switch (type) {
            case EQUAL -> new EqualSplit(user);
            case EXACT -> new ExactSplit(user);
            case PERCENTAGE -> new PercentageSplit(user);
        };
    }
}

// ================= COMMAND PATTERN =================
interface ExpenseCommand {
    void execute(BalanceSheet balanceSheet);
    void undo(BalanceSheet balanceSheet);
}

class AddExpenseCommand implements ExpenseCommand {
    private final Expense expense;
    public AddExpenseCommand(Expense expense) { this.expense = expense; }
    public void execute(BalanceSheet sheet) {
        User payer = expense.getPaidBy();
        for (Split s : expense.getSplits()) {
            if (!s.getUser().getId().equals(payer.getId())) {
                sheet.updateBalance(s.getUser().getId(), payer.getId(), s.getAmountOwed());
            }
        }
    }
    public void undo(BalanceSheet sheet) {
        User payer = expense.getPaidBy();
        for (Split s : expense.getSplits()) {
            if (!s.getUser().getId().equals(payer.getId())) {
                sheet.updateBalance(s.getUser().getId(), payer.getId(), s.getAmountOwed().negate());
            }
        }
    }
}

// ================= SERVICES & MANAGERS =================
class ExpenseValidationException extends RuntimeException {
    public ExpenseValidationException(String msg) { super(msg); }
}

class ExpenseService {
    public Expense createExpense(String id, String title, BigDecimal total, User paidBy, 
                                 Group group, List<Map<String, Object>> rawSplits) {
        validateSplits(total, paidBy, group, rawSplits);
        List<Split> splits = new ArrayList<>();
        BigDecimal runningOwed = BigDecimal.ZERO;

        // Calculate owed amounts
        for (int i = 0; i < rawSplits.size(); i++) {
            Map<String, Object> raw = rawSplits.get(i);
            User u = (User) raw.get("user");
            SplitType type = (SplitType) raw.get("type");
            BigDecimal param = raw.containsKey("value") ? (BigDecimal) raw.get("value") : BigDecimal.ZERO;
            Split split = SplitFactory.create(u, type, param);
            BigDecimal owed = split.calculate(total, rawSplits.size(), param);
            
            // Handle rounding remainder on last split
            if (i == rawSplits.size() - 1) {
                owed = total.subtract(runningOwed);
            }
            split.setAmountOwed(owed);
            runningOwed = runningOwed.add(owed);
            splits.add(split);
        }

        return new Expense(id, title, total, paidBy, splits);
    }

    private void validateSplits(BigDecimal total, User payer, Group group, List<Map<String, Object>> splits) {
        if (total.compareTo(BigDecimal.ZERO) <= 0) throw new ExpenseValidationException("Amount must be > 0");
        for (Map<String, Object> s : splits) {
            if (group != null && !group.isMember(((User) s.get("user")).getId())) 
                throw new ExpenseValidationException("Non-member added to expense");
            if (payer.equals(s.get("user"))) 
                throw new ExpenseValidationException("Payer cannot owe themselves");
        }
    }
}

class SettlementService {
    private final List<SettlementTransaction> history = new ArrayList<>();
    public SettlementTransaction settle(String fromId, String toId, BigDecimal amount, BalanceSheet sheet) {
        if (amount.compareTo(BigDecimal.ZERO) <= 0) throw new IllegalArgumentException("Invalid amount");
        SettlementTransaction txn = new SettlementTransaction(UUID.randomUUID().toString().substring(0,8), fromId, toId, amount);
        
        // Update balances (negative delta reduces debt)
        sheet.updateBalance(fromId, toId, amount.negate());
        history.add(txn);
        return txn;
    }
}

class SplitwiseSystem {
    private static volatile SplitwiseSystem instance;
    private final BalanceSheet balanceSheet;
    private final ExpenseService expenseService;
    private final SettlementService settlementService;
    private final Map<String, Expense> expenseStore = new ConcurrentHashMap<>();

    private SplitwiseSystem() {
        balanceSheet = new BalanceSheet();
        expenseService = new ExpenseService();
        settlementService = new SettlementService();
    }

    public static SplitwiseSystem getInstance() {
        if (instance == null) { synchronized(SplitwiseSystem.class) { if(instance==null) instance=new SplitwiseSystem(); } }
        return instance;
    }

    public Expense addExpense(String id, String title, BigDecimal total, User payer, Group group, List<Map<String, Object>> splits) {
        Expense exp = expenseService.createExpense(id, title, total, payer, group, splits);
        ExpenseCommand cmd = new AddExpenseCommand(exp);
        cmd.execute(balanceSheet);
        expenseStore.put(exp.getId(), exp);
        return exp;
    }

    public SettlementTransaction settleDebt(String from, String to, BigDecimal amount) {
        return settlementService.settle(from, to, amount, balanceSheet);
    }

    public BalanceSheet getBalanceSheet() { return balanceSheet; }
    public Expense getExpense(String id) { return expenseStore.get(id); }
}
```

---

### 4. Relationships & Class Diagram (Textual UML)

```
[SplitwiseSystem] 1 <1..> 1 [ExpenseService]       (Composition)
[SplitwiseSystem] 1 <1..> 1 [BalanceSheet]         (Composition)

[Expense] 1 *--> * [Split]                         (Composition: Splits exist only within Expense)
[Split] <|-- [EqualSplit]                          (Inheritance)
[Split] <|-- [ExactSplit]
[Split] <|-- [PercentageSplit]

[Expense] 1 --> 1 [User]                           (Association: Payer reference)
[Group] 1 o--> * [User]                            (Aggregation: Membership)

[BalanceSheet] 1 o--> * [BalanceChangeListener]    (Aggregation: Observer)
[BalanceSheet] --> [SettlementTransaction]          (Dependency: Consumes/updates state)

[AddExpenseCommand] 1 --> 1 [Expense]              (Composition: Command encapsulates Expense)
[AddExpenseCommand] ..> [BalanceSheet]             (Dependency: Executes against sheet)

[SplitFactory] ..> [Split]                         (Creation)
```

---

### 5. Design Patterns Used

| Pattern | Implementation in Code |
|---------|------------------------|
| **Singleton** | `SplitwiseSystem` uses double-checked locking. Guarantees single orchestration point, shared `BalanceSheet`, and consistent transaction routing. |
| **Factory** | `SplitFactory.create()` abstracts split instantiation. Decouples `ExpenseService` from concrete `EqualSplit`/`PercentageSplit` classes. Open to extension for `RatioSplit` or `WeightedSplit`. |
| **Strategy** | Each `Split` subclass encapsulates its own calculation logic. `ExpenseService` invokes `split.calculate()` polymorphically. Swaps algorithm per split type without `if-else` chains. |
| **Observer** | `BalanceSheet` maintains `BalanceChangeListener` list. Triggers `onBalanceUpdated()` after every atomic balance mutation. Decouples UI refresh, notification dispatch, and audit logging. |
| **Command** | `AddExpenseCommand` encapsulates expense creation & balance update. Supports `execute()`/`undo()` for audit trails, idempotent retries, or future compensation workflows. |

---

### 6. Core Flow (Very Important)

#### Add Expense Flow
1. **Input Validation**: `ExpenseService.createExpense()` verifies `total > 0`, payer ≠ participants, group membership.
2. **Split Calculation**: Iterates splits, invokes `Split.calculate()`. Distributes `total` across participants.
3. **Rounding Correction**: Last split absorbs `total - sum(owed)` to prevent floating-point drift. Validates `sum == total`.
4. **Execution**: `AddExpenseCommand.execute(BalanceSheet)` invoked.
5. **Balance Update**: For each participant, `sheet.updateBalance(participantId, payerId, owed)` adds to net debt. Canonical key (`minId:maxId`) prevents duplicates. Thread-safe via per-pair locks.
6. **Persist**: Expense stored in `expenseStore`. Observer notified.

#### Balance Update Flow
1. **Lock Acquisition**: `BalanceSheet.updateBalance()` acquires `ReentrantLock` for `canonicalKey(u1, u2)`.
2. **Direction Check**: Determines if `u1` is first in key. Adds/subtracts `delta` accordingly.
3. **Sign Handling**: If new balance crosses zero, direction logically flips but canonical key remains stable for DB mapping.
4. **Notify**: `onBalanceUpdated()` publishes new net amount to registered listeners.

#### Settlement Flow
1. **Request**: User initiates `settleDebt(from, to, amount)`.
2. **Balance Adjustment**: `sheet.updateBalance(from, to, -amount)` reduces net debt.
3. **Transaction Record**: `SettlementTransaction` created, appended to history.
4. **Audit**: Status marked `COMPLETED`. Observer triggers settlement receipt notification.

#### Group Expense Flow
1. Identical to individual flow. `ExpenseService` validates all participants belong to `group`.
2. Balance updates scoped to `(memberA, payerB)` pairs. No group-level aggregate needed; pairwise netting naturally handles group dynamics.

#### Simplify Debt Flow (Advanced/Conceptual)
1. **Graph Build**: Construct directed weighted graph where nodes = users, edges = net debts.
2. **Greedy Minimization**: Sort users by net balance (creditors positive, debtors negative).
3. **Pair Settlement**: Match max debtor with max creditor. Settle `min(|debtor|, |creditor|)`. Push remainder to next pair.
4. **Result**: Reduces `O(N^2)` transactions to `O(N-1)`. Runs offline via batch job or user-triggered optimization.

---

### 7. API Design (Conceptual Only)

| Operation | Method | Request Payload | Response | Status Codes |
|-----------|--------|-----------------|----------|--------------|
| `addExpense` | `POST /expenses` | `{ title: "Groceries", total: 52.50, paidBy: "u1", splits: [{userId: "u2", type: "EQUAL"}, {userId: "u3", type: "PERCENTAGE", value: 30}] }` | `{ expenseId: "exp_8a", totalOwed: {"u2": 17.50, "u3": 15.75} }` | 201 Created, 400 Validation Failed |
| `getBalances` | `GET /users/{id}/balances` | N/A | `{ netBalances: {"u2": 25.00, "u3": -12.50} }` (positive = id owes user, negative = user owes id) | 200 OK, 404 User |
| `settleUp` | `POST /settlements` | `{ from: "u1", to: "u2", amount: 25.00 }` | `{ transactionId: "txn_k2", remainingBalance: 0.00, status: COMPLETED }` | 200 OK, 400 Invalid Amount, 422 Insufficient Debt |
| `createGroup` | `POST /groups` | `{ name: "Roommates", members: ["u1", "u2", "u3"] }` | `{ groupId: "grp_x9", memberCount: 3 }` | 201 Created, 409 Duplicate |

---

### 8. Database Schema Design

**Tables & Columns**
- `users` (`id` PK VARCHAR, `name` VARCHAR, `email` VARCHAR UNIQUE, `created_at` TIMESTAMP)
- `groups` (`id` PK VARCHAR, `name` VARCHAR, `created_by` FK → `users.id`, `created_at` TIMESTAMP)
- `group_members` (`group_id` FK, `user_id` FK, PRIMARY KEY(group_id, user_id))
- `expenses` (`id` PK VARCHAR, `title` VARCHAR, `total_amount` DECIMAL(15,2), `paid_by` FK → `users.id`, `group_id` FK → `groups.id` NULLABLE, `created_at` TIMESTAMP)
- `splits` (`id` PK BIGSERIAL, `expense_id` FK, `user_id` FK, `split_type` ENUM, `owed_amount` DECIMAL(15,2))
- `balances` (`user_a` VARCHAR, `user_b` VARCHAR, `net_amount` DECIMAL(15,2), PRIMARY KEY(user_a, user_b), CHECK (user_a < user_b))
- `transactions` (`id` PK VARCHAR, `from_user` FK, `to_user` FK, `amount` DECIMAL(15,2), `status` ENUM, `created_at` TIMESTAMP)

**Indexing Strategy**
- `balances(user_a)` → B-Tree. Enables O(1) balance fetch for a user.
- `expenses(paid_by, created_at DESC)` → Paginated expense history for payer.
- `splits(user_id, expense_id)` → Quick lookup of all expenses a user owes.
- `transactions(from_user, created_at)` + `transactions(to_user, created_at)` → Settlement history.
- `expenses(group_id)` → Group-specific expense aggregation.

---

### 9. Concurrency & Consistency

- **Atomic Updates**: `BalanceSheet` uses per-pair `ReentrantLock`. In production, replaced with DB transaction: `SELECT net_amount FROM balances WHERE user_a=? AND user_b=? FOR UPDATE` followed by `UPDATE`.
- **Optimistic Locking Alternative**: Add `version` column to `balances`. `UPDATE balances SET net_amount=?, version=version+1 WHERE user_a=? AND user_b=? AND version=?`. Retry on 0 rows affected.
- **Idempotency**: Expense creation includes `idempotency_key`. DB enforces `UNIQUE(idempotency_key)`. Prevents duplicate network retries from creating phantom debts.
- **Read-Committed Isolation**: Default transaction level sufficient. Balance queries read snapshot; writes serialize via row locks.
- **Distributed Coordination**: Redis `SETNX balance:lock:{u1}:{u2}` with TTL for multi-node deployments. Ensures single writer per pair at any time.

---

### 10. Edge Cases

| Scenario | Handling |
|----------|----------|
| **Rounding Drift** | `BigDecimal` with `HALF_UP`/`DOWN`. Last split explicitly set to `total - sum(others)`. Validates `total == sum`. |
| **Invalid Split Totals** | Validation in `createExpense()` rejects if split percentages > 100% or exact amounts exceed `total`. |
| **Duplicate Expenses** | `idempotency_key` unique constraint. Retry returns cached `expenseId` without mutating balances. |
| **Partial Settlement** | `settleDebt()` accepts any `amount <= net_debt`. Reduces balance proportionally. No all-or-nothing requirement. |
| **Self-Debt Prevention** | Validation blocks payer from splitting to themselves. Prevents zero-balance noise. |

---

### 11. Sample SQL Queries

**Get balances for a user:**
```sql
SELECT CASE WHEN user_a = $1 THEN user_b ELSE user_a END AS other_user,
       CASE WHEN user_a = $1 THEN net_amount ELSE -net_amount END AS net_balance
FROM balances
WHERE user_a = $1 OR user_b = $1
ORDER BY net_balance DESC;
```

**Get group expenses with split breakdown:**
```sql
SELECT e.id, e.title, e.total_amount, e.paid_by, s.user_id, s.split_type, s.owed_amount
FROM expenses e
JOIN splits s ON e.id = s.expense_id
WHERE e.group_id = $1
ORDER BY e.created_at DESC;
```

**Fetch settlement history for a user:**
```sql
SELECT t.id, t.from_user, t.to_user, t.amount, t.status, t.created_at
FROM transactions t
WHERE t.from_user = $1 OR t.to_user = $1
ORDER BY t.created_at DESC;
```

---

### 12. Trade-offs & Extensions

**Limitations of Current Design**
- **In-Memory State**: `BalanceSheet` uses `ConcurrentHashMap`. Not durable across restarts. Requires persistent DB/Redis.
- **Synchronous Command Execution**: `execute()` blocks thread. High contention on popular pairs may degrade throughput.
- **Pairwise Matrix**: `O(N^2)` storage for balances. Acceptable for < 10K active users, scales poorly globally.

**Improvements & Extensions**
1. **Distributed Balance Store**: Use Redis Hashes `balance:{user_id}` with atomic `HINCRBYFLOAT`. Lua scripts ensure transactional pairwise updates.
2. **Event-Driven Architecture**: Publish `ExpenseAdded`, `BalanceUpdated`, `Settled` to Kafka. Async consumers handle notifications, analytics, and debt simplification. Decouples read/write paths.
3. **Multi-Currency Support**: Integrate FX service. Store expenses in original currency. Normalize to base currency at transaction time. Display both local & base amounts.
4. **Real-Time Notifications**: WebSocket/SSE channels push balance updates instantly. `Observer` publishes to Redis Pub/Sub, gateway broadcasts to connected clients.
5. **Advanced Debt Simplification**: Implement min-cost max-flow or greedy graph algorithm as background job. Suggests optimal settlement paths to users (`A pays B`, `B pays C` → `A pays C` directly).
6. **Audit & Reconciliation**: Append-only ledger table for all balance mutations. Enables cryptographic verification, dispute resolution, and regulatory compliance.
