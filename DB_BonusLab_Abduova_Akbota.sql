CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE customers (
  customer_id BIGSERIAL PRIMARY KEY,
  iin CHAR(12) UNIQUE NOT NULL,
  full_name TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('active','blocked','frozen')),
  daily_limit_kzt NUMERIC(18,2) DEFAULT 5000000,
  created_at TIMESTAMP DEFAULT now()
);

CREATE TABLE accounts (
  account_id BIGSERIAL PRIMARY KEY,
  customer_id BIGINT REFERENCES customers(customer_id),
  account_number TEXT UNIQUE NOT NULL,
  currency TEXT NOT NULL CHECK (currency IN ('KZT','USD','EUR','RUB')),
  balance NUMERIC(20,2) DEFAULT 0,
  is_active BOOLEAN DEFAULT true,
  opened_at TIMESTAMP DEFAULT now()
);

CREATE TABLE exchange_rates (
  rate_id BIGSERIAL PRIMARY KEY,
  from_currency TEXT,
  to_currency TEXT,
  rate NUMERIC(20,10),
  valid_from TIMESTAMP DEFAULT now()
);

CREATE TABLE transactions (
  transaction_id BIGSERIAL PRIMARY KEY,
  from_account_id BIGINT,
  to_account_id BIGINT,
  amount NUMERIC(20,2),
  currency TEXT,
  amount_kzt NUMERIC(20,2),
  type TEXT CHECK (type IN ('transfer','deposit','withdrawal','salary')),
  status TEXT CHECK (status IN ('pending','completed','failed')),
  created_at TIMESTAMP DEFAULT now(),
  description TEXT
);

CREATE TABLE audit_log (
  log_id BIGSERIAL PRIMARY KEY,
  info JSONB,
  created_at TIMESTAMP DEFAULT now()
);


INSERT INTO customers (iin, full_name, status, daily_limit_kzt) VALUES
('880101000001','Ivanov I','active',5000000),
('880102000002','Petrov P','active',3000000),
('880103000003','Sidorov S','blocked',1000000),
('880104000004','Aiman N','active',8000000),
('880105000005','Olga O','frozen',2000000),
('880106000006','Maxim M','active',5000000),
('880107000007','Dana D','active',2000000),
('880108000008','Akbar A','active',7000000),
('880109000009','Lina L','active',4000000),
('880110000010','Sam S','active',6000000);

INSERT INTO accounts (customer_id, account_number, currency, balance) VALUES
(1,'KZ01-0001','KZT',1000000),
(1,'KZ01-0002','USD',1000),
(2,'KZ01-0003','KZT',200000),
(3,'KZ01-0004','EUR',500),
(4,'KZ01-0005','KZT',5000000),
(5,'KZ01-0006','RUB',50000),
(6,'KZ01-0007','USD',300),
(7,'KZ01-0008','KZT',150000),
(8,'KZ01-0009','EUR',1200),
(9,'KZ01-0010','KZT',50000),
(10,'KZ01-0011','USD',2000);

INSERT INTO exchange_rates (from_currency,to_currency,rate) VALUES
('USD','KZT',460),('EUR','KZT',500),('RUB','KZT',6),('KZT','KZT',1),
('USD','EUR',0.92),('EUR','USD',1.09);


CREATE OR REPLACE FUNCTION get_rate(from_cur TEXT, to_cur TEXT)
RETURNS NUMERIC AS $$
DECLARE r NUMERIC;
BEGIN
  IF from_cur = to_cur THEN RETURN 1; END IF;
  SELECT rate INTO r FROM exchange_rates
    WHERE from_currency=from_cur AND to_currency=to_cur
    ORDER BY valid_from DESC LIMIT 1;
  IF r IS NULL THEN RAISE EXCEPTION 'Rate not found % -> %', from_cur, to_cur; END IF;
  RETURN r;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION process_transfer(
  p_from TEXT, p_to TEXT,
  p_amount NUMERIC, p_currency TEXT,
  p_desc TEXT DEFAULT NULL
) RETURNS TEXT AS $$
DECLARE
  v_from accounts%ROWTYPE;
  v_to accounts%ROWTYPE;
  v_cust customers%ROWTYPE;
  v_kzt NUMERIC;
  v_today NUMERIC;
BEGIN
  SELECT * INTO v_from FROM accounts WHERE account_number=p_from FOR UPDATE;
  IF NOT FOUND THEN RETURN 'FROM_NOT_FOUND'; END IF;

  SELECT * INTO v_to FROM accounts WHERE account_number=p_to FOR UPDATE;
  IF NOT FOUND THEN RETURN 'TO_NOT_FOUND'; END IF;

  SELECT * INTO v_cust FROM customers WHERE customer_id=v_from.customer_id;
  IF v_cust.status <> 'active' THEN RETURN 'CUSTOMER_INACTIVE'; END IF;

  
  IF p_currency = v_from.currency THEN
    IF v_from.balance < p_amount THEN RETURN 'NO_MONEY'; END IF;
  ELSE
    IF v_from.balance < p_amount * get_rate(p_currency,v_from.currency) THEN RETURN 'NO_MONEY_CONV'; END IF;
  END IF;

  
  v_kzt := p_amount * get_rate(p_currency,'KZT');
  SELECT COALESCE(SUM(amount_kzt),0) INTO v_today
  FROM transactions WHERE from_account_id=v_from.account_id AND created_at::date = now()::date;

  IF v_today + v_kzt > v_cust.daily_limit_kzt THEN RETURN 'LIMIT_EXCEEDED'; END IF;

  
  IF p_currency = v_from.currency THEN
    UPDATE accounts SET balance = balance - p_amount WHERE account_id=v_from.account_id;
  ELSE
    UPDATE accounts SET balance = balance - (p_amount * get_rate(p_currency,v_from.currency))
    WHERE account_id=v_from.account_id;
  END IF;

  
  IF p_currency = v_to.currency THEN
    UPDATE accounts SET balance = balance + p_amount WHERE account_id=v_to.account_id;
  ELSE
    UPDATE accounts SET balance = balance + (p_amount * get_rate(p_currency,v_to.currency))
    WHERE account_id=v_to.account_id;
  END IF;

  INSERT INTO transactions(from_account_id,to_account_id,amount,currency,amount_kzt,type,status,description)
  VALUES (v_from.account_id,v_to.account_id,p_amount,p_currency,v_kzt,'transfer','completed',p_desc);

  INSERT INTO audit_log(info) VALUES (jsonb_build_object('type','transfer','from',p_from,'to',p_to,'amount',p_amount));

  RETURN 'OK';
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE VIEW customer_balance_summary AS
SELECT c.customer_id, c.full_name,
       a.account_number, a.currency, a.balance,
       (a.balance * get_rate(a.currency,'KZT')) AS balance_kzt
FROM customers c JOIN accounts a ON a.customer_id=c.customer_id;

CREATE OR REPLACE VIEW daily_transaction_report AS
SELECT date(created_at) AS day,
       type,
       count(*) AS cnt,
       sum(amount_kzt) AS total_kzt,
       avg(amount_kzt) AS avg_kzt
FROM transactions
GROUP BY day, type
ORDER BY day DESC;

CREATE OR REPLACE VIEW suspicious_activity
WITH (security_barrier = true) AS
SELECT t.transaction_id, t.from_account_id, t.amount_kzt,
       (t.amount_kzt > 5000000) AS suspicious
FROM transactions t
WHERE t.amount_kzt > 5000000;


CREATE INDEX idx_tx_created ON transactions(created_at);
CREATE INDEX idx_tx_from_date ON transactions(from_account_id,created_at);
CREATE INDEX idx_accounts_active ON accounts(account_number) WHERE is_active=true;
CREATE INDEX idx_accounts_lower ON accounts(lower(account_number));
CREATE INDEX idx_audit_gin ON audit_log USING GIN(info);


CREATE OR REPLACE FUNCTION process_salary_batch(p_company TEXT, p_pay JSONB)
RETURNS JSONB AS $$
DECLARE
  v_comp accounts%ROWTYPE;
  v_lock BIGINT;
  rec RECORD;
  v_total NUMERIC := 0;
  v_succ INT := 0;
  v_fail INT := 0;
  v_fail_list JSONB := '[]'::JSONB;
BEGIN
  v_lock := hashtext(p_company);
  PERFORM pg_advisory_lock(v_lock);

  SELECT * INTO v_comp FROM accounts WHERE account_number=p_company FOR UPDATE;
  IF NOT FOUND THEN
    PERFORM pg_advisory_unlock(v_lock);
    RETURN jsonb_build_object('success',false,'error','NO_COMPANY_ACCOUNT');
  END IF;

  
  FOR rec IN SELECT * FROM jsonb_to_recordset(p_pay) AS (iin TEXT, amount NUMERIC, currency TEXT, description TEXT)
  LOOP
    v_total := v_total + rec.amount * get_rate(COALESCE(rec.currency,v_comp.currency), v_comp.currency);
  END LOOP;

  IF v_total > v_comp.balance THEN
    PERFORM pg_advisory_unlock(v_lock);
    RETURN jsonb_build_object('success',false,'error','NO_MONEY');
  END IF;

 
  FOR rec IN SELECT * FROM jsonb_to_recordset(p_pay) AS (iin TEXT, amount NUMERIC, currency TEXT, description TEXT)
  LOOP
    DECLARE v_cust BIGINT; v_acc accounts%ROWTYPE; v_amt_t NUMERIC;
    BEGIN
      SELECT customer_id INTO v_cust FROM customers WHERE iin = rec.iin;
      IF v_cust IS NULL THEN
        v_fail := v_fail + 1;
        v_fail_list := v_fail_list || jsonb_build_object('iin',rec.iin,'reason','NOT_FOUND');
        CONTINUE;
      END IF;

      SELECT * INTO v_acc FROM accounts WHERE customer_id=v_cust AND is_active=true LIMIT 1;
      IF NOT FOUND THEN
        v_fail := v_fail + 1;
        v_fail_list := v_fail_list || jsonb_build_object('iin',rec.iin,'reason','NO_ACTIVE_ACCOUNT');
        CONTINUE;
      END IF;

      UPDATE accounts
      SET balance = balance - (rec.amount * get_rate(COALESCE(rec.currency,v_comp.currency), v_comp.currency))
      WHERE account_id = v_comp.account_id;

      v_amt_t := rec.amount * get_rate(COALESCE(rec.currency,v_comp.currency), v_acc.currency);
      UPDATE accounts SET balance = balance + v_amt_t WHERE account_id = v_acc.account_id;

      INSERT INTO transactions(from_account_id,to_account_id,amount,currency,amount_kzt,type,status,description)
      VALUES (v_comp.account_id,v_acc.account_id,rec.amount,rec.currency,
              rec.amount * get_rate(COALESCE(rec.currency,v_comp.currency),'KZT'),
              'salary','completed',rec.description);

      v_succ := v_succ + 1;
    END;
  END LOOP;

  PERFORM pg_advisory_unlock(v_lock);
  RETURN jsonb_build_object('success',true,'successful',v_succ,'failed',v_fail,'failed_list',v_fail_list);
END;
$$ LANGUAGE plpgsql;

SELECT process_transfer('KZ01-0001','KZ01-0003', 10000,'KZT','Test 1');
 process_transfer 
------------------
 OK
(1 row)

SELECT process_transfer('KZ01-0002','KZ01-0009', 20,'USD','USD→EUR');
 process_transfer 
------------------
 OK
(1 row)

SELECT process_transfer('BAD','KZ01-0003', 10,'KZT','Err1');
 process_transfer 
------------------
 FROM_NOT_FOUND
(1 row)

SELECT process_transfer('KZ01-0003','KZ01-0001', 999999,'KZT');
 process_transfer 
------------------
 NO_MONEY
(1 row)
  
SELECT process_transfer('KZ01-0004','KZ01-0001', 10,'EUR');
 process_transfer  
-------------------
 CUSTOMER_INACTIVE
(1 row)

SELECT process_salary_batch('KZ01-0005',
  '[{"iin":"880106000006","amount":100,"currency":"USD"}]'::jsonb);
                        process_salary_batch                        
--------------------------------------------------------------------
 {"failed": 0, "success": true, "successful": 1, "failed_list": []}
(1 row)

EXPLAIN ANALYZE
SELECT * FROM transactions
WHERE from_account_id = 1 AND created_at > now() - INTERVAL '7 days';
                                                           QUERY PLAN                                                            
---------------------------------------------------------------------------------------------------------------------------------
 Index Scan using idx_tx_from_date on transactions  (cost=0.15..8.17 rows=1 width=200) (actual time=0.014..0.016 rows=1 loops=1)
   Index Cond: ((from_account_id = 1) AND (created_at > (now() - '7 days'::interval)))
 Planning Time: 0.066 ms
 Execution Time: 0.027 ms
(4 rows)

SELECT * FROM suspicious_activity;
 transaction_id | from_account_id | amount_kzt | suspicious 
----------------+-----------------+------------+------------
(0 rows)
