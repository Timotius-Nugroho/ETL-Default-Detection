def build_sql() -> str:
    return """
    INSERT INTO silver.payment_history (id, client_id, month, pay_status)
    SELECT ph.id, ph.client_id, ph.month, ph.pay_status
    FROM external.payment_history ph
    JOIN external.clients c ON ph.client_id = c.client_id
    WHERE c.age >= 18;
    """
