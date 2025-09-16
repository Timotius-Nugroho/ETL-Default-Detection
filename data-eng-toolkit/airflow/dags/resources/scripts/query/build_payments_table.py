def build_sql() -> str:
    return """
    INSERT INTO silver.payments (id, client_id, month, payment_amount)
    SELECT p.id, p.client_id, p.month, p.payment_amount
    FROM external.payments p
    JOIN external.clients c ON p.client_id = c.client_id
    WHERE c.age >= 18;
    """