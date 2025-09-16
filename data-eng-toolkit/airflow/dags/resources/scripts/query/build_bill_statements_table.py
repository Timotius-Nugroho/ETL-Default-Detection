def build_sql() -> str:
    return """
    INSERT INTO silver.bill_statements (id, client_id, month, bill_amount)
    SELECT bs.id, bs.client_id, bs.month, bs.bill_amount
    FROM external.bill_statements bs
    JOIN external.clients c ON bs.client_id = c.client_id
    WHERE c.age >= 18
      AND bs.bill_amount >= 0;
    """
