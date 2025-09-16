def build_sql() -> str:
    return """
    INSERT INTO silver.clients (client_id, limit_bal, sex, education, marriage, age)
    SELECT client_id, limit_bal, sex, education, marriage, age
    FROM external.clients
    WHERE age >= 18;
    """
