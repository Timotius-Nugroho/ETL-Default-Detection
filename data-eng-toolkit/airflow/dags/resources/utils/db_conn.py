import psycopg2

# Connection string ke Neon
conn = psycopg2.connect(
    dbname="sandbox",
    user="neondb_owner",
    password="npg_Aat0izDbJ7vR",
    host="ep-winter-rain-a1okoe1f-pooler.ap-southeast-1.aws.neon.tech",
    port="5432",
    sslmode="require"
)
