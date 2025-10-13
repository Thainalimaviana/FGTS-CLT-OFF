import sqlite3

def init_db(db_path="consultas.db"):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS consultas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cpf TEXT,
            data TEXT,
            resultado TEXT
            lote_id TEXT
        )
    """)
    try:
        c.execute("ALTER TABLE consultas ADD COLUMN lote_id TEXT")
    except Exception:
        pass

    c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_consultas_cpf_lote ON consultas(cpf, lote_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_consultas_lote_id ON consultas(lote_id)")

    conn.commit()
    conn.close()
