from pathlib import Path
from sqlalchemy import text
from src.db.engine import get_engine

def main():
    engine = get_engine()
    schema_path = Path(__file__).with_name("schema.sql")
    sql = schema_path.read_text(encoding="utf-8")

    with engine.begin() as conn:
        for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
            conn.execute(text(stmt))

    print("✅ DB initialized.")

if __name__ == "__main__":
    main()