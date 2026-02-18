"""Run database migration to update raw_voters schema for NC SBE layout change (02/01/2026)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from src.database.connection import get_engine
from sqlalchemy import text

def migrate():
    engine = get_engine()
    
    statements = [
        "ALTER TABLE raw.raw_voters ADD COLUMN IF NOT EXISTS ssn CHAR(1)",
        "ALTER TABLE raw.raw_voters ADD COLUMN IF NOT EXISTS no_dl_ssn_chkbx CHAR(1)",
        "ALTER TABLE raw.raw_voters ADD COLUMN IF NOT EXISTS hava_id_req CHAR(1)",
        "ALTER TABLE raw.raw_voters DROP COLUMN IF EXISTS dist_2_abbrv",
        "ALTER TABLE raw.raw_voters DROP COLUMN IF EXISTS dist_2_desc",
    ]
    
    with engine.connect() as conn:
        for stmt in statements:
            print(f"Running: {stmt}")
            conn.execute(text(stmt))
        conn.commit()
    
    print("Migration complete.")

if __name__ == "__main__":
    migrate()