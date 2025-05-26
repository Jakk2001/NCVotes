# viz/utils/db.py
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

def get_engine():
    return create_engine(
        f"postgresql://{os.getenv('PGUSER')}:{os.getenv('PGPASSWORD')}@"
        f"{os.getenv('PGHOST')}:{os.getenv('PGPORT')}/{os.getenv('PGDATABASE')}"
    )
