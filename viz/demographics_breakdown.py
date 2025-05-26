# viz/demographics_breakdown.py
import pandas as pd
import matplotlib.pyplot as plt
from utils.db import get_engine

def bar_chart_by_party():
    engine = get_engine()
    query = """
    SELECT party, SUM(total) as total
    FROM registration.voter_registration
    GROUP BY party
    ORDER BY total DESC;
    """
    df = pd.read_sql(query, engine)

    plt.figure(figsize=(8, 6))
    plt.bar(df['party'], df['total'], color='skyblue')
    plt.title("Voter Registration by Party")
    plt.xlabel("Party")
    plt.ylabel("Registered Voters")
    plt.tight_layout()
    plt.savefig("viz/output/party_breakdown.png")
    print("✅ Saved: viz/output/party_breakdown.png")

if __name__ == "__main__":
    bar_chart_by_party()
