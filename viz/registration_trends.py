# viz/registration_trends.py
import pandas as pd
import matplotlib.pyplot as plt
from utils.db import get_engine

def plot_registration_trends():
    engine = get_engine()
    query = """
    SELECT registration_date, SUM(total) as total
    FROM registration.voter_registration
    GROUP BY registration_date
    ORDER BY registration_date;
    """
    df = pd.read_sql(query, engine)

    plt.figure(figsize=(12, 6))
    plt.plot(df['registration_date'], df['total'], marker='o')
    plt.title("Total Voter Registrations Over Time")
    plt.xlabel("Date")
    plt.ylabel("Total Registered Voters")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig("viz/output/registration_trends.png")
    print("✅ Saved: viz/output/registration_trends.png")

if __name__ == "__main__":
    plot_registration_trends()
