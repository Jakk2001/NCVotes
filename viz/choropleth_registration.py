# viz/choropleth_registration.py
import matplotlib.pyplot as plt
from utils.geo import load_county_geojson
from etl.choropleth import get_registration_by_county
import mplcursors

from viz.style import PARTY_COLORS, DEFAULT_FONT, DEFAULT_FONT_SIZE, TITLE_FONT_SIZE
import mplcursors
import matplotlib.pyplot as plt
plt.rcParams["font.family"] = DEFAULT_FONT
plt.rcParams["font.size"] = DEFAULT_FONT_SIZE

cursor = mplcursors.cursor(hover=True)
cursor.connect("add", lambda sel: sel.annotation.set_text(f"Party: {sel.target[0]:.0f}, Value: {sel.target[1]:.0f}"))

def plot_registration_choropleth():
    gdf = load_county_geojson()
    reg_df = get_registration_by_county()

    # Normalize county names for merging
    gdf['county_name'] = gdf['County'].str.lower().str.replace(" county", "", regex=False)
    reg_df['county_name'] = reg_df['county'].str.lower()

    # Merge
    merged = gdf.merge(reg_df, on="county_name", how="left")

    # Plot
    fig, ax = plt.subplots(1, 1, figsize=(10, 8))
    merged.plot(column="registered", cmap="Blues", linewidth=0.8, ax=ax, edgecolor="0.8", legend=True)

    ax.set_title("Voter Registration by County in North Carolina", fontsize=14)
    ax.axis("off")
    plt.tight_layout()
    plt.savefig("viz/output/GEObreakdown.png")
    

if __name__ == "__main__":
    plot_registration_choropleth()
