"""
Create interactive voter registration trend visualizations using Plotly.
Tracks how demographics change over time.
"""
import logging
import sys
from pathlib import Path
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.database.connection import get_engine
from config.settings import CHARTS_DIR, PARTY_COLORS

# Import the trend queries - either from queries_trends module or add to queries.py
try:
    from src.database.queries_trends import (
        get_party_trends_over_time,
        get_age_group_trends_over_time,
        get_gender_trends_over_time,
        get_race_trends_over_time,
        get_weekly_registration_counts,
        get_cumulative_registration
    )
except ImportError:
    # If queries_trends doesn't exist, try importing from main queries module
    from src.database.queries import (
        get_party_trends_over_time,
        get_age_group_trends_over_time,
        get_gender_trends_over_time,
        get_race_trends_over_time,
        get_weekly_registration_counts,
        get_cumulative_registration
    )

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

GENDER_COLORS = {'M': '#4A90E2', 'F': '#E24A90', 'U': '#888888'}
AGE_COLORS = {
    '18-25': '#FF6B6B',
    '26-35': '#4ECDC4',
    '36-50': '#45B7D1',
    '51-65': '#96CEB4',
    '65+': '#FFEAA7'
}
RACE_COLORS = {
    'W': '#4A90E2',  # White - Blue
    'B': '#2E7D32',  # Black - Green
    'A': '#F57C00',  # Asian - Orange
    'I': '#9C27B0',  # Native American - Purple
    'M': '#FF6B6B',  # Multiracial - Red
    'O': '#795548',  # Other - Brown
    'U': '#888888'   # Undesignated - Gray
}

def save_chart(fig, filename):
    """Save Plotly figure as HTML."""
    output_path = CHARTS_DIR / filename
    CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    fig.write_html(str(output_path))
    logger.info(f"Saved: {output_path}")

def plot_party_trends():
    """Interactive multi-line chart showing party registration over time."""
    try:
        engine = get_engine()
        df = get_party_trends_over_time(engine)
        
        if df.empty:
            return False
        
        # Create cumulative totals for each party
        df = df.sort_values(['party', 'registration_date'])
        df['cumulative'] = df.groupby('party')['total'].cumsum()
        
        fig = go.Figure()
        
        for party in df['party'].unique():
            party_df = df[df['party'] == party]
            fig.add_trace(go.Scatter(
                x=party_df['registration_date'],
                y=party_df['cumulative'],
                name=party,
                mode='lines',
                line=dict(color=PARTY_COLORS.get(party, '#888888'), width=3),
                hovertemplate='<b>' + party + '</b><br>Date: %{x|%Y-%m-%d}<br>Total Registered: %{y:,}<br><extra></extra>'
            ))
        
        fig.update_layout(
            title='Party Registration Trends Over Time (Cumulative)',
            xaxis_title='Registration Date',
            yaxis_title='Cumulative Registered Voters',
            hovermode='x unified',
            height=600,
            legend=dict(
                orientation="v",
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01
            )
        )
        
        save_chart(fig, 'party_trends.html')
        return True
        
    except Exception as e:
        logger.error(f"Failed to create party trends: {e}")
        return False

def plot_age_group_trends():
    """Interactive multi-line chart showing age group registration over time."""
    try:
        engine = get_engine()
        df = get_age_group_trends_over_time(engine)
        
        if df.empty:
            return False
        
        # Create cumulative totals for each age group
        df = df.sort_values(['age_group', 'registration_date'])
        df['cumulative'] = df.groupby('age_group')['total'].cumsum()
        
        fig = go.Figure()
        
        for age_group in ['18-25', '26-35', '36-50', '51-65', '65+']:
            if age_group in df['age_group'].values:
                age_df = df[df['age_group'] == age_group]
                fig.add_trace(go.Scatter(
                    x=age_df['registration_date'],
                    y=age_df['cumulative'],
                    name=age_group,
                    mode='lines',
                    line=dict(color=AGE_COLORS.get(age_group, '#888888'), width=3),
                    hovertemplate='<b>' + age_group + '</b><br>Date: %{x|%Y-%m-%d}<br>Total Registered: %{y:,}<br><extra></extra>'
                ))
        
        fig.update_layout(
            title='Age Group Registration Trends Over Time (Cumulative)',
            xaxis_title='Registration Date',
            yaxis_title='Cumulative Registered Voters',
            hovermode='x unified',
            height=600,
            legend=dict(
                orientation="v",
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01
            )
        )
        
        save_chart(fig, 'age_group_trends.html')
        return True
        
    except Exception as e:
        logger.error(f"Failed to create age group trends: {e}")
        return False

def plot_gender_trends():
    """Interactive multi-line chart showing gender registration over time."""
    try:
        engine = get_engine()
        df = get_gender_trends_over_time(engine)
        
        if df.empty:
            return False
        
        # Create cumulative totals for each gender
        df = df.sort_values(['gender', 'registration_date'])
        df['cumulative'] = df.groupby('gender')['total'].cumsum()
        
        fig = go.Figure()
        
        for gender in df['gender'].unique():
            gender_df = df[df['gender'] == gender]
            fig.add_trace(go.Scatter(
                x=gender_df['registration_date'],
                y=gender_df['cumulative'],
                name=gender,
                mode='lines',
                line=dict(color=GENDER_COLORS.get(gender, '#888888'), width=3),
                hovertemplate='<b>Gender: ' + gender + '</b><br>Date: %{x|%Y-%m-%d}<br>Total Registered: %{y:,}<br><extra></extra>'
            ))
        
        fig.update_layout(
            title='Gender Registration Trends Over Time (Cumulative)',
            xaxis_title='Registration Date',
            yaxis_title='Cumulative Registered Voters',
            hovermode='x unified',
            height=600,
            legend=dict(
                orientation="v",
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01
            )
        )
        
        save_chart(fig, 'gender_trends.html')
        return True
        
    except Exception as e:
        logger.error(f"Failed to create gender trends: {e}")
        return False

def plot_race_trends():
    """Interactive multi-line chart showing race registration over time."""
    try:
        engine = get_engine()
        df = get_race_trends_over_time(engine)
        
        if df.empty:
            return False
        
        # Create cumulative totals for each race
        df = df.sort_values(['race', 'registration_date'])
        df['cumulative'] = df.groupby('race')['total'].cumsum()
        
        fig = go.Figure()
        
        race_labels = {
            'W': 'White',
            'B': 'Black',
            'A': 'Asian',
            'I': 'Native American',
            'M': 'Multiracial',
            'O': 'Other',
            'U': 'Undesignated'
        }
        
        for race in df['race'].unique():
            race_df = df[df['race'] == race]
            race_label = race_labels.get(race, race)
            fig.add_trace(go.Scatter(
                x=race_df['registration_date'],
                y=race_df['cumulative'],
                name=race_label,
                mode='lines',
                line=dict(color=RACE_COLORS.get(race, '#888888'), width=3),
                hovertemplate='<b>' + race_label + '</b><br>Date: %{x|%Y-%m-%d}<br>Total Registered: %{y:,}<br><extra></extra>'
            ))
        
        fig.update_layout(
            title='Race Registration Trends Over Time (Cumulative)',
            xaxis_title='Registration Date',
            yaxis_title='Cumulative Registered Voters',
            hovermode='x unified',
            height=600,
            legend=dict(
                orientation="v",
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01
            )
        )
        
        save_chart(fig, 'race_trends.html')
        return True
        
    except Exception as e:
        logger.error(f"Failed to create race trends: {e}")
        return False

def plot_weekly_registrations():
    """Interactive bar chart showing weekly registration patterns."""
    try:
        engine = get_engine()
        df = get_weekly_registration_counts(engine)
        
        if df.empty:
            return False
        
        fig = go.Figure(data=[
            go.Bar(
                x=df['week_start'],
                y=df['total'],
                marker_color='#634ea3',
                hovertemplate='<b>Week of %{x|%Y-%m-%d}</b><br>Registrations: %{y:,}<br><extra></extra>'
            )
        ])
        
        fig.update_layout(
            title='Weekly Voter Registrations',
            xaxis_title='Week',
            yaxis_title='New Registrations',
            hovermode='x unified',
            height=500
        )
        
        save_chart(fig, 'weekly_registrations.html')
        return True
        
    except Exception as e:
        logger.error(f"Failed to create weekly registrations: {e}")
        return False

def plot_cumulative_total():
    """Interactive area chart showing cumulative registration growth."""
    try:
        engine = get_engine()
        df = get_cumulative_registration(engine)
        
        if df.empty:
            return False
        
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=df['registration_date'],
            y=df['cumulative_total'],
            mode='lines',
            fill='tozeroy',
            line=dict(color='#634ea3', width=2),
            fillcolor='rgba(99, 78, 163, 0.3)',
            hovertemplate='<b>Date: %{x|%Y-%m-%d}</b><br>Total Registered: %{y:,}<br><extra></extra>'
        ))
        
        fig.update_layout(
            title='Cumulative Voter Registration Growth',
            xaxis_title='Date',
            yaxis_title='Total Registered Voters',
            hovermode='x unified',
            height=500
        )
        
        save_chart(fig, 'cumulative_registration.html')
        return True
        
    except Exception as e:
        logger.error(f"Failed to create cumulative registration: {e}")
        return False
    
def generate_key_stats():
    """Generate key statistics and save to JSON file for fast page loads."""
    try:
        from src.database.queries_key_stats import get_key_stats
        import json
        
        logger.info("Generating key statistics...")
        engine = get_engine()
        stats = get_key_stats(engine)
        
        # Save to JSON file
        stats_file = CHARTS_DIR / 'trends_key_stats.json'
        with open(stats_file, 'w') as f:
            json.dump(stats, f, indent=2)
        
        logger.info(f"✓ Key statistics saved to {stats_file}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to generate key statistics: {e}")
        return False

def generate_all_trends():
    """Generate all trend visualizations and key statistics."""
    logger.info("Generating all trend visualizations...")
    
    success_count = 0
    total_count = 7  # Updated to include key stats
    
    if plot_party_trends():
        success_count += 1
    if plot_age_group_trends():
        success_count += 1
    if plot_gender_trends():
        success_count += 1
    if plot_race_trends():
        success_count += 1
    if plot_weekly_registrations():
        success_count += 1
    if plot_cumulative_total():
        success_count += 1
    if generate_key_stats():  # ADD THIS LINE
        success_count += 1
    
    logger.info(f"Generated {success_count}/{total_count} trend visualizations")
    return success_count == total_count

def main():
    """Entry point for command-line execution."""
    success = generate_all_trends()
    if not success:
        logger.error("Some trend visualizations failed")
        exit(1)
    logger.info("All trend visualizations created successfully")

if __name__ == "__main__":
    main()