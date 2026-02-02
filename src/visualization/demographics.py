import logging
import plotly.graph_objects as go
from pathlib import Path
from src.database.connection import get_engine
from src.database.queries import (
    get_registration_by_party,
    get_party_by_race,
    get_party_by_gender,
    get_party_by_age_group,
    get_gender_breakdown,
    get_gender_by_age_group,
    get_gender_by_race,
    get_age_group_breakdown,
    get_race_breakdown
)
from config.settings import CHARTS_DIR, PARTY_COLORS

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

def save_chart(fig, filename):
    """Save Plotly figure as HTML."""
    output_path = CHARTS_DIR / filename
    CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    fig.write_html(str(output_path))
    logger.info(f"Saved: {output_path}")

def plot_party_breakdown():
    """Interactive bar chart of party registration."""
    try:
        engine = get_engine()
        df = get_registration_by_party(engine)
        
        if df.empty:
            return False
        
        colors = [PARTY_COLORS.get(p, '#888888') for p in df['party']]
        
        fig = go.Figure(data=[
            go.Bar(
                x=df['party'],
                y=df['total'],
                marker_color=colors,
                marker_line_color='black',
                marker_line_width=0.5,
                hovertemplate='<b>%{x}</b><br>Registered: %{y:,}<br><extra></extra>'
            )
        ])
        
        fig.update_layout(
            title='Voter Registration by Party in North Carolina',
            xaxis_title='Party Affiliation',
            yaxis_title='Registered Voters',
            hovermode='closest',
            height=500
        )
        
        save_chart(fig, 'party_breakdown.html')
        return True
        
    except Exception as e:
        logger.error(f"Failed to create party breakdown: {e}")
        return False

def plot_party_by_race():
    """Interactive grouped bar chart of party by race."""
    try:
        engine = get_engine()
        df = get_party_by_race(engine)
        
        if df.empty:
            return False
        
        top_races = df.groupby('race')['total'].sum().nlargest(6).index
        df = df[df['race'].isin(top_races)]
        
        fig = go.Figure()
        
        for party in df['party'].unique():
            party_df = df[df['party'] == party]
            fig.add_trace(go.Bar(
                name=party,
                x=party_df['race'],
                y=party_df['total'],
                marker_color=PARTY_COLORS.get(party, '#888888'),
                hovertemplate='<b>%{x}</b><br>Party: ' + party + '<br>Count: %{y:,}<br><extra></extra>'
            ))
        
        fig.update_layout(
            title='Party Registration by Race',
            xaxis_title='Race',
            yaxis_title='Registered Voters',
            barmode='group',
            height=500
        )
        
        save_chart(fig, 'party_by_race.html')
        return True
        
    except Exception as e:
        logger.error(f"Failed to create party by race: {e}")
        return False

def plot_party_by_gender():
    """Interactive grouped bar chart of party by gender."""
    try:
        engine = get_engine()
        df = get_party_by_gender(engine)
        
        if df.empty:
            return False
        
        fig = go.Figure()
        
        for party in df['party'].unique():
            party_df = df[df['party'] == party]
            fig.add_trace(go.Bar(
                name=party,
                x=party_df['gender'],
                y=party_df['total'],
                marker_color=PARTY_COLORS.get(party, '#888888'),
                hovertemplate='<b>%{x}</b><br>Party: ' + party + '<br>Count: %{y:,}<br><extra></extra>'
            ))
        
        fig.update_layout(
            title='Party Registration by Gender',
            xaxis_title='Gender',
            yaxis_title='Registered Voters',
            barmode='group',
            height=500
        )
        
        save_chart(fig, 'party_by_gender.html')
        return True
        
    except Exception as e:
        logger.error(f"Failed to create party by gender: {e}")
        return False

def plot_party_by_age_group():
    """Interactive stacked bar chart of party by age."""
    try:
        engine = get_engine()
        df = get_party_by_age_group(engine)
        
        if df.empty:
            return False
        
        fig = go.Figure()
        
        for party in df['party'].unique():
            party_df = df[df['party'] == party]
            fig.add_trace(go.Bar(
                name=party,
                x=party_df['age_group'],
                y=party_df['total'],
                marker_color=PARTY_COLORS.get(party, '#888888'),
                hovertemplate='<b>%{x}</b><br>Party: ' + party + '<br>Count: %{y:,}<br><extra></extra>'
            ))
        
        fig.update_layout(
            title='Party Registration by Age Group',
            xaxis_title='Age Group',
            yaxis_title='Registered Voters',
            barmode='stack',
            height=500
        )
        
        save_chart(fig, 'party_by_age.html')
        return True
        
    except Exception as e:
        logger.error(f"Failed to create party by age: {e}")
        return False

def plot_gender_breakdown():
    """Interactive pie chart of gender distribution."""
    try:
        engine = get_engine()
        df = get_gender_breakdown(engine)
        
        if df.empty:
            return False
        
        colors = [GENDER_COLORS.get(g, '#888888') for g in df['gender']]
        
        fig = go.Figure(data=[go.Pie(
            labels=df['gender'],
            values=df['total'],
            marker=dict(colors=colors, line=dict(color='white', width=2)),
            hovertemplate='<b>%{label}</b><br>Count: %{value:,}<br>Percentage: %{percent}<br><extra></extra>'
        )])
        
        fig.update_layout(
            title='Voter Registration by Gender',
            height=500
        )
        
        save_chart(fig, 'gender_breakdown.html')
        return True
        
    except Exception as e:
        logger.error(f"Failed to create gender breakdown: {e}")
        return False

def plot_gender_by_age_group():
    """Interactive grouped bar chart of gender by age."""
    try:
        engine = get_engine()
        df = get_gender_by_age_group(engine)
        
        if df.empty:
            return False
        
        fig = go.Figure()
        
        for gender in df['gender'].unique():
            gender_df = df[df['gender'] == gender]
            fig.add_trace(go.Bar(
                name=gender,
                x=gender_df['age_group'],
                y=gender_df['total'],
                marker_color=GENDER_COLORS.get(gender, '#888888'),
                hovertemplate='<b>%{x}</b><br>Gender: ' + gender + '<br>Count: %{y:,}<br><extra></extra>'
            ))
        
        fig.update_layout(
            title='Gender Distribution by Age Group',
            xaxis_title='Age Group',
            yaxis_title='Registered Voters',
            barmode='group',
            height=500
        )
        
        save_chart(fig, 'gender_by_age.html')
        return True
        
    except Exception as e:
        logger.error(f"Failed to create gender by age: {e}")
        return False

def plot_gender_by_race():
    """Interactive stacked bar chart of gender by race."""
    try:
        engine = get_engine()
        df = get_gender_by_race(engine)
        
        if df.empty:
            return False
        
        race_totals = df.groupby('race')['total'].sum().nlargest(6)
        df = df[df['race'].isin(race_totals.index)]
        
        fig = go.Figure()
        
        for gender in df['gender'].unique():
            gender_df = df[df['gender'] == gender]
            fig.add_trace(go.Bar(
                name=gender,
                x=gender_df['race'],
                y=gender_df['total'],
                marker_color=GENDER_COLORS.get(gender, '#888888'),
                hovertemplate='<b>%{x}</b><br>Gender: ' + gender + '<br>Count: %{y:,}<br><extra></extra>'
            ))
        
        fig.update_layout(
            title='Gender Distribution by Race',
            xaxis_title='Race',
            yaxis_title='Registered Voters',
            barmode='stack',
            height=500
        )
        
        save_chart(fig, 'gender_by_race.html')
        return True
        
    except Exception as e:
        logger.error(f"Failed to create gender by race: {e}")
        return False

def plot_age_group_breakdown():
    """Interactive pie chart of age distribution."""
    try:
        engine = get_engine()
        df = get_age_group_breakdown(engine)
        
        if df.empty:
            return False
        
        colors = [AGE_COLORS.get(age, '#888888') for age in df['age_group']]
        
        fig = go.Figure(data=[go.Pie(
            labels=df['age_group'],
            values=df['total'],
            marker=dict(colors=colors, line=dict(color='white', width=2)),
            hovertemplate='<b>%{label}</b><br>Count: %{value:,}<br>Percentage: %{percent}<br><extra></extra>'
        )])
        
        fig.update_layout(
            title='Voter Registration by Age Group',
            height=500
        )
        
        save_chart(fig, 'age_breakdown.html')
        return True
        
    except Exception as e:
        logger.error(f"Failed to create age breakdown: {e}")
        return False

def plot_race_breakdown():
    """Interactive pie chart of race distribution."""
    try:
        engine = get_engine()
        df = get_race_breakdown(engine)
        
        if df.empty:
            return False
        
        import pandas as pd
        top_races = df.nlargest(6, 'total')
        others_total = df[~df['race'].isin(top_races['race'])]['total'].sum()
        
        if others_total > 0:
            others_row = pd.DataFrame({'race': ['Other'], 'total': [others_total]})
            plot_df = pd.concat([top_races, others_row], ignore_index=True)
        else:
            plot_df = top_races
        
        fig = go.Figure(data=[go.Pie(
            labels=plot_df['race'],
            values=plot_df['total'],
            marker=dict(line=dict(color='white', width=2)),
            hovertemplate='<b>%{label}</b><br>Count: %{value:,}<br>Percentage: %{percent}<br><extra></extra>'
        )])
        
        fig.update_layout(
            title='Voter Registration by Race',
            height=500
        )
        
        save_chart(fig, 'race_breakdown.html')
        return True
        
    except Exception as e:
        logger.error(f"Failed to create race breakdown: {e}")
        return False

def generate_all_demographics_charts():
    """Generate all demographics visualizations."""
    charts = [
        ("Party Breakdown", plot_party_breakdown),
        ("Party by Race", plot_party_by_race),
        ("Party by Gender", plot_party_by_gender),
        ("Party by Age", plot_party_by_age_group),
        ("Gender Breakdown", plot_gender_breakdown),
        ("Gender by Age", plot_gender_by_age_group),
        ("Gender by Race", plot_gender_by_race),
        ("Age Breakdown", plot_age_group_breakdown),
        ("Race Breakdown", plot_race_breakdown),
    ]
    
    results = {}
    for name, func in charts:
        logger.info(f"Generating {name}...")
        results[name] = func()
    
    success_count = sum(results.values())
    logger.info(f"Generated {success_count}/{len(charts)} charts")
    return results

def main():
    """Entry point for command-line execution."""
    results = generate_all_demographics_charts()
    if not all(results.values()):
        logger.error("Some visualizations failed")
        exit(1)
    logger.info("All visualizations completed")

if __name__ == "__main__":
    main()