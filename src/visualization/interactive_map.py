"""
Create interactive choropleth maps with embedded subcategory controls.
Each main map (party, race, gender) has dropdown to switch between subcategories.
"""
import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import geopandas as gpd
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import json
import logging
from shapely.geometry import shape
from src.database.connection import get_engine
from src.database.queries import get_county_data_by_layer
from config.settings import GEO_DATA_DIR, OUTPUT_DIR

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_county_geometries():
    """Load NC county GeoJSON data."""
    geojson_path = GEO_DATA_DIR / "nc_counties.geojson"
    
    if not geojson_path.exists():
        logger.error(f"GeoJSON file not found: {geojson_path}")
        raise FileNotFoundError(f"County GeoJSON not found at {geojson_path}")
    
    logger.info(f"Loading county geometries from {geojson_path}")
    
    # Load GeoJSON directly using JSON and shapely (bypasses fiona issues)
    try:
        import json
        from shapely.geometry import shape
        
        with open(geojson_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if 'features' not in data:
            raise ValueError("GeoJSON file does not contain 'features' key")
        
        features = data['features']
        
        if not features:
            raise ValueError("GeoJSON file contains no features")
        
        geometries = [shape(feature['geometry']) for feature in features]
        properties = [feature['properties'] for feature in features]
        
        gdf = gpd.GeoDataFrame(properties, geometry=geometries)
        gdf.crs = "EPSG:4326"
        
        logger.info(f"Loaded {len(gdf)} county geometries successfully")
        
    except Exception as e:
        logger.error(f"Failed to load GeoJSON file: {e}")
        logger.error("Please ensure the file is valid GeoJSON format")
        raise
    
    # Normalize county names
    gdf['county_name'] = gdf['County'].str.lower().str.replace(" county", "", regex=False).str.strip()
    
    return gdf

def prepare_map_data(gdf, data_df, value_column, county_column='county'):
    """Merge geographic data with statistical data."""
    data_df['county_name'] = data_df[county_column].str.lower().str.strip()
    merged = gdf.merge(data_df, on='county_name', how='left')
    merged[value_column] = merged[value_column].fillna(0)
    return merged

def create_total_voters_map(output_filename='interactive_map_total.html'):
    """Create Total Voters map with logarithmic scale."""
    try:
        logger.info("Creating Total Voters map")
        
        gdf = load_county_geometries()
        engine = get_engine()
        data_df = get_county_data_by_layer(engine, 'total')
        
        if data_df.empty:
            logger.warning("No data found for total voters")
            return None
        
        merged = prepare_map_data(gdf, data_df, 'registered', 'county')
        merged['registered_log'] = np.log10(merged['registered'] + 1)
        
        geojson = json.loads(merged.to_json())
        
        fig = go.Figure(go.Choroplethmapbox(
            geojson=geojson,
            locations=merged.index,
            z=merged['registered_log'],
            customdata=list(zip(merged['County'], merged['registered'])),
            colorscale='Blues',
            hovertemplate="<b>%{customdata[0]}</b><br>Registered Voters: %{customdata[1]:,.0f}<br><extra></extra>",
            marker_opacity=0.7,
            marker_line_width=1,
            marker_line_color='white',
            showscale=True,
            colorbar=dict(title=dict(text='Log Scale'), thickness=15, len=0.7)
        ))
        
        fig.update_layout(
            title=dict(text="Total Registered Voters by County", font=dict(size=24, color='#16003f'), x=0.5, xanchor='center'),
            mapbox=dict(style='carto-positron', center=dict(lat=35.5, lon=-79.5), zoom=6),
            height=700,
            margin=dict(l=0, r=0, t=60, b=0),
            font=dict(family='Arial, sans-serif')
        )
        
        output_path = OUTPUT_DIR / 'maps' / output_filename
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        config = {
            'displayModeBar': True,
            'scrollZoom': True,
            'modeBarButtonsToRemove': ['select2d', 'lasso2d', 'toImage', 'zoom2d', 'pan2d', 'zoomIn2d', 'zoomOut2d', 'autoScale2d', 'resetScale2d'],
            'displaylogo': False
        }
        
        fig.write_html(str(output_path), config=config)
        logger.info(f"Total Voters map created: {output_path}")
        return output_path
        
    except Exception as e:
        logger.error(f"Failed to create Total Voters map: {e}", exc_info=True)
        return None

def create_party_map(output_filename='interactive_map_party.html'):
    """Create Partisan Affiliation map with dropdown to switch between all parties."""
    try:
        logger.info("Creating Partisan Affiliation map")
        
        gdf = load_county_geometries()
        engine = get_engine()
        data_df = get_county_data_by_layer(engine, 'party')
        
        if data_df.empty:
            logger.warning("No data found for party affiliation")
            return None
        
        merged = prepare_map_data(gdf, data_df, 'dem_pct', 'county')
        merged['dem_pct'] = merged['dem_pct'].fillna(0)
        merged['rep_pct'] = merged['rep_pct'].fillna(0)
        merged['una_pct'] = merged['una_pct'].fillna(0)
        merged['lib_pct'] = merged['lib_pct'].fillna(0)
        merged['gre_pct'] = merged['gre_pct'].fillna(0)
        merged['cst_pct'] = merged['cst_pct'].fillna(0)
        
        geojson = json.loads(merged.to_json())
        customdata = list(zip(
            merged['County'], merged['total'],
            merged['dem_pct'], merged['rep_pct'], merged['una_pct'],
            merged['lib_pct'], merged['gre_pct'], merged['cst_pct']
        ))
        
        # Create figure with Democrat data as default
        fig = go.Figure(go.Choroplethmapbox(
            geojson=geojson,
            locations=merged.index,
            z=merged['dem_pct'],
            customdata=customdata,
            colorscale= ["Blues"],
            coloraxis=dict(cmin=0, cmax=100),
            zmin=0,           
            zmax=100,
            hovertemplate=(
                "<b>%{customdata[0]}</b><br>"
                "Total Voters: %{customdata[1]:,.0f}<br>"
                "Democrat: %{customdata[2]:.1f}%<br>"
                "<extra></extra>"
            ),
            marker_opacity=0.7,
            marker_line_width=1,
            marker_line_color='white',
            showscale=True,
            colorbar=dict(title=dict(text='% Democrat'), thickness=15, len=0.7),
        ))
        
        # Add dropdown menu to switch between all parties
        fig.update_layout(
            updatemenus=[
                dict(
                    buttons=list([
                        dict(
                            args=[{
                                "z": [merged['dem_pct']], 
                                "colorscale": ["Blues"],
                                "zmin": 0,       
                                "zmax": 100,
                                "colorbar.title.text": "% Democrat",
                                "hovertemplate": "<b>%{customdata[0]}</b><br>Total Voters: %{customdata[1]:,.0f}<br>Democrat: %{customdata[2]:.1f}%<br><extra></extra>"
                            }],
                            label="% Democrat",
                            method="restyle"
                        ),
                        dict(
                            args=[{
                                "z": [merged['rep_pct']], 
                                "colorscale": ["Reds"], 
                                "colorbar.title.text": "% Republican",
                                "zmin": 0,
                                "zmax": 100,
                                "hovertemplate": "<b>%{customdata[0]}</b><br>Total Voters: %{customdata[1]:,.0f}<br>Republican: %{customdata[3]:.1f}%<br><extra></extra>"
                            }],
                            label="% Republican",
                            method="restyle"
                        ),
                        dict(
                            args=[{
                                "z": [merged['una_pct']], 
                                "colorscale": ["Purples"], 
                                "colorbar.title.text": "% Unaffiliated",
                                "zmin": 0,
                                "zmax": 100,
                                "hovertemplate": "<b>%{customdata[0]}</b><br>Total Voters: %{customdata[1]:,.0f}<br>Unaffiliated: %{customdata[4]:.1f}%<br><extra></extra>"
                            }],
                            label="% Unaffiliated",
                            method="restyle"
                        ),
                        dict(
                            args=[{
                                "z": [merged['lib_pct']], 
                                "colorscale": ["Yellows"], 
                                "colorbar.title.text": "% Libertarian",
                                "zmin": 0,
                                "zmax": 5,
                                "hovertemplate": "<b>%{customdata[0]}</b><br>Total Voters: %{customdata[1]:,.0f}<br>Libertarian: %{customdata[5]:.1f}%<br><extra></extra>"
                            }],
                            label="% Libertarian",
                            method="restyle"
                        ),
                        dict(
                            args=[{
                                "z": [merged['gre_pct']], 
                                "colorscale": ["Greens"], 
                                "colorbar.title.text": "% Green",
                                "zmin": 0,
                                "zmax": 2,
                                "hovertemplate": "<b>%{customdata[0]}</b><br>Total Voters: %{customdata[1]:,.0f}<br>Green: %{customdata[6]:.1f}%<br><extra></extra>"
                            }],
                            label="% Green",
                            method="restyle"
                        ),
                        dict(
                            args=[{
                                "z": [merged['cst_pct']], 
                                "colorscale": [[[0, 'rgb(240,248,255)'], [1, 'rgb(70,130,180)']]], 
                                "colorbar.title.text": "% Constitution",
                                "zmin": 0,
                                "zmax": 1,
                                "hovertemplate": "<b>%{customdata[0]}</b><br>Total Voters: %{customdata[1]:,.0f}<br>Constitution: %{customdata[7]:.1f}%<br><extra></extra>"
                            }],
                            label="% Constitution",
                            method="restyle"
                        )
                    ]),
                    direction="down",
                    pad={"r": 10, "t": 10},
                    showactive=True,
                    x=0.01,
                    xanchor="left",
                    y=0.99,
                    yanchor="top",
                    bgcolor="white",
                    bordercolor="#333",
                    borderwidth=1,
                    font=dict(size=12)
                )
            ]
        )
        
        fig.update_layout(
            title=dict(text="Partisan Affiliation by County", font=dict(size=24, color='#16003f'), x=0.5, xanchor='center'),
            mapbox=dict(style='carto-positron', center=dict(lat=35.5, lon=-79.5), zoom=6),
            height=700,
            margin=dict(l=0, r=0, t=60, b=0),
            font=dict(family='Arial, sans-serif')
        )
        
        output_path = OUTPUT_DIR / 'maps' / output_filename
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        config = {
            'displayModeBar': True,
            'scrollZoom': True,
            'modeBarButtonsToRemove': ['select2d', 'lasso2d', 'toImage', 'zoom2d', 'pan2d', 'zoomIn2d', 'zoomOut2d', 'autoScale2d', 'resetScale2d'],
            'displaylogo': False
        }
        
        fig.write_html(str(output_path), config=config)
        logger.info(f"Partisan Affiliation map created: {output_path}")
        return output_path
        
    except Exception as e:
        logger.error(f"Failed to create Partisan Affiliation map: {e}", exc_info=True)
        return None

def create_race_map(output_filename='interactive_map_race.html'):
    """Create Race map with dropdown to switch between all racial categories."""
    try:
        logger.info("Creating Race map")
        
        gdf = load_county_geometries()
        engine = get_engine()
        data_df = get_county_data_by_layer(engine, 'race')
        
        if data_df.empty:
            logger.warning("No data found for race demographics")
            return None
        
        merged = prepare_map_data(gdf, data_df, 'white_pct', 'county')
        merged['white_pct'] = merged['white_pct'].fillna(0)
        merged['black_pct'] = merged['black_pct'].fillna(0)
        merged['asian_pct'] = merged['asian_pct'].fillna(0)
        merged['native_pct'] = merged['native_pct'].fillna(0)
        merged['multi_pct'] = merged['multi_pct'].fillna(0)
        merged['other_pct'] = merged['other_pct'].fillna(0)
        merged['pacific_pct'] = merged['pacific_pct'].fillna(0)
        merged['undesig_pct'] = merged['undesig_pct'].fillna(0)
        
        geojson = json.loads(merged.to_json())
        customdata = list(zip(
            merged['County'], merged['total'],
            merged['white_pct'], merged['black_pct'], merged['asian_pct'],
            merged['native_pct'], merged['multi_pct'], merged['other_pct'],
            merged['pacific_pct'], merged['undesig_pct']
        ))
        
        # Create figure with White data as default
        fig = go.Figure(go.Choroplethmapbox(
            geojson=geojson,
            locations=merged.index,
            z=merged['white_pct'],
            customdata=customdata,
            colorscale='Viridis',
            hovertemplate=(
                "<b>%{customdata[0]}</b><br>"
                "Total Voters: %{customdata[1]:,.0f}<br>"
                "White: %{customdata[2]:.1f}%<br>"
                "<extra></extra>"
            ),
            marker_opacity=0.7,
            marker_line_width=1,
            marker_line_color='white',
            showscale=True,
            colorbar=dict(title=dict(text='% White'), thickness=15, len=0.7),
            zmin=0,
            zmax=100
        ))
        
        # Add dropdown menu to switch between racial categories
        fig.update_layout(
            updatemenus=[
                dict(
                    buttons=list([
                        dict(
                            args=[{
                                "z": [merged['white_pct']], 
                                "colorbar.title.text": "% White",
                                "zmin": 0,
                                "zmax": 100,
                                "hovertemplate": "<b>%{customdata[0]}</b><br>Total Voters: %{customdata[1]:,.0f}<br>White: %{customdata[2]:.1f}%<br><extra></extra>"
                            }],
                            label="% White",
                            method="restyle"
                        ),
                        dict(
                            args=[{
                                "z": [merged['black_pct']], 
                                "colorbar.title.text": "% Black",
                                "zmin": 0,
                                "zmax": 100,
                                "hovertemplate": "<b>%{customdata[0]}</b><br>Total Voters: %{customdata[1]:,.0f}<br>Black: %{customdata[3]:.1f}%<br><extra></extra>"
                            }],
                            label="% Black",
                            method="restyle"
                        ),
                        dict(
                            args=[{
                                "z": [merged['asian_pct']], 
                                "colorbar.title.text": "% Asian",
                                "zmin": 0,
                                "zmax": 10,
                                "hovertemplate": "<b>%{customdata[0]}</b><br>Total Voters: %{customdata[1]:,.0f}<br>Asian: %{customdata[4]:.1f}%<br><extra></extra>"
                            }],
                            label="% Asian",
                            method="restyle"
                        ),
                        dict(
                            args=[{
                                "z": [merged['native_pct']], 
                                "colorbar.title.text": "% Native American",
                                "zmin": 0,
                                "zmax": 5,
                                "hovertemplate": "<b>%{customdata[0]}</b><br>Total Voters: %{customdata[1]:,.0f}<br>Native American: %{customdata[5]:.1f}%<br><extra></extra>"
                            }],
                            label="% Native American",
                            method="restyle"
                        ),
                        dict(
                            args=[{
                                "z": [merged['pacific_pct']], 
                                "colorbar.title.text": "% Pacific Islander",
                                "zmin": 0,
                                "zmax": 1,
                                "hovertemplate": "<b>%{customdata[0]}</b><br>Total Voters: %{customdata[1]:,.0f}<br>Pacific Islander: %{customdata[8]:.1f}%<br><extra></extra>"
                            }],
                            label="% Pacific Islander",
                            method="restyle"
                        ),
                        dict(
                            args=[{
                                "z": [merged['multi_pct']], 
                                "colorbar.title.text": "% Multiracial",
                                "zmin": 0,
                                "zmax": 10,
                                "hovertemplate": "<b>%{customdata[0]}</b><br>Total Voters: %{customdata[1]:,.0f}<br>Multiracial: %{customdata[6]:.1f}%<br><extra></extra>"
                            }],
                            label="% Multiracial",
                            method="restyle"
                        ),
                        dict(
                            args=[{
                                "z": [merged['other_pct']], 
                                "colorbar.title.text": "% Other",
                                "zmin": 0,
                                "zmax": 5,
                                "hovertemplate": "<b>%{customdata[0]}</b><br>Total Voters: %{customdata[1]:,.0f}<br>Other: %{customdata[7]:.1f}%<br><extra></extra>"
                            }],
                            label="% Other",
                            method="restyle"
                        ),
                        dict(
                            args=[{
                                "z": [merged['undesig_pct']], 
                                "colorbar.title.text": "% Undesignated",
                                "zmin": 0,
                                "zmax": 5,
                                "hovertemplate": "<b>%{customdata[0]}</b><br>Total Voters: %{customdata[1]:,.0f}<br>Undesignated: %{customdata[9]:.1f}%<br><extra></extra>"
                            }],
                            label="% Undesignated",
                            method="restyle"
                        )
                    ]),
                    direction="down",
                    pad={"r": 10, "t": 10},
                    showactive=True,
                    x=0.01,
                    xanchor="left",
                    y=0.99,
                    yanchor="top",
                    bgcolor="white",
                    bordercolor="#333",
                    borderwidth=1,
                    font=dict(size=12)
                )
            ]
        )
        
        fig.update_layout(
            title=dict(text="Race Demographics by County", font=dict(size=24, color='#16003f'), x=0.5, xanchor='center'),
            mapbox=dict(style='carto-positron', center=dict(lat=35.5, lon=-79.5), zoom=6),
            height=700,
            margin=dict(l=0, r=0, t=60, b=0),
            font=dict(family='Arial, sans-serif')
        )
        
        output_path = OUTPUT_DIR / 'maps' / output_filename
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        config = {
            'displayModeBar': True,
            'scrollZoom': True,
            'modeBarButtonsToRemove': ['select2d', 'lasso2d', 'toImage', 'zoom2d', 'pan2d', 'zoomIn2d', 'zoomOut2d', 'autoScale2d', 'resetScale2d'],
            'displaylogo': False
        }
        
        fig.write_html(str(output_path), config=config)
        logger.info(f"Race map created: {output_path}")
        return output_path
        
    except Exception as e:
        logger.error(f"Failed to create Race map: {e}", exc_info=True)
        return None

def create_gender_map(output_filename='interactive_map_gender.html'):
    """Create Gender map with dropdown to switch between male/female/undesignated."""
    try:
        logger.info("Creating Gender map")
        
        gdf = load_county_geometries()
        engine = get_engine()
        data_df = get_county_data_by_layer(engine, 'gender')
        
        if data_df.empty:
            logger.warning("No data found for gender distribution")
            return None
        
        merged = prepare_map_data(gdf, data_df, 'female_pct', 'county')
        merged['female_pct'] = merged['female_pct'].fillna(0)
        merged['male_pct'] = merged['male_pct'].fillna(0)
        merged['undesig_pct'] = merged['undesig_pct'].fillna(0)
        
        geojson = json.loads(merged.to_json())
        customdata = list(zip(
            merged['County'], merged['total'],
            merged['female_pct'], merged['male_pct'], merged['undesig_pct']
        ))
        
        # Create figure with Female data as default (pink)
        fig = go.Figure(go.Choroplethmapbox(
            geojson=geojson,
            locations=merged.index,
            z=merged['female_pct'],
            customdata=customdata,
            colorscale=[[0, "rgb(255,240,245)"], [1, "rgb(255,105,180)"]],  # Light pink to hot pink
            hovertemplate=(
                "<b>%{customdata[0]}</b><br>"
                "Total Voters: %{customdata[1]:,.0f}<br>"
                "Female: %{customdata[2]:.1f}%<br>"
                "<extra></extra>"
            ),
            marker_opacity=0.7,
            marker_line_width=1,
            marker_line_color='white',
            showscale=True,
            colorbar=dict(title=dict(text='% Female'), thickness=15, len=0.7),
            zmin=45,
            zmax=55
        ))
        
        # Add dropdown menu to switch between genders
        fig.update_layout(
            updatemenus=[
                dict(
                    buttons=list([
                        dict(
                            args=[{
                                "z": [merged['female_pct']], 
                                "colorscale": [[[0, "rgb(255,240,245)"], [1, "rgb(255,105,180)"]]],
                                "colorbar.title.text": "% Female",
                                "zmin": 45,
                                "zmax": 55,
                                "hovertemplate": "<b>%{customdata[0]}</b><br>Total Voters: %{customdata[1]:,.0f}<br>Female: %{customdata[2]:.1f}%<br><extra></extra>"
                            }],
                            label="% Female",
                            method="restyle"
                        ),
                        dict(
                            args=[{
                                "z": [merged['male_pct']], 
                                "colorscale": [[[0, "rgb(224,247,255)"], [1, "rgb(0,102,204)"]]],  # Light blue to blue
                                "colorbar.title.text": "% Male",
                                "zmin": 45,
                                "zmax": 55,
                                "hovertemplate": "<b>%{customdata[0]}</b><br>Total Voters: %{customdata[1]:,.0f}<br>Male: %{customdata[3]:.1f}%<br><extra></extra>"
                            }],
                            label="% Male",
                            method="restyle"
                        ),
                        dict(
                            args=[{
                                "z": [merged['undesig_pct']], 
                                "colorscale": [[[0, "rgb(242,240,247)"], [1, "rgb(158,154,200)"]]],  # Light purple to purple
                                "colorbar.title.text": "% Undesignated",
                                "zmin": 0,
                                "zmax": 2,
                                "hovertemplate": "<b>%{customdata[0]}</b><br>Total Voters: %{customdata[1]:,.0f}<br>Undesignated: %{customdata[4]:.1f}%<br><extra></extra>"
                            }],
                            label="% Undesignated",
                            method="restyle"
                        )
                    ]),
                    direction="down",
                    pad={"r": 10, "t": 10},
                    showactive=True,
                    x=0.01,
                    xanchor="left",
                    y=0.99,
                    yanchor="top",
                    bgcolor="white",
                    bordercolor="#333",
                    borderwidth=1,
                    font=dict(size=12)
                )
            ]
        )
        
        fig.update_layout(
            title=dict(text="Gender Distribution by County", font=dict(size=24, color='#16003f'), x=0.5, xanchor='center'),
            mapbox=dict(style='carto-positron', center=dict(lat=35.5, lon=-79.5), zoom=6),
            height=700,
            margin=dict(l=0, r=0, t=60, b=0),
            font=dict(family='Arial, sans-serif')
        )
        
        output_path = OUTPUT_DIR / 'maps' / output_filename
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        config = {
            'displayModeBar': True,
            'scrollZoom': True,
            'modeBarButtonsToRemove': ['select2d', 'lasso2d', 'toImage', 'zoom2d', 'pan2d', 'zoomIn2d', 'zoomOut2d', 'autoScale2d', 'resetScale2d'],
            'displaylogo': False
        }
        
        fig.write_html(str(output_path), config=config)
        logger.info(f"Gender map created: {output_path}")
        return output_path
        
    except Exception as e:
        logger.error(f"Failed to create Gender map: {e}", exc_info=True)
        return None

def create_all_maps():
    """Generate all 4 main maps."""
    results = {
        'total': create_total_voters_map(),
        'party': create_party_map(),
        'race': create_race_map(),
        'gender': create_gender_map()
    }
    
    success_count = sum(1 for r in results.values() if r is not None)
    logger.info(f"Generated {success_count}/4 maps successfully")
    
    return results

def main():
    """Entry point for command-line execution."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate interactive voter registration maps')
    parser.add_argument('--layer', 
                       choices=['total', 'party', 'race', 'gender', 'all'],
                       default='all', 
                       help='Map layer to generate')
    
    args = parser.parse_args()
    
    if args.layer == 'all':
        create_all_maps()
    elif args.layer == 'total':
        create_total_voters_map()
    elif args.layer == 'party':
        create_party_map()
    elif args.layer == 'race':
        create_race_map()
    elif args.layer == 'gender':
        create_gender_map()
    
    logger.info("Map generation complete")

if __name__ == "__main__":
    main()