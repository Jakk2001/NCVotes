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
            colorscale='blues',
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
            colorscale= 'blues',
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
                                "colorscale":  [[[0, 'rgb(240,248,255)'], [1, 'rgb(0, 60, 179)']]],
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
                                "colorscale": [[[0, 'rgb(240,248,255)'], [1, 'rgb(119, 32, 156)']]], 
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
                                "colorscale": [[[0, 'rgb(50,50,50)'], [1, 'rgb(214, 238, 5)']]], 
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



"""
Add this function to src/visualization/interactive_map.py
"""

def create_unregistered_voters_map(output_filename='interactive_map_unregistered.html'):
    """Create map showing proportion of eligible but unregistered voters."""
    try:
        logger.info("Creating Unregistered Voters map")
        
        # Data from 2025_NCMissingVoters.pdf pages 24-26
        unregistered_pct = {
            "ALAMANCE": 12.9, "ALEXANDER": 5.2, "ALLEGHANY": 2.1, "ANSON": 42.6,
            "ASHE": 1.5, "AVERY": 0.9, "BEAUFORT": 31.8, "BERTIE": 43.8,
            "BLADEN": 38.4, "BRUNSWICK": 6.4, "BUNCOMBE": 5.1, "BURKE": 7.4,
            "CABARRUS": 8.9, "CALDWELL": 7.1, "CAMDEN": 15.7, "CARTERET": 7.1,
            "CASWELL": 27.8, "CATAWBA": 10.9, "CHATHAM": 8.2, "CHEROKEE": 1.2,
            "CHOWAN": 29.0, "CLAY": 1.0, "CLEVELAND": 18.4, "COLUMBUS": 24.9,
            "CRAVEN": 20.0, "CUMBERLAND": 27.2, "CURRITUCK": 10.9, "DARE": 6.0,
            "DAVIDSON": 12.9, "DAVIE": 10.6, "DUPLIN": 33.8, "DURHAM": 15.3,
            "EDGECOMBE": 48.3, "FORSYTH": 16.7, "FRANKLIN": 22.8, "GASTON": 15.3,
            "GATES": 23.0, "GRAHAM": 1.7, "GRANVILLE": 28.5, "GREENE": 38.3,
            "GUILFORD": 17.2, "HALIFAX": 46.5, "HARNETT": 20.3, "HAYWOOD": 2.8,
            "HENDERSON": 4.2, "HERTFORD": 47.5, "HOKE": 29.8, "HYDE": 17.6,
            "IREDELL": 10.9, "JACKSON": 1.6, "JOHNSTON": 18.1, "JONES": 30.1,
            "LEE": 29.0, "LENOIR": 35.7, "LINCOLN": 9.2, "MACON": 1.1,
            "MADISON": 2.9, "MARTIN": 35.2, "MCDOWELL": 6.1, "MECKLENBURG": 20.4,
            "MITCHELL": 1.7, "MONTGOMERY": 21.3, "MOORE": 8.8, "NASH": 34.4,
            "NEW HANOVER": 11.9, "NORTHAMPTON": 54.6, "ONSLOW": 14.3, "ORANGE": 8.7,
            "PAMLICO": 12.9, "PASQUOTANK": 28.1, "PENDER": 15.6, "PERQUIMANS": 24.2,
            "PERSON": 30.5, "PITT": 38.2, "POLK": 6.3, "RANDOLPH": 9.6,
            "RICHMOND": 34.9, "ROBESON": 27.1, "ROCKINGHAM": 23.1, "ROWAN": 23.0,
            "RUTHERFORD": 12.5, "SAMPSON": 29.9, "SCOTLAND": 37.1, "STANLY": 15.2,
            "STOKES": 5.3, "SURRY": 5.5, "SWAIN": 1.2, "TRANSYLVANIA": 4.7,
            "TYRRELL": 33.7, "UNION": 15.3, "VANCE": 55.2, "WAKE": 23.5,
            "WARREN": 49.7, "WASHINGTON": 54.6, "WATAUGA": 3.1, "WAYNE": 37.5,
            "WILKES": 6.6, "WILSON": 46.6, "YADKIN": 5.0, "YANCEY": 0.6
        }
        
        # County population data (2025)
        population_data = {
            "WAKE": 1261494, "MECKLENBURG": 1236342, "GUILFORD": 564752,
            "FORSYTH": 402451, "DURHAM": 350018, "CUMBERLAND": 338449,
            "BUNCOMBE": 281631, "UNION": 269262, "JOHNSTON": 257230,
            "CABARRUS": 249242, "NEW HANOVER": 246612, "GASTON": 245867,
            "ONSLOW": 214724, "IREDELL": 212411, "ALAMANCE": 186463,
            "PITT": 182610, "DAVIDSON": 180731, "BRUNSWICK": 174369,
            "CATAWBA": 169153, "ROWAN": 154885, "ORANGE": 153515,
            "HARNETT": 150400, "RANDOLPH": 149137, "HENDERSON": 121966,
            "WAYNE": 121106, "ROBESON": 119543, "MOORE": 109762,
            "CRAVEN": 105005, "CLEVELAND": 102827, "LINCOLN": 99436,
            "NASH": 99184, "ROCKINGHAM": 94396, "BURKE": 88943,
            "CHATHAM": 85756, "FRANKLIN": 82450, "CALDWELL": 80899,
            "WILSON": 80310, "PENDER": 71798, "SURRY": 71637,
            "CARTERET": 70806, "LEE": 69653, "STANLY": 68834,
            "WILKES": 66355, "RUTHERFORD": 65805, "HAYWOOD": 63174,
            "GRANVILLE": 61726, "SAMPSON": 61066, "HOKE": 56177,
            "LENOIR": 55697, "WATAUGA": 54574, "DUPLIN": 51259,
            "COLUMBUS": 49968, "EDGECOMBE": 49302, "HALIFAX": 46916,
            "STOKES": 46228, "DAVIE": 46172, "MCDOWELL": 45570,
            "JACKSON": 45512, "BEAUFORT": 44742, "VANCE": 42345,
            "RICHMOND": 41977, "PASQUOTANK": 41656, "PERSON": 40454,
            "MACON": 39004, "YADKIN": 38192, "DARE": 38185,
            "ALEXANDER": 36959, "TRANSYLVANIA": 34272, "SCOTLAND": 34136,
            "CURRITUCK": 32947, "CHEROKEE": 30794, "BLADEN": 30049,
            "ASHE": 27464, "MONTGOMERY": 26487, "MADISON": 22665,
            "ANSON": 22483, "CASWELL": 22247, "MARTIN": 21562,
            "GREENE": 20698, "POLK": 20610, "WARREN": 19400,
            "HERTFORD": 19104, "YANCEY": 19046, "AVERY": 17911,
            "BERTIE": 16865, "NORTHAMPTON": 16455, "MITCHELL": 15057,
            "SWAIN": 13967, "CHOWAN": 13888, "PERQUIMANS": 13553,
            "PAMLICO": 12650, "CLAY": 12211, "ALLEGHANY": 11418,
            "CAMDEN": 11236, "WASHINGTON": 10569, "GATES": 10244,
            "JONES": 9476, "GRAHAM": 8279, "HYDE": 4528, "TYRRELL": 3514
        }
        
        gdf = load_county_geometries()
        engine = get_engine()
        
        # Get registered voters by county
        registered_df = get_county_data_by_layer(engine, 'total')
        
        if registered_df.empty:
            logger.warning("No data found for registered voters")
            return None
        
        # Create dataframe with all calculations
        data_rows = []
        for county_name in unregistered_pct.keys():
            county_upper = county_name.upper()
            unreg_pct = unregistered_pct[county_upper]
            population = population_data.get(county_upper, 0)
            
            # Find registered voters for this county
            registered = 0
            for _, row in registered_df.iterrows():
                if row['county'].strip().upper() == county_upper:
                    registered = row['registered']
                    break
            
            # Calculate eligible voters (registered / (1 - unreg_pct/100))
            if unreg_pct < 100:
                eligible = registered / (1 - unreg_pct / 100)
            else:
                eligible = registered
            
            unregistered = eligible - registered
            
            data_rows.append({
                'county': county_name.lower(),
                'population': population,
                'registered': registered,
                'eligible': int(eligible),
                'unregistered': int(unregistered),
                'unreg_pct': unreg_pct
            })
        
        data_df = pd.DataFrame(data_rows)
        
        # Merge with geometry
        merged = prepare_map_data(gdf, data_df, 'unreg_pct', 'county')
        
        geojson = json.loads(merged.to_json())
        customdata = list(zip(
            merged['County'],
            merged['population'],
            merged['registered'],
            merged['eligible'],
            merged['unregistered'],
            merged['unreg_pct']
        ))
        
        # Create figure with red color scale (higher = more unregistered = worse)
        fig = go.Figure(go.Choroplethmapbox(
            geojson=geojson,
            locations=merged.index,
            z=merged['unreg_pct'],
            customdata=customdata,
            colorscale=[[0, "rgb(255,245,240)"], [0.2, "rgb(254,224,210)"], 
                       [0.4, "rgb(252,187,161)"], [0.6, "rgb(252,146,114)"],
                       [0.8, "rgb(251,106,74)"], [1, "rgb(203,24,29)"]],
            hovertemplate=(
                "<b>%{customdata[0]}</b><br>"
                "Population: %{customdata[1]:,.0f}<br>"
                "Registered: %{customdata[2]:,.0f}<br>"
                "Eligible: %{customdata[3]:,.0f}<br>"
                "Unregistered: %{customdata[4]:,.0f}<br>"
                "Unregistered: %{customdata[5]:.1f}%<br>"
                "<extra></extra>"
            ),
            marker_opacity=0.7,
            marker_line_width=1,
            marker_line_color='white',
            showscale=True,
            colorbar=dict(title=dict(text='% Unregistered'), thickness=15, len=0.7),
            zmin=0,
            zmax=60
        ))
        
        fig.update_layout(
            title=dict(
                text="Eligible but Unregistered Voters by County",
                font=dict(size=24, color='#16003f'),
                x=0.5,
                xanchor='center'
            ),
            mapbox=dict(
                style='carto-positron',
                center=dict(lat=35.5, lon=-79.5),
                zoom=6
            ),
            height=700,
            margin=dict(l=0, r=0, t=60, b=0),
            font=dict(family='Arial, sans-serif')
        )
        
        output_path = OUTPUT_DIR / 'maps' / output_filename
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        config = {
            'displayModeBar': True,
            'scrollZoom': True,
            'modeBarButtonsToRemove': ['select2d', 'lasso2d', 'toImage', 
                                      'zoom2d', 'pan2d', 'zoomIn2d', 
                                      'zoomOut2d', 'autoScale2d', 'resetScale2d'],
            'displaylogo': False
        }
        
        fig.write_html(str(output_path), config=config)
        logger.info(f"Unregistered Voters map created: {output_path}")
        return output_path
        
    except Exception as e:
        logger.error(f"Failed to create Unregistered Voters map: {e}", exc_info=True)
        return None



def create_all_maps():
    """Generate all 4 main maps."""
    results = {
        'total': create_total_voters_map(),
        'party': create_party_map(),
        'race': create_race_map(),
        'gender': create_gender_map(),
        'unregistered': create_unregistered_voters_map()
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
    elif args.layer == 'unregistered':
        create_unregistered_voters_map()
    
    logger.info("Map generation complete")

if __name__ == "__main__":
    main()