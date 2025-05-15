import streamlit as st
import streamlit.components.v1 as components
import folium
import geopandas as gpd
import pandas as pd
from datetime import datetime
from shapely.geometry import Point
from folium.plugins import HeatMap
from streamlit_folium import folium_static
import os

# ---------- Load shapefile ----------
@st.cache_data
def load_shapefile():
    shapefile_path = r"tha_adm_rtsd_itos_20210121_shp\tha_admbnda_adm1_rtsd_20220121.shp"
    gdf = gpd.read_file(shapefile_path)
    gdf = gdf.drop(columns=gdf.select_dtypes(include=['datetime64']).columns)
    gdf = gdf.to_crs(epsg=4326)
    return gdf

# ---------- Load all parquet files ----------
@st.cache_data
def load_parquet_data_combined():
    dfs = []
    root_dir = r'df_thai'
    for root, _, files in os.walk(root_dir):
        for file in files:
            if file.endswith('.parquet'):
                df = pd.read_parquet(os.path.join(root, file))
                dfs.append(df)
    combined_df = pd.concat(dfs, ignore_index=True)
    combined_df['acq_date'] = pd.to_datetime(combined_df['acq_date']).dt.date
    return combined_df

# ---------- cache filtered result ----------
@st.cache_data
def filter_by_date(df, mode, date_start, date_end, date_exact):
    if mode == 'range':
        return df[(df['acq_date'] >= date_start) & (df['acq_date'] <= date_end)]
    else:
        return df[df['acq_date'] == date_exact]

# ---------- Generate Heatmap ----------
def generate_heatmap(filter_mode, date_start, date_end, date_exact, gdf, df_all):
    # Filter
    # if filter_mode == 'range':
    #     df_filtered = df_all[(df_all['acq_date'] >= date_start) & (df_all['acq_date'] <= date_end)]
    # else:
    #     df_filtered = df_all[df_all['acq_date'] == date_exact]
    
    df_filtered = filter_by_date(df_all, filter_mode, date_start, date_end, date_exact)


    # Prepare heat data
    heat_data = []
    if 'latitude' in df_filtered.columns and 'longitude' in df_filtered.columns and not df_filtered.empty:
        # for _, row in df_filtered.iterrows():
        #     lat = row['latitude']
        #     lon = row['longitude']
        #     brightness = row.get('brightness', 1)
        #     heat_data.append([lat, lon, brightness])


        df_filtered['brightness'] = df_filtered.get('brightness', 1)
        heat_data = df_filtered[['latitude', 'longitude', 'brightness']].values.tolist()


    # Folium Map Init
    mymap = folium.Map(location=[13.7367, 100.5231], zoom_start=6)

    # GeoDataFrame of fire points
    heat_points = gpd.GeoDataFrame(
        geometry=[Point(lon, lat) for lat, lon, _ in heat_data],
        crs="EPSG:4326"
    )

    # Spatial Join to provinces
    joined = gpd.sjoin(heat_points, gdf, how="left", predicate="within")
    province_counts = joined['ADM1_TH'].value_counts().reset_index()
    province_counts.columns = ['ADM1_TH', 'heat_spot_count']

    # Merge to shapefile
    gdf = gdf.merge(province_counts, on='ADM1_TH', how='left')
    gdf['heat_spot_count'] = gdf['heat_spot_count'].fillna(0).astype(int)

    # Draw Province Layer
    folium.GeoJson(
        gdf,
        name='Provinces',
        tooltip=folium.GeoJsonTooltip(fields=['ADM1_TH', 'heat_spot_count'], aliases=['Province', 'Heat Spots']),
        style_function=lambda x: {
            'fillColor': '#e6bead',
            'color': 'black',
            'weight': 0.5,
            'fillOpacity': 0.1
        }
    ).add_to(mymap)

    # Normalize heat
    min_b, max_b = 250, 400
    normalized = []
    for lat, lon, b in heat_data:
        b = max(min_b, min(b, max_b))
        weight = (b - min_b) / (max_b - min_b)
        normalized.append([lat, lon, weight])

    # HeatMap Layer
    if normalized:
        HeatMap(
            normalized,
            radius=10,
            blur=15,
            max_zoom=7,
            gradient={
                "0.2": "#FFA500",
                "0.5": "#FF4500",
                "0.8": "#FF0000",
                "1.0": "#8B0000"
            }
        ).add_to(mymap)

    return mymap, province_counts.sort_values("heat_spot_count", ascending=False)

# ---------- Streamlit UI ----------
def main():
    st.set_page_config(layout="wide")

    # Load once
    if 'all_data' not in st.session_state:
        st.session_state.all_data = load_parquet_data_combined()
    if 'gdf' not in st.session_state:
        st.session_state.gdf = load_shapefile()

    df_all = st.session_state.all_data
    gdf = st.session_state.gdf

    # Sidebar
    st.sidebar.title("🔍 Filter Options")
    filter_mode = st.sidebar.radio("Filter Mode", ['exact', 'range'])

    if filter_mode == 'range':
        start_date = st.sidebar.date_input("Start Date", datetime(2025, 4, 1))
        end_date = st.sidebar.date_input("End Date", datetime(2025, 4, 10))
        exact_date = None
    else:
        exact_date = st.sidebar.date_input("Exact Date", datetime(2025, 4, 10))
        start_date = end_date = None

    # Heatmap
    mymap, province_counts = generate_heatmap(filter_mode, start_date, end_date, exact_date, gdf, df_all)
    map_html = mymap.get_root().render()

    # Table Overlay
    table_html = """
    <div style="position: relative; width: 100%; height: 90vh;">
        {map_html}
        <div style="position: absolute; top: 20px; right: 20px; background-color: rgba(255,255,255,0.95); padding: 15px; border-radius: 10px; z-index:9999; width: 300px; max-height: 80vh; overflow-y: auto; font-family: Arial;">
            <h4 style="margin-top:0;">🔥 จุดความร้อนรายจังหวัด</h4>
            <table style="width: 100%; border-collapse: collapse;">
                <thead><tr><th style="text-align:left;">จังหวัด</th><th>จำนวน</th></tr></thead>
                <tbody>
    """.format(map_html=map_html)

    for _, row in province_counts.iterrows():
        table_html += f"<tr><td>{row['ADM1_TH']}</td><td>{row['heat_spot_count']}</td></tr>"

    table_html += """
                </tbody>
            </table>
        </div>
    </div>
    """

    # Display on page
    components.html(table_html, height=800, scrolling=False)

if __name__ == "__main__":
    main()