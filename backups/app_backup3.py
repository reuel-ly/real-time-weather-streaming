"""
Streaming Data Dashboard Template
STUDENT PROJECT: Big Data Streaming Dashboard

This is a template for students to build a real-time streaming data dashboard.
Students will need to implement the actual data processing, Kafka consumption,
and storage integration.

IMPLEMENT THE TODO SECTIONS
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import time
import json
import io
from datetime import datetime, timedelta
from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable
from streamlit_autorefresh import st_autorefresh
from fastavro import reader, parse_schema
from pymongo import MongoClient
import certifi
import pymongo
import pandas as pd
from datetime import datetime, timedelta

MONGO_URI = "mongodb+srv://qrcmpornobe_db_user:reuel@groceryinventorysystem.gpheuwl.mongodb.net/?appName=GroceryInventorySystem"
# Initialize MongoDB client with caching only once to prevent dns srv timeouts
@st.cache_resource
def get_mongo_client():    
    return MongoClient(MONGO_URI, tlsCAFile=certifi.where())

# MongoDB configuration
MONGO_DB = "weather_db"
MONGO_COLLECTION = "weather"

mongo_client = get_mongo_client()
mongo_db = mongo_client[MONGO_DB]
mongo_collection = mongo_db[MONGO_COLLECTION]


# Page configuration
st.set_page_config(
    page_title="Streaming Data Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)


WEATHER_SCHEMA = {
    "type": "record",
    "name": "WeatherAPI",
    "namespace": "com.bigdata.streaming",
    "fields": [
        {"name": "timestamp", "type": "string"},
        {"name": "value", "type": "double"},
        {"name": "metric_type", "type": "string"},
        {"name": "sensor_id", "type": "string"},
        {"name": "location", "type": "string"},
        {"name": "unit", "type": "string"}
    ]
}

parsed_schema = parse_schema(WEATHER_SCHEMA)

def setup_sidebar():
    st.sidebar.title("Dashboard Controls")
    
    # STUDENT TODO: Add configuration options for data sources
    st.sidebar.subheader("Data Source Configuration")
    
    # Placeholder for Kafka configuration
    kafka_broker = st.sidebar.text_input(
        "Kafka Broker", 
        value="localhost:9092",
        help="Configure your Kafka broker address"
    )
    
    kafka_topic = st.sidebar.text_input(
        "Kafka Topic", 
        value="streaming-data",
        help="Specify the Kafka topic to consume from"
    )
    
    return {
        "kafka_broker": kafka_broker,
        "kafka_topic": kafka_topic,
    }

# ──────────────────────────────────────────────
# 4️⃣ Sample Data Generation (for testing without Kafka)
def generate_sample_data():
    now = datetime.now()
    times = [now - timedelta(seconds=i*10) for i in range(50)]
    data = pd.DataFrame({
        "timestamp": times,
        "value": [100 + i * 0.2 for i in range(50)],
        "metric_type": ["temperature"] * 50,
        "sensor_id": ["sensor_1"] * 50,
        "location": ["lab_room"] * 50,
        "unit": ["C"] * 50
    })
    return data
# ──────────────────────────────────────────────


# Kafka Consumer using Avro
def consume_kafka_data(config):
    kafka_broker = config["kafka_broker"]
    kafka_topic = config["kafka_topic"]

    try:
        consumer = KafkaConsumer(
            kafka_topic,
            bootstrap_servers=[kafka_broker],
            auto_offset_reset="latest",
            enable_auto_commit=True,
            group_id="streamlit-dashboard",
            consumer_timeout_ms=5000
        )
    except NoBrokersAvailable:
        st.error("No Kafka brokers available. Using sample data.")
        return generate_sample_data()

    records = []
    try:
        start = time.time()
        while time.time() - start < 5:  # read for 5 seconds
            msg_pack = consumer.poll(timeout_ms=1000)
            for _, messages in msg_pack.items():
                for message in messages:
                    try:
                        bytes_reader = io.BytesIO(message.value)
                        avro_records = [r for r in reader(bytes_reader, parsed_schema)]
                        for r in avro_records:
                            timestamp_str = r["timestamp"]
                            if timestamp_str.endswith("Z"):
                                timestamp_str = timestamp_str[:-1] + "+00:00"
                            timestamp = datetime.fromisoformat(timestamp_str)
                            records.append({
                                "timestamp": timestamp,
                                "value": float(r["value"]),
                                "metric_type": r["metric_type"],
                                "sensor_id": r["sensor_id"],
                                "location": r["location"],
                                "unit": r["unit"]
                            })
                    except Exception as e:
                        st.warning(f"Error decoding Avro message: {e}")
    except KafkaError as e:
        st.error(f"Kafka error: {e}")
        return generate_sample_data()

    if not records:
        st.info("No Avro messages received from Kafka. Showing sample data.")
        return generate_sample_data()

    df = pd.DataFrame(records)
    df.sort_values("timestamp", inplace=True)
    return df

def query_historical_data(time_range, show_all, metric_type=None, mongo_collection=mongo_collection):
    """
    STUDENT TODO: Implement actual historical data query
    
    This function should:
    1. Connect to HDFS/MongoDB
    2. Query historical data based on time range and selected metrics
    3. Return aggregated historical data
    
    Parameters:
    - time_range: time period to query (e.g., "1h", "24h", "7d")
    - metrics: list of metric types to include
    
    Expected return format:
    pandas DataFrame with historical data
    """
 
    # SHOW ALL DATA MODE
    if show_all == "Show All Data":
        results = list(mongo_collection.find({}))
        if not results:
            st.warning("No historical data found in MongoDB.")
            return pd.DataFrame()

        df = pd.DataFrame(results)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.drop(columns=["_id"], errors="ignore")
        return df

    # Time range filter
    now = datetime.utcnow()
    if time_range == "1h":
        start_time = now - timedelta(hours=1)
    elif time_range == "24h":
        start_time = now - timedelta(days=1)
    elif time_range == "7d":
        start_time = now - timedelta(days=7)
    elif time_range == "30d":
        start_time = now - timedelta(days=30)
    else:
        start_time = now - timedelta(days=365)

    query = {"timestamp": {"$gte": start_time}}

    # Metric filter
    if metric_type:
        query["metric_type"] = {"$in": metric_type}

    # Fetch from MongoDB
    results = list(mongo_collection.find(query))

    if not results:
        return pd.DataFrame()

    df = pd.DataFrame(results)

    # Convert timestamp
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # Remove MongoDB internal ID
    df = df.drop(columns=["_id"], errors="ignore")

    return df


def display_real_time_view(config, refresh_interval):
    """
    Page 1: Real-time Streaming View
    STUDENT TODO: Implement real-time data visualization from Kafka
    """
    st.header("📈 Real-time Streaming Dashboard")
    
    # Refresh status
    refresh_state = st.session_state.refresh_state
    st.info(f"**Auto-refresh:** {'🟢 Enabled' if refresh_state['auto_refresh'] else '🔴 Disabled'} - Updates every {refresh_interval} seconds")
    
    # Loading indicator for data consumption
    with st.spinner("Fetching real-time data from Kafka..."):
        real_time_data = consume_kafka_data(config)
    
    if real_time_data is not None:
        # Data freshness indicator
        data_freshness = datetime.now() - refresh_state['last_refresh']
        freshness_color = "🟢" if data_freshness.total_seconds() < 10 else "🟡" if data_freshness.total_seconds() < 30 else "🔴"
        
        st.success(f"{freshness_color} Data updated {data_freshness.total_seconds():.0f} seconds ago")
        
        # Real-time data metrics
        st.subheader("📊 Live Data Metrics")
        if not real_time_data.empty:
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Records Received", len(real_time_data))
            with col2:
                st.metric("Latest Value", f"{real_time_data['value'].iloc[-1]:.2f}")
            with col3:
                st.metric("Data Range", f"{real_time_data['timestamp'].min().strftime('%H:%M')} - {real_time_data['timestamp'].max().strftime('%H:%M')}")
        
        # Real-time chart
        st.subheader("📈 Real-time Trend")
        
        if not real_time_data.empty:
            # STUDENT TODO: Customize this chart for your specific data
            fig = px.line(
                real_time_data,
                x='timestamp',
                y='value',
                title=f"Real-time Data Stream (Last {len(real_time_data)} records)",
                labels={'value': 'Temperature', 'timestamp': 'Time'},
                template='plotly_white'
            )
            fig.update_layout(
                xaxis_title="Time",
                yaxis_title="Value",
                hovermode='x unified'
            )
            st.plotly_chart(fig, width='stretch')
            
            # Raw data table with auto-refresh
            with st.expander("📋 View Raw Data"):
                st.dataframe(
                    real_time_data.sort_values('timestamp', ascending=False),
                    width='stretch',
                    height=300
                )
        else:
            st.warning("No real-time data available.")
    
    else:
        st.error("Kafka data consumption not implemented")

def display_historical_view(config):
    st.header("📊 Historical Data Analysis")
    
    with st.expander("ℹ️ Implementation Guide"):
        st.info("""
        **STUDENT TODO:** This page should display historical data queried from HDFS or MongoDB.
        Implement the following:
        - Connection to your chosen storage system (HDFS/MongoDB)
        - Interactive filters and selectors for data exploration
        - Data aggregation and analysis capabilities
        - Historical trend visualization
        """)
    
    # Interactive controls
    st.subheader("Data Filters")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        show_all = st.selectbox(
            "Mode",
            ["Show All Data", "Filtered View"],
            help="Changes the query mode to show all data or apply filters"
        )

    with col2:
        time_range = st.selectbox(
            "Time Range",
            ["1h", "24h", "7d", "30d"],
            help="Implement time-based filtering in your query"
        )
    
    with col3:
        metric_type = st.selectbox(
            "Metric Type",
            ["temperature", "humidity", "pressure", "all"],
            help="Implement metric filtering in your query"
        )
    
    with col4:
        aggregation = st.selectbox(
            "Aggregation",
            ["raw", "hourly", "daily", "weekly"],
            help="Implement data aggregation in your query"
        )   

    historical_data = query_historical_data(time_range, show_all, [metric_type] if metric_type != "all" else None)
    
    if historical_data is not None:
        # Display raw data
        st.subheader("Historical Data Table")
        st.info("STUDENT TODO: Customize data display for your specific dataset")
        
        st.dataframe(
            historical_data,
            width='stretch',
            hide_index=True
        )
        
        # Historical trends
        st.subheader("Historical Trends")
        
        if not historical_data.empty:
            fig = px.line(
                historical_data,
                x='timestamp',
                y='Temperature',
                title="Weather Trend in Manila, Philippines",
                labels={'value': 'Temperature', 'timestamp': 'Time'},
            )
            st.plotly_chart(fig, width='stretch')
            
            # Additional analysis
            st.subheader("Data Summary")
            col1, col2 = st.columns(2)
            
            with col1:
                st.metric("Total Records", len(historical_data))
                st.metric("Date Range", f"{historical_data['timestamp'].min().strftime('%Y-%m-%d')} to {historical_data['timestamp'].max().strftime('%Y-%m-%d')}")
            
            with col2:
                st.metric("Average Temperature", f"{historical_data['value'].mean():.2f}")
                st.metric("Data Variability", f"{historical_data['value'].std():.2f}")
    
    else:
        st.error("Historical data query not implemented")

def main():
    st.title("🚀 Streaming Weather Data")
    
    with st.expander("📋 Project Instructions"):
        st.markdown("""
        **STUDENT PROJECT TEMPLATE**
        
        ### Implementation Required:
        - **Real-time Data**: Connect to Kafka and process streaming data👍
        - **Historical Data**: Query from HDFS/MongoDB👍
        - **Visualizations**: Create meaningful charts
        - **Error Handling**: Implement robust error handling👍
        """)
    
    # Initialize session state for refresh management
    if 'refresh_state' not in st.session_state:
        st.session_state.refresh_state = {
            'last_refresh': datetime.now(),
            'auto_refresh': True
        }
    
    # Setup configuration
    config = setup_sidebar()
    
    # Refresh controls in sidebar
    st.sidebar.subheader("Refresh Settings")
    st.session_state.refresh_state['auto_refresh'] = st.sidebar.checkbox(
        "Enable Auto Refresh",
        value=st.session_state.refresh_state['auto_refresh'],
        help="Automatically refresh real-time data"
    )
    
    if st.session_state.refresh_state['auto_refresh']:
        refresh_interval = st.sidebar.slider(
            "Refresh Interval (seconds)",
            min_value=5,
            max_value=60,
            value=30,
            help="Set how often real-time data refreshes"
        )
        
        # Auto-refresh using streamlit-autorefresh package
        st_autorefresh(interval=refresh_interval * 1000, key="auto_refresh")
    
    # Manual refresh button
    if st.sidebar.button("🔄 Manual Refresh"):
        st.session_state.refresh_state['last_refresh'] = datetime.now()
        st.rerun()
    
    # Display refresh status
    st.sidebar.markdown("---")
    st.sidebar.metric("Last Refresh", st.session_state.refresh_state['last_refresh'].strftime("%H:%M:%S"))
    
    # Create tabs for different views
    tab1, tab2 = st.tabs(["📈 Real-time Streaming", "📊 Historical Data"])
    
    with tab1:
        display_real_time_view(config, refresh_interval)
    
    with tab2:
        display_historical_view(config)
    

if __name__ == "__main__":
    main()