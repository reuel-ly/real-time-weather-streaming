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
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.functions import window, avg
import os
from dotenv import load_dotenv

load_dotenv() 

url = os.getenv("DATABASE_URL")

MONGO_URI = url
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

# Initialize Pyspark session with caching only once
@st.cache_resource
def get_spark():
    spark = (
        SparkSession.builder
        .appName("WeatherApp")
        .master("local[*]")
        .config(
            "spark.jars",
            r"C:\mongo-spark-connector\mongo-spark-connector_2.12-10.4.0.jar"
        )
        .getOrCreate()
    )
    return spark

spark = get_spark()

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
# Sample Data Generation (for testing without Kafka)
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

def load_spark_weather_df():
    spark = get_spark()
    
    return (
        spark.read
        .format("mongodb")
        .option("connection.uri", "mongodb://localhost:27017")
        .option("database", "weather_db")
        .option("collection", "weather_data")
        .load()
    )



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

    # SPARK QUERIES
    spark_df = load_spark_weather_df()

    # Time range filter
    now = datetime.utcnow()
    if time_range == "5m":
        start_time = now - timedelta(minutes=5)
    elif time_range == "15m":
        start_time = now - timedelta(minutes=15)
    elif time_range == "30m":
        start_time = now - timedelta(minutes=30)
    elif time_range == "1h":
        start_time = now - timedelta(hours=1)
    elif time_range == "24h":
        start_time = now - timedelta(days=1)
    elif time_range == "7d":
        start_time = now - timedelta(days=7)
    elif time_range == "30d":
        start_time = now - timedelta(days=30)
    else:
        start_time = now - timedelta(days=365)

    
    spark_df = spark_df.withColumn("timestamp", to_timestamp("timestamp"))
    spark_df = spark_df.filter(col("timestamp") >= start_time)

    # Metric filter
    if metric_type:
        spark_df = spark_df.filter(col("metric_type").isin(metric_type))

    # Convert Spark - Pandas
    pdf = spark_df.toPandas()

     #Clean MongoDB object ID column if exists
    if "_id" in pdf.columns:
        pdf = pdf.drop(columns=["_id"], errors="ignore")

    return pdf




def display_real_time_view(config, refresh_interval):
    st.header("📈 Real-time Streaming Dashboard")

    # Initialize session state for refresh and data
    if 'refresh_state' not in st.session_state:
        st.session_state.refresh_state = {'auto_refresh': True, 'last_refresh': datetime.now()}
    
    if 'real_time_data' not in st.session_state:
        st.session_state.real_time_data = pd.DataFrame()  # empty dataframe to store cumulative data

    refresh_state = st.session_state.refresh_state

    st.info(f"**Auto-refresh:** {'🟢 Enabled' if refresh_state['auto_refresh'] else '🔴 Disabled'} - Updates every {refresh_interval} seconds")

    # Fetch new Kafka data
    with st.spinner("Fetching real-time data from Kafka..."):
        new_data = consume_kafka_data(config)  # your existing Kafka consumption function

    if new_data is not None and not new_data.empty:
        # Append new data to session-state dataframe
        st.session_state.real_time_data = pd.concat(
            [st.session_state.real_time_data, new_data]
        ).drop_duplicates().reset_index(drop=True)

        # Update last refresh time
        refresh_state['last_refresh'] = datetime.now()

    real_time_data = st.session_state.real_time_data

    if not real_time_data.empty:
        # Data freshness indicator
        data_freshness = datetime.now() - refresh_state['last_refresh']
        freshness_color = "🟢" if data_freshness.total_seconds() < 10 else "🟡" if data_freshness.total_seconds() < 30 else "🔴"
        st.success(f"{freshness_color} Data updated {data_freshness.total_seconds():.0f} seconds ago")

        # Live metrics
        st.subheader("📊 Live Data Metrics")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Records Received", len(real_time_data))
        with col2:
            st.metric("Latest Value", f"{real_time_data['value'].iloc[-1]:.2f}")
        with col3:
            st.metric("Data Range", f"{real_time_data['timestamp'].min().strftime('%H:%M')} - {real_time_data['timestamp'].max().strftime('%H:%M')}")

        # Real-time chart
        st.subheader("📈 Real-time Trend")
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
            yaxis_title="Temperature",
            hovermode='x unified'
        )
        st.plotly_chart(fig, width='stretch')

        # Raw data table
        with st.expander("📋 View Raw Data"):
            st.dataframe(
                real_time_data.sort_values('timestamp', ascending=False),
                width='stretch',
                height=300
            )
    else:
        st.warning("No real-time data available.")


def display_historical_view(config):
    st.header("📊 Historical Data Analysis")
       
    # Interactive controls
    st.subheader("Data Filters")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        show_all = st.selectbox(
            "Mode",
            ["Show All Data", "Filtered View"],
            help="Changes the query mode to show all data or apply filters"
        )

    # If SHOW ALL DATA → hide the other filters and replace with disabled text
    if show_all == "Show All Data":
        with col2:
            st.write("Time Range")
            st.info("Disabled (Showing All Data)")
        with col3:
            st.write("Metric Type")
            st.info("Disabled (Showing All Data)")
        with col4:
            st.write("Aggregation")
            st.info("Disabled (Showing All Data)")

        # Set default values so your query logic doesn't break
        time_range = None
        metric_type = None
        aggregation = "raw"

    else:
        # Normal interactive controls (Enabled when Filtered View is selected)
        with col2:
            time_range = st.selectbox(
                "Time Range",
                ["5m","10m", "15m", "30m", "1h", "24h", "7d", "30d"],
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
        if aggregation != "raw":
            # Convert Pandas → Spark
            spark_df = spark.createDataFrame(historical_data)
            # Apply window aggregation
            if aggregation == "hourly":
                spark_df = spark_df.groupBy(window(col("timestamp"), "1 hour"), col("metric_type")) \
                                .avg("value") \
                                .withColumnRenamed("avg(value)", "avg_value")
            elif aggregation == "daily":
                spark_df = spark_df.groupBy(window(col("timestamp"), "1 day"), col("metric_type")) \
                                .avg("value") \
                                .withColumnRenamed("avg(value)", "avg_value")
            elif aggregation == "weekly":
                spark_df = spark_df.groupBy(window(col("timestamp"), "7 days"), col("metric_type")) \
                                .avg("value") \
                                .withColumnRenamed("avg(value)", "avg_value")
            # Convert Spark → Pandas
            pdf = spark_df.toPandas()
        else:
            pdf = historical_data

        

        # Display raw data
        st.subheader("Historical Data Table")
        
        st.dataframe(
            historical_data,
            width='stretch',
            hide_index=True
        )
        
        # Historical trends
        st.subheader("Historical Trends")
        if aggregation == "raw":
            fig = px.line(pdf, x="timestamp", y="value", color="metric_type",
                        title="Historical Values")
        else:
            fig = px.line(pdf, x="window.start", y="avg_value", color="metric_type",
                        title=f"{aggregation.capitalize()} Averaged Values")

        st.plotly_chart(fig, use_container_width=True)
            
        
        st.header("📊 Top-N Temperature (℃) Visualizer")

        if "value" in pdf.columns and "timestamp" in pdf.columns:

            # --- User Inputs ---
            metric_types = pdf["metric_type"].unique() if "metric_type" in pdf.columns else ["temperature"]

            metric_col, top_n_col, order_col = st.columns(3)

            with metric_col:
                metric_filter = st.selectbox("Select Metric Type", metric_types)

            with top_n_col:
                top_n = st.number_input(
                    "Select Top N",
                    min_value=1,
                    max_value=100,
                    value=5,
                    step=1
                )

            with order_col:
                order_type = st.selectbox("Order By", ["Highest", "Lowest"])

            # --- Filter by selected metric ---
            filtered_df = (
                pdf[pdf["metric_type"] == metric_filter]
                if "metric_type" in pdf.columns
                else pdf
            )

            if filtered_df.empty:
                st.warning(f"No data available for metric type: {metric_filter}")
            else:
                # --- Sort values ---
                if order_type == "Highest":
                    top_rows = filtered_df.nlargest(top_n, "value")
                else:
                    top_rows = filtered_df.nsmallest(top_n, "value")
                
                st.write(f"### Top {top_n} {order_type} {metric_filter} values")

                # --- Histogram  ---
                fig_topN = px.bar(
                    top_rows.sort_values("value", ascending=(order_type == "Lowest")),
                    x="timestamp",
                    y="value",
                    title=f"Top {top_n} {order_type} {metric_filter.capitalize()} Readings",
                    labels={"timestamp": "Timestamp", "value": metric_filter.capitalize()},
                    template="plotly_white"
                )
                

                fig_topN.update_layout(
                    xaxis_title="Timestamp",
                    yaxis_title=f"{metric_filter.capitalize()} Value",
                    showlegend=False
                )

                st.plotly_chart(fig_topN, use_container_width=True)

                # --- Show Table ---
                with st.expander("📋 View Records"):
                    st.dataframe(top_rows[["timestamp", "value"]])
        else:
            st.warning("Required columns ('value', 'timestamp') not found in dataset.")

        # Additional analysis
        st.subheader("Data Summary")
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Total Records", len(pdf))
            st.metric("Date Range",
                    f"{pdf['timestamp'].min().strftime('%Y-%m-%d')} to {pdf['timestamp'].max().strftime('%Y-%m-%d')}"
                    if "timestamp" in pdf.columns else "N/A")
        with col2:
            st.metric("Average Value", f"{pdf['value'].mean():.2f} ℃" if "value" in pdf.columns else "N/A")
            st.metric("Data Variability", f"{pdf['value'].std():.2f}" if "value" in pdf.columns else "N/A")

    else:
        st.error("Historical data query not implemented")

def main():
    st.title("🌞 Real-Time Weather Data Streaming")

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