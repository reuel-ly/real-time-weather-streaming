# Streaming Data Dashboard - Student Project Template

## 📊 Project Overview

This project is a comprehensive big data streaming dashboard that demonstrates real-time data processing and visualization using modern data engineering technologies. Students will build a two-tier data pipeline with real-time streaming capabilities and historical data analysis.

### Architecture Overview

The project features a **dual-pipeline architecture** with two storage options:

1. **Real-time Pipeline**: Kafka → Streamlit Dashboard (Live View)
2. **Historical Pipeline**: Kafka → Storage System (HDFS/MongoDB) → Streamlit Dashboard (Historical View)

### Two-Page Dashboard Structure

- **📈 Real-time Streaming View**: Live data consumption from Kafka with dynamic visualizations
- **📊 Historical Data View**: Query and analyze historical data from your chosen storage system

## 🎯 Technical Requirements

### Mandatory Components

- [ ] **Real-time Data Processing**: Kafka consumer implementation
- [ ] **Historical Data Storage**: HDFS or MongoDB integration
- [ ] **Interactive Dashboard**: Two-page Streamlit application
- [ ] **Data Visualization**: Real-time and historical charts
- [ ] **Error Handling**: Robust error handling for data pipelines

### Interactive Widget Requirements (Historical View)

- [ ] Time range selector (1h, 24h, 7d, 30d)
- [ ] Metric type filter
- [ ] Data aggregation options (raw, hourly, daily, weekly)
- [ ] Custom visualization controls

## 🚀 Setup and Installation

### Prerequisites

- Python 3.8+
- Apache Kafka (local installation or cloud service)
- Storage System: HDFS **OR** MongoDB
- Git for version control

### Step 1: Install Dependencies

```bash
# Fork and clone the repository
git clone [STUDENT: Your repository URL]
cd big-data-streamlit-template

# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install required packages
pip install -r requirements.txt
```

### Step 2: Set Up Apache Kafka

#### Local Kafka Installation

### Step 3: Configure Storage System

#### Option 1: HDFS Setup

#### Option 2: MongoDB Setup

### Step 4: Environment Configuration (optional):

Create a `.env` file for configuration (optional):

```env
KAFKA_BROKER=localhost:9092
KAFKA_TOPIC=streaming-data
STORAGE_TYPE=[STUDENT: hdfs OR mongodb]
HDFS_HOST=localhost
HDFS_PORT=9000
MONGO_URI=mongodb://localhost:27017
MONGO_DB=streaming_data
```

## 🏃‍♂️ Running the Project

### Execution Sequence

1. **Start Kafka**
2. **Run Data Producer**
3. **Launch Dashboard**

### Step 1: Start Data Producer

```bash
# Basic producer with default settings
python producer.py

# Custom configuration
python producer.py --bootstrap-servers localhost:9092 --topic streaming-data --rate 5 --duration 300

# Parameters:
# --bootstrap-servers: Kafka broker address
# --topic: Kafka topic name
# --rate: Messages per second
# --duration: Run duration in seconds (omit for continuous)
```

### Step 2: Launch Dashboard

```bash
streamlit run app.py
```

The dashboard will be available at: `http://localhost:8501`

### Step 3: Verify Data Flow

1. Check producer output in terminal
2. Verify Kafka messages: `kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic streaming-data --from-beginning`
3. Monitor dashboard for real-time updates

## 📋 Student Tasks

### Core Implementation Tasks

#### 1. Data Producer Implementation (`producer.py`)

- [ ] **Implement Kafka Producer**: Complete the `StreamingDataProducer` class
- [ ] **Data Generation**: Replace sample data with realistic data for your use case
- [ ] **Serialization**: Choose and implement data serialization (JSON/Avro)
- [ ] **Error Handling**: Add comprehensive error handling and retry logic
- [ ] **Rate Limiting**: Implement proper message rate control

**Use Case Options** (choose one or define your own):
- IoT Sensor Data (temperature, humidity, pressure)
- Financial Data (stock prices, transactions)
- Web Analytics (page views, user interactions)
- System Metrics (CPU usage, memory, network)

#### 2. Dashboard Implementation (`app.py`)

- [ ] **Kafka Consumer**: Implement `consume_kafka_data()` function
- [ ] **Storage Integration**: Implement `query_historical_data()` function
- [ ] **Data Visualization**: Customize charts for your specific data
- [ ] **Configuration**: Update sidebar controls for your storage system

#### 3. Storage System Integration

**Choose ONE storage system to implement:**

- [ ] **HDFS Integration**: Implement HDFS data writing and querying
- [ ] **MongoDB Integration**: Implement MongoDB document storage and queries

## 📁 Deliverables

### Required Files

- [ ] **`app.py`**: Complete Streamlit dashboard implementation
- [ ] **`producer.py`**: Functional Kafka data producer
- [ ] **`requirements.txt`**: All necessary dependencies
- [ ] **`README.md`**: Project documentation (this file)
- [ ] **`.gitignore`**: Git ignore rules (provided)

## 📚 Learning Resources

### Essential Documentation
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Streamlit Documentation](https://docs.streamlit.io/)
- [HDFS Python API (PyArrow)](https://arrow.apache.org/docs/python/filesystems.html)
- [MongoDB Python Driver (PyMongo)](https://pymongo.readthedocs.io/)

## Student Implementation Notes

### Data Schema Requirements

Your implemented data must include these **required fields** for dashboard compatibility:

```python
{
    "timestamp": "2023-10-01T12:00:00Z",  # ISO format timestamp
    "value": 123.45,                      # Numeric measurement value  
    "metric_type": "temperature",         # Type of metric
    "sensor_id": "sensor_001",            # Unique data source identifier
    # Add additional fields for your specific use case
}
```

**Good luck with your streaming data dashboard project!** 

*Remember: This template provides the structure - your  skills will bring it to life with meaningful data and insights.*