"""
Kafka Producer Template for Streaming Data Dashboard
STUDENT PROJECT: Big Data Streaming Data Producer

This is a template for students to build a Kafka producer that generates and sends
streaming data to Kafka for consumption by the dashboard.

DO NOT MODIFY THE TEMPLATE STRUCTURE - IMPLEMENT THE TODO SECTIONS
"""
import requests
import argparse
import json
import time
import random
import math
from fastavro import writer, parse_schema
import io
from datetime import datetime, timedelta
from typing import Dict, Any, List
from pymongo import MongoClient
import certifi

# Kafka libraries
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
import os
from dotenv import load_dotenv

load_dotenv() 

key = os.getenv("API_KEY")
url = os.getenv("DATABASE_URL")

MONGO_URI = url

class StreamingDataProducer:
   
    def __init__(self, bootstrap_servers: str, topic: str, mongo_uri=MONGO_URI, mongo_db="weather_db", mongo_collection="weather"):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        
        # Kafka producer configuration
        self.producer_config = {
            'bootstrap_servers': bootstrap_servers,
            # 'security_protocol': 'SSL',  # If using SSL
            # 'ssl_cafile': 'path/to/ca.pem',  # If using SSL
            # 'ssl_certfile': 'path/to/service.cert',  # If using SSL
            # 'ssl_keyfile': 'path/to/service.key',  # If using SSL
            # 'compression_type': 'gzip',  # Optional: Enable compression
            # 'batch_size': 16384,  # Optional: Tune batch size
            # 'linger_ms': 10,  # Optional: Wait for batch fill
        }

        # MongoDB setup
        if mongo_uri:
            self.mongo_client = MongoClient(mongo_uri, tlsCAFile=certifi.where())
            self.mongo_db = self.mongo_client[mongo_db]
            self.mongo_collection = self.mongo_db[mongo_collection]
            print(f"✅ Connected to MongoDB: {mongo_db}.{mongo_collection}")
        else:
            self.mongo_client = None
            print("⚠️ MongoDB URI not provided. Data will not be saved to MongoDB.")
        

        # this is openweather api key
        self.API_KEY = key   

        # seconds between API calls
        self.FETCH_INTERVAL = 30                  

        # --- AVRO SCHEMA ---
        self.WEATHER_SCHEMA = {
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
        self.parsed_schema = parse_schema(self.WEATHER_SCHEMA)
        
        # Initialize Kafka producer
        try:
            self.producer = KafkaProducer(**self.producer_config)
            print(f"Kafka producer initialized for {bootstrap_servers} on topic {topic}")
        except NoBrokersAvailable:
            print(f"ERROR: No Kafka brokers available at {bootstrap_servers}")
            self.producer = None
        except Exception as e:
            print(f"ERROR: Failed to initialize Kafka producer: {e}")
            self.producer = None


#------------------NOTE: Functions that are for testing------------------------

    # This is a function for testing only
    def generate_sample_data(self) -> Dict[str, Any]:
    
        """
        Generate realistic streaming data with stateful patterns
        
        This function creates continuously changing records with realistic patterns:
        - Daily cycles using sine waves
        - Gradual trends and realistic noise
        - Multiple metric types (temperature, humidity, pressure)
        - Temporal consistency with progressive timestamps
        
        Expected data format (must include these fields for dashboard compatibility):
        {
            "timestamp": "2023-10-01T12:00:00Z",  # ISO format timestamp
            "value": 123.45,                      # Numeric measurement value
            "metric_type": "temperature",         # Type of metric (temperature, humidity, etc.)
            "sensor_id": "sensor_001",            # Unique identifier for data source
            "location": "server_room_a",          # Sensor location
            "unit": "celsius",                    # Measurement unit
        }
        """
        
        # Select a random sensor from the expanded pool
        sensor = random.choice(self.sensors)
        sensor_id = sensor["id"]
        metric_type = sensor["type"]
        
        # Initialize sensor state if not exists
        if sensor_id not in self.sensor_states:
            config = self.metric_ranges[metric_type]
            base_value = random.uniform(config["min"], config["max"])
            trend = random.uniform(config["trend_range"][0], config["trend_range"][1])
            phase_offset = random.uniform(0, 2 * 3.14159)  # Random phase for daily cycle
            
            self.sensor_states[sensor_id] = {
                "base_value": base_value,
                "trend": trend,
                "phase_offset": phase_offset,
                "last_value": base_value,
                "message_count": 0
            }
        
        state = self.sensor_states[sensor_id]
        
        # Calculate progressive timestamp with configurable intervals
        current_time = self.base_time + timedelta(seconds=self.time_counter)
        self.time_counter += random.uniform(0.5, 2.0)  # Variable intervals for realism
        
        # Generate realistic value with patterns
        config = self.metric_ranges[metric_type]
        
        # Daily cycle using sine wave (24-hour period)
        hours_in_day = 24
        current_hour = current_time.hour + current_time.minute / 60
        daily_cycle = math.sin(2 * 3.14159 * current_hour / hours_in_day + state["phase_offset"])
        
        # Apply trend over time (slow drift)
        trend_effect = state["trend"] * (state["message_count"] / 100.0)
        
        # Add realistic noise (small random variations)
        noise = random.uniform(-config["daily_amplitude"] * 0.1, config["daily_amplitude"] * 0.1)
        
        # Calculate final value with bounds checking
        base_value = state["base_value"]
        daily_variation = daily_cycle * config["daily_amplitude"]
        raw_value = base_value + daily_variation + trend_effect + noise
        
        # Ensure value stays within reasonable bounds
        bounded_value = max(config["min"], min(config["max"], raw_value))
        
        # Update state
        state["last_value"] = bounded_value
        state["message_count"] += 1
        
        # Occasionally introduce small trend changes for realism
        if random.random() < 0.01:  # 1% chance per message
            state["trend"] = random.uniform(config["trend_range"][0], config["trend_range"][1])
        
        # NOTE: this is a fake data generator for testing only
        # Generate realistic data structure
        sample_data = {
            "timestamp": current_time.isoformat() + 'Z',
            "value": round(bounded_value, 2),
            "metric_type": metric_type,
            "sensor_id": sensor_id,
            "location": sensor["location"],
            "unit": sensor["unit"],
        }        
        return sample_data


        # --- CONFIGURATION ---
    
    # This function is not in use currently
    def fetch_stock_price(self, symbol: str):
        """Fetch the latest stock price from Alpha Vantage"""
        url = "https://www.alphavantage.co/query"
        params = {
            "function": "TIME_SERIES_INTRADAY",
            "symbol": symbol,
            "interval": "1min",
            "apikey": self.API_KEY
        }
        response = requests.get(url, params=params)
        data = response.json()
        try:
            latest_time = list(data["Time Series (1min)"].keys())[0]
            price = float(data["Time Series (1min)"][latest_time]["1. open"])
            return latest_time, price
        except Exception as e:
            print("⚠️ Alpha Vantage fetch error:", e)
            print("Response:", data)
            return None, None

    # This function is not in use currently
    def generate_stock_data(self) -> Dict[str, Any]:
        """Generate one Avro-compatible record from live Alpha Vantage data"""
        timestamp, price = self.fetch_stock_price(self.SYMBOL)
        if not timestamp or not price:
            return None

        record = {
            "timestamp": timestamp,
            "value": price,
            "metric_type": "stock_price",
            "sensor_id": self.SYMBOL,
            "location": "AlphaVantage",
            "unit": "USD",
        }
        return record

    # This is the loop for testing only
    def produce_stream(self, messages_per_second: float = 0.1, duration: int = None):
        """
        STUDENT TODO: Implement the main streaming loop
        
        Parameters:
        - messages_per_second: Rate of message production (default: 0.1 for 10-second intervals)
        - duration: Total runtime in seconds (None for infinite)
        """
        
        print(f"Starting producer: {messages_per_second} msg/sec ({1/messages_per_second:.1f} second intervals), duration: {duration or 'infinite'}")
        
        start_time = time.time()
        message_count = 0
        
        try:
            while True:
                # Check if we've reached the duration limit
                if duration and (time.time() - start_time) >= duration:
                    print(f"Reached duration limit of {duration} seconds")
                    break
                
                # Generate and send data
                data = self.generate_sample_data()
                success = self.send_message(data)
                
                if success:
                    message_count += 1
                    if message_count % 10 == 0:  # Print progress every 10 messages
                        print(f"Sent {message_count} messages...")
                
                # Calculate sleep time to maintain desired message rate
                sleep_time = 1.0 / messages_per_second
                time.sleep(sleep_time)
                
        except KeyboardInterrupt:
            print("\nProducer interrupted by user")
        except Exception as e:
            print(f"Streaming error: {e}")
        finally:
            # Implement proper cleanup
            self.close()
            print(f"Producer stopped. Total messages sent: {message_count}")

#---------------------------------------------------------------------------------------
    
    
    # Save data to MongoDB
    def save_to_mongo(self, record: dict):
            """Save a single record to MongoDB."""
            if self.mongo_client:
                try:
                    self.mongo_collection.insert_one(record)
                    print(f"💾 Saved to MongoDB: {record['timestamp']} - {record['value']}°C")
                except Exception as e:
                    print(f"⚠️ MongoDB insert error: {e}")

    # Retrieves data from OpenWeatherMap API and sends to Kafka
    def send_weather_stream(self, city: str):
        # Continuously fetch and send live weather data to Kafka
        print(f"🌤 Streaming live weather data for {city} from OpenWeatherMap...")
        while True:
            try:
                url = "https://api.openweathermap.org/data/2.5/weather"
                params = {
                    "q": city,
                    "appid": self.API_KEY,
                    "units": "metric"
                }
                response = requests.get(url, params=params)
                data = response.json()
                
                if "main" not in data:
                    print("⚠️ No weather data fetched, retrying...")
                    time.sleep(self.FETCH_INTERVAL)
                    continue

                record = {
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                    "value": data["main"]["temp"],        # Temperature in Celsius
                    "metric_type": "temperature",
                    "sensor_id": f"weather_{city.lower()}",
                    "location": city,
                    "unit": "C"
                }

                # Serialize and send to Kafka
                bytes_writer = io.BytesIO()
                writer(bytes_writer, self.parsed_schema, [record])
                if self.producer:
                    self.producer.send(self.topic, bytes_writer.getvalue())
                    print(f"✅ Sent weather data: {record['value']}°C at {record['timestamp']}")
                else:
                    print("⚠️ Kafka producer not initialized, cannot send message")

                # Save to MongoDB
                self.save_to_mongo(record)

            except Exception as e:
                print(f"⚠️ Error fetching or sending weather data: {e}")

            time.sleep(self.FETCH_INTERVAL)
    
    # Convert data to AVRO format
    def serialize_data(self, data: Dict[str, Any]) -> bytes:
        """
        Serialize data to Avro format for Kafka.
        """

        try:
            bytes_writer = io.BytesIO()
            writer(bytes_writer, self.parsed_schema, [data])
            return bytes_writer.getvalue()
        except Exception as e:
            print(f"Error serializing Avro data: {e}")
            return None

    def decode_avro_message(self, msg_value):
        bytes_reader = io.BytesIO(msg_value)
        for record in reader(bytes_reader, parsed_schema):
            return record

    def send_message(self, data: Dict[str, Any]) -> bool:
        """
        Implement message sending to Kafka
        
        Parameters:
        - data: Dictionary containing the message data
        
        Returns:
        - bool: True if message was sent successfully, False otherwise
        """
        
        # Check if producer is initialized
        if not self.producer:
            print("ERROR: Kafka producer not initialized")
            return False
        
        # Serialize the data
        serialized_data = self.serialize_data(data)
        if not serialized_data:
            print("ERROR: Serialization failed")
            return False
        
        try:
            # Send message to Kafka
            future = self.producer.send(self.topic, value=serialized_data)
            # Wait for send confirmation with timeout
            result = future.get(timeout=10)
            print(f"Message sent successfully - Topic: {self.topic}, Data: {data}")
            return True
            
        except KafkaError as e:
            print(f"Kafka send error: {e}")
            return False
        except Exception as e:
            print(f"Unexpected error during send: {e}")
            return False

    def close(self):
        """Implement producer cleanup and resource release"""
        if self.producer:
            try:
                # Ensure all messages are sent
                self.producer.flush(timeout=10)
                # Close producer connection
                self.producer.close()
                print("Kafka producer closed successfully")
            except Exception as e:
                print(f"Error closing Kafka producer: {e}")


def parse_arguments():
    """STUDENT TODO: Configure command-line arguments for flexibility"""
    parser = argparse.ArgumentParser(description='Kafka Streaming Data Producer')
    
    # STUDENT TODO: Add additional command-line arguments as needed
    parser.add_argument(
        '--bootstrap-servers',
        type=str,
        default='localhost:9092',
        help='Kafka bootstrap servers (default: localhost:9092)'
    )
    
    parser.add_argument(
        '--topic',
        type=str,
        default='streaming-data',
        help='Kafka topic to produce to (default: streaming-data)'
    )
    
    parser.add_argument(
        '--rate',
        type=float,
        default=0.1,
        help='Messages per second (default: 0.1 for 10-second intervals)'
    )
    
    parser.add_argument(
        '--duration',
        type=int,
        default=None,
        help='Run duration in seconds (default: infinite)'
    )

    parser.add_argument(
        '--mode',
        choices=['synthetic', 'weather'],
        default='synthetic',
        help='Data source mode: synthetic (default) or stock (Alpha Vantage live data)'
    )
    return parser.parse_args()


def main():
    """
    STUDENT TODO: Customize the main execution flow as needed
    
    Implementation Steps:
    1. Parse command-line arguments
    2. Initialize Kafka producer
    3. Start streaming data
    4. Handle graceful shutdown
    """
    
    print("=" * 60)
    print("STREAMING DATA PRODUCER")
    print("=" * 60)
    
    # Parse command-line arguments
    args = parse_arguments()
    
    # Initialize producer
    producer = StreamingDataProducer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic
    )
    
    # Start producing stream based on mode
    try:
        if args.mode == 'weather':
            # Run Weather API streaming mode
            producer.send_weather_stream(city="Manila")
        else:
            # Default synthetic data mode
            producer.produce_stream(
                messages_per_second=args.rate,
                duration=args.duration
            )
    except Exception as e:
        print(f"Error in main execution: {e}")
    finally:
        print("Producer execution completed")


# STUDENT TODO: Testing Instructions
if __name__ == "__main__":
    main()