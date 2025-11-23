# Setup Instructions

## 1. Setup Requirements

🔹 **Install Kafka with Docker Desktop**

This project uses the Wurstmeister Kafka image.

Follow this YouTube tutorial for installation and setup:\
➡️ https://www.youtube.com/watch?v=EiMi11slVnY

🔹 **Install Java 17 (Required for Kafka & Spark)**

Install Java 17 on Windows using the guide below:\
➡️ https://www.youtube.com/watch?v=ykAhL1IoQUM

------------------------------------------------------------------------

## 2. Getting the Source Code & Environment Ready

📥 **Clone or Download the Repository**

Clone using Git Bash:

``` sh
git clone https://github.com/reuel-ly/real-time-weather-streaming.git
```

Or download directly from GitHub:\
➡️ https://github.com/reuel-ly/real-time-weather-streaming

------------------------------------------------------------------------

📦 **Create a Conda Environment**

Using Anaconda Prompt or PowerShell:

``` sh
conda create -n bigdata python=3.10.13
conda activate bigdata
```

------------------------------------------------------------------------

📚 **Install Required Python Packages**

Navigate to the project folder, then install dependencies:

``` sh
pip install -r requirements.txt
```

------------------------------------------------------------------------

🔐 **Create a .env File**

Inside the project directory, create a file named `.env`.

This file stores your MongoDB connection URL and OpenWeatherMap API key.

Example `.env`:

``` sh
# This file holds sensitive data. DO NOT commit this to Git!

API_KEY="1234567890"
DATABASE_URL="mongodb+srv://db_user:dbpassword@groceryinventorysystem.gpheuwl.mongodb.net/?appName=GroceryInventorySystem"
```

------------------------------------------------------------------------

## 3. Running the Program

▶️ **Run the Kafka Producer (`producer.py`)**

``` sh
python producer.py --mode weather
```

------------------------------------------------------------------------

📊 **Run the Streamlit Dashboard (`app.py`)**

``` sh
streamlit run app.py
```

------------------------------------------------------------------------

⚠️ **Important: Clear Streamlit Cache When Re-running**

``` sh
streamlit cache clear
```

This ensures the MongoDB and PySpark clients are properly refreshed.
