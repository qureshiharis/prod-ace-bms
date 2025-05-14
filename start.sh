#!/bin/bash

# Start volga_consumer.py in the background
python3 volga_consumer.py &

# Start the Streamlit dashboard in the foreground
streamlit run dashboard.py --server.port=8501 --server.address=0.0.0.0