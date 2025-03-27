#!/bin/env python3

import re
from collections import Counter
import matplotlib.pyplot as plt
import pandas as pd

# Load log data from Nginx access log
log_file_path = '/var/log/nginx/access.log'
#log_file_path = '/home/atomic/logs/access.log'



try:
    with open(log_file_path, 'r') as log_file:
        log_data = log_file.read()
except FileNotFoundError:
    print(f'Log file not found at {log_file_path}. Please check the path and try again.')
    log_data = ''

# Regex pattern for extracting log data
log_pattern = r'(?P<ip>\S+) - - \[(?P<date>[^\]]+)\] "(?P<method>\S+) (?P<endpoint>\S+) (?P<protocol>\S+)" (?P<status>\d+) (?P<size>\S+) "[^"]*" "(?P<user_agent>[^"]*)"'

# Parse log data
log_entries = [m.groupdict() for m in re.finditer(log_pattern, log_data)]

# Convert to DataFrame for analysis
df = pd.DataFrame(log_entries)

# Convert size to numeric, errors='coerce' will convert invalid parsing to NaN
df['size'] = pd.to_numeric(df['size'], errors='coerce')

# Display data to the user
print(df.head())

# Analysis
# Top IP addresses
top_ips = df['ip'].value_counts().head(5)
print("Top IP Addresses:")
print(top_ips)

# Most accessed endpoints
top_endpoints = df['endpoint'].value_counts().head(5)
print("\nMost Accessed Endpoints:")
print(top_endpoints)

# Status code distribution
status_distribution = df['status'].value_counts()
print("\nStatus Code Distribution:")
print(status_distribution)

# User agent analysis
user_agents = df['user_agent'].value_counts().head(5)
print("\nTop User Agents:")
print(user_agents)

# Visualizations
plt.figure()
status_distribution.plot(kind='bar', title='HTTP Status Code Distribution')
plt.xlabel('Status Code')
plt.ylabel('Frequency')
plt.show()

plt.figure()
top_endpoints.plot(kind='bar', title='Top Accessed Endpoints')
plt.xlabel('Endpoint')
plt.ylabel('Hits')
plt.show()

plt.figure()
top_ips.plot(kind='bar', title='Top IP Addresses')
plt.xlabel('IP Address')
plt.ylabel('Hits')
plt.show()

