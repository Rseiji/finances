#!/bin/bash

# Update package list and install Grafana
echo "Updating package list..."
sudo apt-get update

echo "Installing Grafana..."
sudo apt-get install -y grafana

# Start and enable Grafana service
echo "Starting Grafana service..."
sudo systemctl start grafana-server
sudo systemctl enable grafana-server

# Print Grafana access information
echo "Grafana installation complete."
echo "Access Grafana at: http://localhost:3000"
echo "Default username: admin"
echo "Default password: admin"

# Instructions for further setup
echo "Next steps:"
echo "1. Log in to Grafana at http://localhost:3000."
echo "2. Add your data source (e.g., PostgreSQL, MySQL, or InfluxDB)."
echo "3. Create a dashboard with panels for Total Investments, Crypto Profits, and Stock Market Profits."
