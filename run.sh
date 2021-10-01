#!/bin/bash
echo "Installing Python"
python --version
sudo apt-get update
sudo apt-get install python3.9 python3-pip

echo "Installing packages"
pip install numpy
pip install pandas

echo "Running script"
python ./Ex.py