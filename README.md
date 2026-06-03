# 99 B-Line Analysis

## Overview

This project aims to conduct hypothesis testing on two main questions: Is there a significant difference between scheduled and actual durations of the 99 B-line, and are there differences between days of the week? Inspired by my experiences of taking this bus to school, I seek to investigate the accuracy of what is being said compared to true experience and my expectations.

## Data

Public real-time transit data was retrieved from the TransLink site: https://www.translink.ca/about-us/doing-business-with-translink/app-developer-resources/gtfs/gtfs-realtime

## Requirements

Python Version 3.14.3 was used in this analysis.

### Packages
* gtfs_realtime_pb2 and google.protobuf to retrieve realtime feeds; see GTFS Realtime Overview (https://developers.google.com/transit/gtfs-realtime/) for more information
* Pandas, SQLite for data cleaning & wrangling
* SciPy for hypothesis testing
* MatPlotLib for data visualizations

### Getting Started
Data collection was done in data_collection.py. Script can be run and adjusted for desired times and should save locally, in which realtime_to_sql.ipyb and realtime_analysis.ipynb can be run in that order for further analysis. 
