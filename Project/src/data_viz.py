"""
Script:        data_viz.py
Authors:       Gopinath Mohanasundaram
Created:       2025-11-19
Last Modified: 2025-11-19
Version:       1.0

Purpose:       Provides line and bar graph plotting functionality.
Usage:         main.ipynb

Inputs:        monthly_trends/{year}/part-*.csv
Outputs:       plots
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Plots bar graphs on top trending keywords
def plot_kwfa_stats(file):
    
    # Load CSV
    df = pd.read_csv(file)
    
    # Convert month to actual datetime for better plotting
    df['month'] = pd.to_datetime(df['month'])
    
    # Sort by frequency for top keywords
    df_sorted = df.sort_values(by="freq", ascending=False)
    df_top20 = df_sorted.head(20)
    
    # 1. Keyword Frequency
    plt.figure(figsize=(12, 6))
    plt.bar(df_top20["token"], df_top20["freq"])
    plt.xticks(rotation=45, ha="right")
    plt.xlabel("Keywords")
    plt.ylabel("Frequency")
    plt.title("Top 20 Keywords by Frequency")
    plt.tight_layout()
    plt.show()
    
    # 2. Comparing CURRENT VS PREVIOUS frequency
    df_comp = df_top20.sort_values(by="freq", ascending=False)
    
    x = np.arange(len(df_comp))
    width = 0.35
    
    plt.figure(figsize=(12, 6))
    plt.bar(x - width/2, df_comp["freq"], width, label="Current Freq")
    plt.bar(x + width/2, df_comp["prev_freq"], width, label="Previous Freq")
    
    plt.xticks(x, df_comp["token"], rotation=45, ha="right")
    plt.xlabel("Keyword")
    plt.ylabel("Frequency")
    plt.title("Current vs Previous Keyword Frequency (Top 20 Keywords)")
    plt.legend()
    plt.tight_layout()
    plt.show()
    
    # 3. Month-wise Distribution (Show total email keyword frequency per month)
    df_month = df.groupby("month")["freq"].sum().reset_index()
    
    plt.figure(figsize=(12, 6))
    plt.plot(df_month["month"], df_month["freq"], marker='o')
    plt.xlabel("Month")
    plt.ylabel("Total Keyword Frequency")
    plt.title("Month-wise Keyword Frequency Distribution")
    plt.grid(True)
    plt.tight_layout()
    plt.show()
    
    # 4.Keyword Trend Line Chart
    
    # Taking only the top 20 tokens for clarity
    top_tokens = df_top20["token"].unique()
    
    plt.figure(figsize=(14, 7))
    
    # Generating unique colors using a colormap
    colors = plt.cm.tab20(np.linspace(0, 1, len(top_tokens)))
    
    # Plotting each token with a unique color
    for i, token in enumerate(top_tokens):
        temp = df[df['token'] == token]
        plt.plot(temp['month'], temp['freq'], 
                 color=colors[i], 
                 marker='o', 
                 label=token)
    
    plt.xlabel("Month")
    plt.ylabel("Frequency")
    plt.title("Keyword Trends Over Time (Top 20 Keywords)")
    plt.legend(loc="upper left", bbox_to_anchor=(1, 1))
    plt.grid(True)
    plt.tight_layout()
    plt.show()