import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import os
import numpy as np
import pandas as pd

OUTPUT_DIR = './plot'

def plot_borough_stats(borough_stats_pd):

    borough_stats_pd = borough_stats_pd.sort_values('total_revenue', ascending=False)
    borough_order = borough_stats_pd['pickup_borough']

    metrics_to_plot = [
        ('trip_count', 'Total Trip Count (Demand)', 'Rides', 'bar', True),
        ('avg_fare', 'Average Trip Fare', 'USD ($)', 'bar', False),
        ('avg_distance', 'Average Distance vs. Duration', 'Miles', 'bar', False),
        ('total_revenue', 'Total Revenue Generated', 'USD ($)', 'bar', True),
        ('avg_passengers', 'Average Passengers per Trip', 'Passengers', 'bar', False),
    ]

    fig, axes = plt.subplots(3, 2, figsize=(14, 14))
    axes = axes.flatten()

    for i, (col, title, ylabel, plot_type, use_log_scale) in enumerate(metrics_to_plot):
        ax = axes[i]
        
        # Plot the primary bar chart
        borough_stats_pd.plot(kind=plot_type, x='pickup_borough', y=col, ax=ax, 
                            legend=False, width=0.8, color='#1f77b4')
        
        if use_log_scale:
            ax.set_yscale('log')
            ax.set_title(f"{title} (Log Scale)", fontsize=14)
        else:
            ax.set_title(title, fontsize=14)
        
        ax.set_title(title, fontsize=14)
        ax.set_xlabel('')
        ax.set_ylabel(ylabel)
        ax.tick_params(axis='x', rotation=45, labelsize=10)
        ax.grid(axis='y', linestyle='--', alpha=0.7)

        # Special case: Plot Duration on the same axis as Distance (using a twin axis)
        if col == 'avg_distance':
            ax_twin = ax.twinx()
            ax_twin.plot(borough_order, borough_stats_pd['avg_duration_mins'], 
                        marker='o', linestyle='-', color='#d62728', label='Avg Duration')
            ax_twin.set_ylabel('Duration (mins)', color='#d62728')
            ax_twin.tick_params(axis='y', labelcolor='#d62728')
            ax.set_title('Avg. Distance & Duration per Trip')

    # Remove the unused 6th subplot
    fig.delaxes(axes[5])

    plt.tight_layout(rect=[0, 0, 1, 0.98])
    fig.suptitle('NYC Taxi Trip Characteristics by Borough', fontsize=16, y=1.0)


    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Save the plot
    output_path = os.path.join(OUTPUT_DIR, 'borough_stats.png')
    plt.savefig(output_path)

    # Close the figure
    plt.close(fig)

    return output_path


def plot_congestion_by_hour(congestion_by_hour_pd):
    congestion_by_hour_pd['trip_count'] = congestion_by_hour_pd['trip_count'].astype(int)
    hours = np.arange(24)
    hour_labels = [f'{h}:00' for h in hours[::2]]

    fig, axes = plt.subplots(2, 1, figsize=(12, 10), sharex=True)
    plt.subplots_adjust(hspace=0.35)

    ax1 = axes[0]
    ax1.set_title('Hourly Congestion Analysis: Speed vs. Trip Volume', fontsize=15)

    color_speed = '#1f77b4'
    ax1.plot(congestion_by_hour_pd['pickup_hour'], congestion_by_hour_pd['avg_speed_mph'], 
            color=color_speed, marker='o', linestyle='-', label='Avg Speed')
    ax1.set_ylabel('Average Speed (mph)', color=color_speed)
    ax1.tick_params(axis='y', labelcolor=color_speed)
    ax1.grid(axis='y', linestyle='--', alpha=0.7)

    ax2 = ax1.twinx()
    color_demand = '#ff7f0e'
    # Plot Trip Count (Bar)
    ax2.bar(congestion_by_hour_pd['pickup_hour'], congestion_by_hour_pd['trip_count'], 
            color=color_demand, alpha=0.3, width=0.8, label='Trip Count')
    ax2.set_ylabel('Trip Count (Demand)', color=color_demand)
    ax2.tick_params(axis='y', labelcolor=color_demand)

    # Add combined legend (manual placement is often best for twin axes)
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper right')
    ax1.set_xticks(hours[::2])
    ax1.set_xticklabels(hour_labels)
    ax1.tick_params(axis='x', labelbottom=True)
    ax1.set_xlabel('Pickup Hour of Day (24-Hour Clock)')


    ax3 = axes[1]
    ax3.set_title('Hourly Average Trip Duration', fontsize=15)
    ax3.plot(congestion_by_hour_pd['pickup_hour'], congestion_by_hour_pd['avg_duration_mins'], 
            color='#2ca02c', marker='o', linestyle='-', label='Avg Duration')
    ax3.set_ylabel('Average Duration (minutes)')
    ax3.grid(axis='y', linestyle='--', alpha=0.7)
    ax3.set_xticks(hours[::2])
    ax3.set_xticklabels(hour_labels)
    ax3.set_xlabel('Pickup Hour of Day (24-Hour Clock)')

    # Save the plot
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, 'hourly_congestion_stats.png')
    plt.savefig(output_path)
    plt.close(fig)

    return output_path


def plot_yearly_trends(yearly_trends_pd):

    fig, axes = plt.subplots(3, 1, figsize=(10, 12), sharex=True)

    # Define a list of metrics to plot
    metrics = [
        ('trip_count', 'Total Trip Count (Demand)', 'Trip Count'),
        ('avg_fare', 'Average Fare', 'USD ($)'),
        ('avg_distance', 'Average Distance', 'Miles')
    ]

    for i, (col, title, ylabel) in enumerate(metrics):
        ax = axes[i]
        
        # Plot the line trend
        ax.plot(yearly_trends_pd['pickup_year'], yearly_trends_pd[col], 
                marker='o', linestyle='-', label=col, color=f'C{i}')
        
        # Add title and labels
        ax.set_title(title, fontsize=14)
        ax.set_title(title, fontsize=14)
        ax.set_ylabel(ylabel, fontsize=12)
        ax.grid(True, linestyle='--', alpha=0.6)
        
        # Custom Y-axis formatting for large trip count numbers
        if col == 'trip_count':
            def formatter(x, pos):
                return f'{x*1e-6:.1f}M'
            ax.yaxis.set_major_formatter(ticker.FuncFormatter(formatter))
            
            
        # Remove X-axis labels/ticks from upper plots for cleaner look
        if i < len(metrics) - 1:
            ax.tick_params(axis='x', labelbottom=False)

    axes[-1].set_xlabel('Year', fontsize=12)
    axes[-1].set_xticks(yearly_trends_pd['pickup_year'])
    axes[-1].tick_params(axis='x', rotation=45)


    fig.suptitle('Annual Taxi Trip Trends (2011-2024)', fontsize=16, y=1.0)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, 'yearly_trends.png')
    plt.savefig(output_path)
    plt.close(fig)

    return output_path


def plot_dow_patterns(dow_patterns_pd):
    fig, axes = plt.subplots(3, 1, figsize=(10, 12), sharex=True)
    axes = axes.flatten()
    plt.subplots_adjust(hspace=0.3)

    metrics_to_plot = [
        ('trip_count', 'Total Trip Count (Demand)', 'Rides', True, 'bar'),
        ('avg_fare', 'Average Trip Fare', 'USD ($)', False, 'bar'),
        ('avg_speed_mph', 'Average Trip Speed', 'MPH', False, 'line') # Line chart for speed trend
    ]

    for i, (col, title, ylabel, format_y, plot_type) in enumerate(metrics_to_plot):
        ax = axes[i]
        
        # Plot the chart
        if plot_type == 'bar':
            dow_patterns_pd.plot(kind='bar', x='day_name', y=col, ax=ax, 
                                legend=False, width=0.8, color=f'C{i}')
            # Rotate x-labels only on the bottom plot
            if i < len(metrics_to_plot) - 1:
                ax.tick_params(axis='x', labelbottom=False)
            else:
                ax.tick_params(axis='x', rotation=45, labelsize=10)
        elif plot_type == 'line':
            dow_patterns_pd.plot(kind='line', x='day_name', y=col, ax=ax, 
                                marker='o', linestyle='-', legend=False, color=f'C{i}')
            ax.tick_params(axis='x', rotation=45, labelsize=10) # Rotate x-labels on the last plot

        ax.set_title(title, fontsize=14)
        ax.set_xlabel('')
        ax.set_ylabel(ylabel, fontsize=12)
        ax.grid(axis='y', linestyle='--', alpha=0.7)
        
        # Custom Y-axis formatting for large trip count numbers
        if format_y:
            def formatter(x, pos):
                return f'{x*1e-6:.1f}M'
            ax.yaxis.set_major_formatter(ticker.FuncFormatter(formatter))

    fig.suptitle('Weekly Taxi Trip Patterns', fontsize=16, y=1.0)
    fig.align_labels()

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, 'weekly_patterns_dashboard.png')
    plt.savefig(output_path)
    plt.close(fig)

    return output_path


def plot_monthly_patterns(monthly_patterns_pd):
    fig, axes = plt.subplots(2, 2, figsize=(14, 10), sharex=True)
    axes = axes.flatten()
    plt.subplots_adjust(hspace=0.3, wspace=0.3)

    metrics_to_plot = [
        ('trip_count', 'Total Trip Count (Demand)', 'Rides', True),
        ('avg_fare', 'Average Trip Fare', 'USD ($)', False),
        ('avg_distance', 'Average Trip Distance', 'Miles', False),
        ('avg_speed_mph', 'Average Trip Speed', 'MPH', False)
    ]

    for i, (col, title, ylabel, format_y) in enumerate(metrics_to_plot):
        ax = axes[i]
        
        # Plot the line trend
        monthly_patterns_pd.plot(kind='line', x='month_name', y=col, ax=ax, 
                                marker='o', linestyle='-', legend=False, color=f'C{i}')

        ax.set_title(title, fontsize=14)
        ax.set_xlabel('')
        ax.set_ylabel(ylabel, fontsize=12)
        ax.grid(axis='y', linestyle='--', alpha=0.7)
        
        # Custom Y-axis formatting for large trip count numbers
        if col == 'trip_count':
            def formatter(x, pos):
                return f'{x*1e-6:.1f}M'
            ax.yaxis.set_major_formatter(ticker.FuncFormatter(formatter))
        
        # Rotate X-axis labels only on the bottom plots
        if i >= 2:
            ax.tick_params(axis='x', rotation=45, labelsize=10)
        else:
            ax.tick_params(axis='x', labelbottom=False)


    fig.suptitle('Monthly Taxi Trip Trends', fontsize=16, y=1.02)
    fig.align_labels()

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, 'monthly_patterns_dashboard.png')
    plt.savefig(output_path)
    plt.close(fig)

    return output_path


def plot_airport_trips_volume(airport_trips_pd):

    airport_trips_pd = airport_trips_pd.sort_values('trip_count', ascending=True)
    airport_trips_pd['route_label'] = (
        airport_trips_pd['pickup_borough'] + ' \u2192 ' + airport_trips_pd['dropoff_borough']
    )

    fig, ax = plt.subplots(figsize=(10, 8))

    # Plot the horizontal bar chart
    ax.barh(airport_trips_pd['route_label'], airport_trips_pd['trip_count'], color='#1f77b4')

    ax.set_xscale('log')
    ax.set_title('Top 15 Airport Trip Routes by Volume (Log Scale)', fontsize=16)
    ax.set_xlabel('Trip Count (Log Scale)', fontsize=12)
    ax.set_ylabel('Route (Pickup \u2192 Dropoff)', fontsize=12)

    # Custom X-axis formatter for Log scale to show counts
    def formatter(x, pos):
        if x >= 1e6:
            return f'{x*1e-6:.1f}M'
        elif x >= 1e3:
            return f'{x*1e-3:.0f}K'
        return f'{x:.0f}'
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(formatter))

    ax.grid(axis='x', linestyle='--', alpha=0.6)
    plt.tight_layout()

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, 'airport_trips_top15.png')
    plt.savefig(output_path)
    plt.close(fig)

    return output_path

def plot_top_routes(top_routes_pd):
    top_routes_pd = top_routes_pd.sort_values('trip_count', ascending=True)
    top_routes_pd['route_label'] = (
        top_routes_pd['pickup_borough'] + ' \u2192 ' + top_routes_pd['dropoff_borough']
    )

    fig, ax = plt.subplots(figsize=(10, 8))
    # Plot the horizontal bar chart
    ax.barh(top_routes_pd['route_label'], top_routes_pd['trip_count'], color='#1f77b4')

    ax.set_xscale('log')
    ax.set_title('Top 10 Taxi Routes by Volume (Log Scale)', fontsize=16)
    ax.set_xlabel('Trip Count (Log Scale)', fontsize=12)
    ax.set_ylabel('Route (Pickup \u2192 Dropoff)', fontsize=12)

    # Custom X-axis formatter for Log scale to show counts
    def formatter(x, pos):
        if x >= 1e6:
            return f'{x*1e-6:.0f}M'
        elif x >= 1e3:
            return f'{x*1e-3:.0f}K'
        return f'{x:.0f}'
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(formatter))

    ax.grid(axis='x', linestyle='--', alpha=0.6)
    plt.tight_layout()


    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, 'top_routes.png')
    plt.savefig(output_path)
    plt.close(fig)

    return output_path

def plot_passenger_patterns(passenger_patterns_pd):
    passenger_patterns_pd = passenger_patterns_pd.sort_values('passenger_count')

    # --- Step 1: Plot Bar Chart ---
    fig, ax = plt.subplots(figsize=(10, 6))

    # Plot the vertical bar chart
    ax.bar(passenger_patterns_pd['passenger_count'], passenger_patterns_pd['trip_count'], 
        color='#2ca02c', width=0.8)

    # --- Step 2: Apply Logarithmic Scale to Y-axis ---
    ax.set_yscale('log')
    ax.set_title('Trip Volume by Passenger Count (Log Scale)', fontsize=16)
    ax.set_xlabel('Passenger Count', fontsize=12)
    ax.set_ylabel('Trip Count (Log Scale)', fontsize=12)

    # Custom X-ticks to label all integer counts
    ax.set_xticks(passenger_patterns_pd['passenger_count'])

    # Custom Y-axis formatter for Log scale to show counts
    def formatter(x, pos):
        if x >= 1e6:
            return f'{x*1e-6:.0f}M'
        elif x >= 1e3:
            return f'{x*1e-3:.0f}K'
        return f'{x:.0f}'
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(formatter))

    ax.grid(axis='y', linestyle='--', alpha=0.6)
    plt.tight_layout()

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, 'passenger_patterns.png')
    plt.savefig(output_path)
    plt.close(fig)

    return output_path

def plot_fare_efficiency(fare_efficiency_pd):
    fare_efficiency_pd = fare_efficiency_pd.sort_values('avg_fare_per_mile', ascending=False)

    fig, ax = plt.subplots(figsize=(8, 6))

    # Plot the bar chart for avg_fare_per_mile
    ax.bar(fare_efficiency_pd['pickup_borough'], fare_efficiency_pd['avg_fare_per_mile'], 
        color='#ff7f0e', width=0.7)

    ax.set_title('Average Fare Per Mile by Borough', fontsize=16)
    ax.set_xlabel('Pickup Borough', fontsize=12)
    ax.set_ylabel('Average Fare Per Mile (USD/Mile)', fontsize=12)
    ax.tick_params(axis='x', rotation=45, labelsize=10)
    ax.grid(axis='y', linestyle='--', alpha=0.7)

    # Add data labels on top of bars for exact values
    for p in ax.patches:
        ax.annotate(f'${p.get_height():.2f}', (p.get_x() + p.get_width() / 2., p.get_height()), 
                    ha='center', va='center', xytext=(0, 10), textcoords='offset points')

    plt.tight_layout()

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, 'fare_per_mile.png')
    plt.savefig(output_path)
    plt.close(fig)

    return output_path


def format_hour(hour):
    if hour == 0:
        return '12 AM'
    elif hour < 12:
        return f'{hour} AM'
    elif hour == 12:
        return '12 PM'
    else:
        return f'{hour - 12} PM'


def plot_peak_hours(peak_hours_pd):
    peak_hours_pd['hour_label'] = peak_hours_pd['pickup_hour'].apply(format_hour)
    fig, ax1 = plt.subplots(figsize=(10, 6))
    plt.subplots_adjust(right=0.85) # Make room for the right axis label

    color_count = '#1f77b4'
    ax1.bar(peak_hours_pd['hour_label'], peak_hours_pd['trip_count'], 
            color=color_count, alpha=0.7, width=0.6, label='Trip Count')
    ax1.set_ylabel('Trip Count', color=color_count, fontsize=12)
    ax1.tick_params(axis='y', labelcolor=color_count)

    # Custom Y1-axis formatting for millions
    def formatter_count(x, pos):
        return f'{x*1e-6:.1f}M'
    ax1.yaxis.set_major_formatter(ticker.FuncFormatter(formatter_count))
    ax1.grid(axis='y', linestyle='--', alpha=0.5)

    ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis
    color_fare = '#d62728'
    ax2.plot(peak_hours_pd['hour_label'], peak_hours_pd['avg_fare'], 
            color=color_fare, marker='o', linestyle='-', linewidth=2, label='Avg Fare')
    ax2.set_ylabel('Average Fare (USD $)', color=color_fare, fontsize=12)
    ax2.tick_params(axis='y', labelcolor=color_fare)

    # Custom Y2-axis formatting for currency
    ax2.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: f'${x:.2f}'))

    ax1.set_title('Top 10 Busiest Hours: Volume vs. Average Cost', fontsize=16)
    ax1.set_xlabel('Pickup Hour (Sorted by Trip Count)', fontsize=12)
    ax1.tick_params(axis='x', rotation=45)

    # Add combined legend
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper right')

    plt.tight_layout()

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, 'peak_hours.png')
    plt.savefig(output_path)
    plt.close(fig)

    return output_path


def plot_weekend_comparison(weekend_comparison_pd):
    fig, ax = plt.subplots(figsize=(8, 8))

    # Define labels and sizes
    labels = weekend_comparison_pd['is_weekend']
    sizes = weekend_comparison_pd['trip_count']
    colors = ['#1f77b4', '#ff7f0e'] # Consistent colors for visualization

    # Plot the pie chart
    ax.pie(sizes, 
        labels=labels, 
        autopct='%1.1f%%', # Format percentage with one decimal place
        startangle=90, 
        colors=colors,
        wedgeprops={'edgecolor': 'black', 'linewidth': 1, 'antialiased': True},
        textprops={'fontsize': 14}
    )

    ax.set_title('Proportion of Total Trips: Weekday vs. Weekend', fontsize=16)
    ax.axis('equal')

    plt.tight_layout()

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, 'weekend_comparison.png')
    plt.savefig(output_path)
    plt.close(fig)

    return output_path


plot_map = {
    'borough_stats': plot_borough_stats,
    'congestion_by_hour': plot_congestion_by_hour,
    'yearly_trends': plot_yearly_trends,
    'dow_patterns': plot_dow_patterns,
    'monthly_patterns': plot_monthly_patterns,
    'airport_trips': plot_airport_trips_volume,
    'top_routes': plot_top_routes,
    'passenger_patterns': plot_passenger_patterns,
    'fare_efficiency': plot_fare_efficiency,
    'peak_hours': plot_peak_hours,
    'weekend_comparison': plot_weekend_comparison
}