# Data Profile & Time Management App

A Streamlit application that provides two powerful tools:
1. **Data Profiler** - Generate comprehensive data profiling reports
2. **Time Management Excel Generator** - Create customized time management templates

## Features

### Data Profiler
- Upload CSV or Excel files (up to 10 MB)
- Generate detailed profiling reports using ydata-profiling
- Multiple display modes (Primary, Dark, Orange)
- Minimal or full report options

### Time Management Excel Generator
- **Daily Schedule Template**: Time slots from 6 AM to 10 PM in 30-minute intervals
- **Weekly Plan Template**: Week-by-week breakdown with task priorities and status tracking
- **Monthly Plan Template**: Long-term planning with goal categorization and progress monitoring
- **Task Tracker Template**: Comprehensive task management with 20 pre-formatted tasks
- Professional Excel formatting with frozen headers and data validation
- Customizable date ranges and durations

## Installation

```bash
pip install -r requirements.txt
```

## Usage

```bash
streamlit run app.py
```

Then navigate to the app in your browser and:
- Select "Data Profiler" mode to analyze your data
- Select "Time Management Excel Generator" mode to create time management templates

## Requirements

See `requirements.txt` for all dependencies.

