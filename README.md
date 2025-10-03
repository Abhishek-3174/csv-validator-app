# CSV Validator Application


## Features

- **CSV File Validation**: Upload and validate CSV files with automatic delimiter detection
- **Data Quality Checks**: Find duplicates, missing values, and data type validation
- **PySpark SQL Integration**: Run SQL queries on your CSV data
- **File Comparison**: Compare two CSV files and identify differences
- **Regex Validation**: Validate data against regular expression patterns
- **VLOOKUP Operations**: Perform lookup operations between datasets
- **Caching System**: Optimized performance with automatic caching for large files
- **Cross-Platform**: Works on Windows, macOS, and Linux

## Prerequisites

- Python 3.8 or higher
- Java 8 or higher (required for PySpark)
- Virtual environment (recommended)

## Installation & Setup

### Step 1: Clone the Repository
```bash
git clone <your-repository-url>
cd csv-validator-app
```

### Step 2: Create Virtual Environment
```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate
```

### Step 3: Install Dependencies
```bash
pip install -r requirements.txt
```

### Step 4: Verify Installation
```bash
python -c "import pandas, pyspark, numpy; print('All dependencies installed successfully!')"
```

## Running the Application

### GUI Application (Recommended)
```bash
# Make sure virtual environment is activated
source venv/bin/activate  # On macOS/Linux
# or
venv\Scripts\activate     # On Windows

# Run the application
python3 csv_gui_tool.py
```

## Usage Guide

### 1. Loading CSV Files
1. Click "Upload CSV" button
2. Select your CSV file from the file dialog
3. The application will automatically:
   - Detect the CSV delimiter (comma, semicolon, tab, etc.)
   - Load the data with caching for faster subsequent loads
   - Display column checkboxes and data type dropdowns

### 2. Selecting Columns and Data Types
1. Check the boxes next to columns you want to validate
2. Select the expected data type for each column:
   - **String**: Text data
   - **Integer**: Whole numbers
   - **Float**: Decimal numbers
   - **Date**: Date values (YYYY-MM-DD format)
   - **ENUM**: Predefined values (you'll be prompted to enter allowed values)

### 3. Choosing Validation Actions
Select from the dropdown menu:
- **Find Duplicates**: Find duplicate rows
- **Validate Missing Values**: Check for missing/null values
- **Validate Data Types**: Verify data types match expectations
- **Compare CSV Files**: Compare two CSV files
- **Check Regex**: Validate data against regex patterns
- **Validate Spark SQL Expression**: Run SQL queries on your data
- **Run VLOOKUP**: Perform VLOOKUP operations

### 4. Running Validations
1. Click "Run Action" button
2. For SQL queries, you'll see a processing popup
3. Results will be displayed in a new window with options to:
   - Download results as CSV
   - View detailed information
   - Close the results window

## Detailed Features

### Find Duplicates
- **Purpose**: Identify duplicate rows in your dataset
- **Process**: Select columns → Choose "Find Duplicates" → Run Action
- **Result**: Shows total duplicates found with sample preview
- **Download**: Get all duplicate rows as CSV

### Validate Missing Values
- **Purpose**: Check for missing, null, or empty values
- **Process**: Select columns → Choose "Validate Missing Values" → Run Action
- **Result**: Shows count of missing values per column
- **Download**: Get rows with missing values as CSV

### Validate Data Types
- **Purpose**: Ensure data matches expected types
- **Process**: Select columns and their expected data types → Choose "Validate Data Types" → Run Action
- **Result**: Shows invalid entries with reasons
- **Download**: Get all invalid rows as CSV

### Compare CSV Files
- **Purpose**: Compare two CSV files and find differences
- **Process**: Choose "Compare CSV Files" → Run Action → Select both files → Specify primary key columns
- **Result**: Shows differences between files
- **Download**: Get comparison results as CSV

### Check Regex
- **Purpose**: Validate data against regular expression patterns
- **Process**: Choose "Check Regex" → Run Action → Enter regex pattern → Select columns
- **Result**: Shows rows that match/don't match the pattern
- **Download**: Get validation results as CSV

### Validate Spark SQL Expression
- **Purpose**: Run SQL queries on your CSV data
- **Process**: Choose "Validate Spark SQL Expression" → Run Action → Enter SQL query → Submit
- **Features**:
  - Full SQL support (SELECT, WHERE, GROUP BY, ORDER BY, etc.)
  - Automatic column type casting
  - Processing popup during query execution
- **Result**: Shows query results with sample preview
- **Download**: Get full query results as CSV

### Run VLOOKUP
- **Purpose**: Perform VLOOKUP operations between datasets
- **Process**: Choose "Run VLOOKUP" → Run Action → Follow the VLOOKUP wizard
- **Result**: Shows VLOOKUP results
- **Download**: Get VLOOKUP results as CSV

## Cache Management

### Understanding Cache
Cache files are stored in "validatorAppCache" folder:
- Improves performance for large files
- Automatically created on first load
- Persists between application sessions

### Clear Cache
**When to clear cache:**
- CSV file has been updated
- Data structure changed
- Experiencing data inconsistencies
- Want to force fresh data load

**How to clear cache:**
1. Click "Clear Cache" button
2. Confirm deletion in the popup dialog
3. Cache directory will be emptied

## Project Structure

```
csv-validator-app/
├── csv_gui_tool.py              # Main GUI application
├── CSVDataValidator.py          # Core validation logic
├── csv_diff_tool.py             # CSV comparison tool
├── regex_checker.py             # Regex validation tool
├── vlookup_tool.py              # VLOOKUP operations tool
├── dataValidations.py           # Data validation utilities
├── requirements.txt             # Python dependencies
├── User_Guide.txt               # Detailed user guide
├── testdata.csv                 # Sample test data
├── test_large_sample.csv        # Large sample data for testing
├── data/                        # Data directory
├── validatorAppCache/           # Cache directory (auto-created)
└── venv/                        # Virtual environment
```

## Troubleshooting

### Common Issues

**Issue**: "No module named tkinter"
- **Solution**: Make sure you activated the virtual environment:
  ```bash
  source venv/bin/activate
  ```

**Issue**: "Missing optional dependency 'pyarrow'"
- **Solution**: Reinstall pyarrow in the virtual environment:
  ```bash
  python -m pip install pyarrow
  ```

**Issue**: Application won't start
- **Solution**: Check if virtual environment is activated and try:
  ```bash
  python -m py_compile csv_gui_tool.py
  ```

**Issue**: SQL query fails
- **Solution**: 
  - Check column names match exactly (case-sensitive)
  - Ensure data types are compatible
  - Use proper SQL syntax

**Issue**: Cache files not loading
- **Solution**: Clear cache and reload the CSV file

## Requirements

The following Python packages are required (see `requirements.txt`):

```
pandas>=2.0.0
pyspark>=3.4.0
numpy>=1.24.0
pyarrow>=10.0.0
chardet>=5.0.0
pyinstaller>=6.0.0
```
