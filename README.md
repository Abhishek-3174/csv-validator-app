# CSV Validator App

A simple Python GUI application for validating CSV files with data quality checks and SQL queries.

## Quick Start

### 1. Setup
```bash
# Clone the repository
git clone <your-repository-url>
cd csv-validator-app

# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate  # On macOS/Linux
# or
venv\Scripts\activate     # On Windows

# Install dependencies
pip install --upgrade pip
pip install -r requirements.txt
```

### 2. Run the App
```bash
python csv_gui_tool.py
```

That's it! The GUI will open and you can start validating your CSV files.

## What You Can Do

- **Upload CSV files** and validate data quality
- **Find duplicates** in your data
- **Check for missing values**
- **Validate data types** (string, integer, float, date)
- **Run SQL queries** on your CSV data
- **Compare two CSV files**
- **Validate data with regex patterns**
- **Perform VLOOKUP operations**

## Requirements

- Python 3.8 or higher
- Java 8 or higher (for PySpark)

## Troubleshooting

**Problem**: "No module named tkinter"
- **Solution**: Make sure you activated the virtual environment first

**Problem**: "Could not find pandas>=2.0.0"
- **Solution**: Use `python3` instead of `python` and upgrade pip

**Problem**: App won't start
- **Solution**: Check that virtual environment is activated and all dependencies are installed

## Files

- `csv_gui_tool.py` - Main application
- `requirements.txt` - Python dependencies
- `User_Guide.txt` - Detailed user guide

---

**Ready to validate your CSV files!** 🎉