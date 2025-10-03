# -*- coding: utf-8 -*-
import pandas as pd
import tkinter as tk
from tkinter import filedialog, messagebox
import re
from datetime import datetime
import csv
import os
from CSVDataValidator import CSVDataValidator
from csv_diff_tool import run_diff
from regex_checker import open_regex_check_window
from pyspark.sql import SparkSession
import tkinter.simpledialog as simpledialog
from tkinter import messagebox
from pyspark.sql.functions import expr
from vlookup_tool import open_vlookup_window


try:
    spark = SparkSession.builder.appName("CSVValidator").getOrCreate()
    SPARK_AVAILABLE = True
except Exception as e:
    print(f"PySpark not available: {e}")
    spark = None
    SPARK_AVAILABLE = False


class CSVValidatorApp:
    def __init__(self, master):
        self.master = master
        master.title("Modern CSV Validator")
        master.geometry("700x600")

        self.df = None
        self.sdf = None  # Spark DataFrame - created once and reused
        self.file_path = None
        self.parquet_path = None
        self.check_vars = {}
        self.type_vars = {}

        # Upload and Cache buttons frame
        button_frame = tk.Frame(master)
        button_frame.pack(pady=10)
        
        self.upload_button = tk.Button(button_frame, text="Upload CSV", command=self.load_csv)
        self.upload_button.pack(side=tk.LEFT, padx=5)
        
        self.clear_cache_button = tk.Button(
            button_frame, 
            text="Clear Cache", 
            command=self.clear_cache
        )
        self.clear_cache_button.pack(side=tk.LEFT, padx=5)

        self.check_frame = tk.LabelFrame(master, text="Select Columns & Data Types", padx=10, pady=10)
        self.check_frame.pack(fill="both", expand=True, padx=20, pady=10)

        self.scrollbar = tk.Scrollbar(self.check_frame)
        self.scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        self.canvas = tk.Canvas(self.check_frame, yscrollcommand=self.scrollbar.set)
        self.canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        self.scrollbar.config(command=self.canvas.yview)

        self.check_inner_frame = tk.Frame(self.canvas)
        self.canvas.create_window((0, 0), window=self.check_inner_frame, anchor='nw')
        self.check_inner_frame.bind("<Configure>", lambda e: self.canvas.configure(scrollregion=self.canvas.bbox("all")))

        self.selected_action = tk.StringVar()
        self.selected_action.set("Choose Action")

        actions = [
            "Check Missing Fields",
            "Find Duplicates",
            "Validate Data Types",
            "Compare CSV Files",
            "Check Regex" ,
            "Validate Spark SQL Expression",
            "Run VLOOKUP"
        ]

        action_frame = tk.Frame(master)
        action_frame.pack(pady=10)

        self.action_menu = tk.OptionMenu(action_frame, self.selected_action, *actions)
        self.action_menu.pack(side=tk.LEFT, padx=(0, 5))

        self.info_button = tk.Button(
            action_frame,
            text="ℹ",
            font=("Helvetica", 12, "bold"),
            command=self.show_data_info,
            width=3
        )
        self.info_button.pack(side=tk.LEFT)
        self.info_button.config(state=tk.DISABLED)


        self.run_button = tk.Button(master, text="Run Action", command=self.run_selected_action)
        self.run_button.pack(pady=5)

    def run_selected_action(self):
        action = self.selected_action.get()
        
        if action == "Check Missing Fields":
            self.validate_missing()
        elif action == "Find Duplicates":
            self.find_duplicates()
        elif action == "Validate Data Types":
            self.validate_data_types()
        elif action == "Compare CSV Files":
            self.open_compare_window()
        elif action == "Check Regex":
            open_regex_check_window(
                self.master,
                self.df,
                self.show_popup
            )
        elif action == "Validate Spark SQL Expression":
            self.validate_spark_sql_expression()
        elif action == "Run VLOOKUP":
            open_vlookup_window(self.master, self.show_popup)

        else:
            messagebox.showwarning("Warning", "Please choose an action from the dropdown.")


    def load_csv(self):
        file_path = filedialog.askopenfilename(filetypes=[("CSV files", "*.csv")])
        if not file_path:
            return

        try:
            # Create cache directory if it doesn't exist
            cache_dir = os.path.join(os.path.dirname(file_path), 'validatorAppCache')
            if not os.path.exists(cache_dir):
                os.makedirs(cache_dir)
            
            # Generate cache file paths in the cache directory
            filename = os.path.basename(file_path)
            cache_filename = os.path.splitext(filename)[0] + '_optimized'
            parquet_path = os.path.join(cache_dir, cache_filename + '.parquet')
            
            if os.path.exists(parquet_path):
                # Load from parquet for faster loading
                self.df = pd.read_parquet(parquet_path, engine='pyarrow')
                self.parquet_path = parquet_path
                messagebox.showinfo("Success", 
                                  "Loaded from optimized cache!\n"
                                  "Rows: {:,}\n"
                                  "Columns: {}".format(len(self.df), len(self.df.columns)))
                
                # Create Spark DataFrame once for faster SQL queries
                if SPARK_AVAILABLE:
                    self.sdf = spark.createDataFrame(self.df)
                    self.sdf.createOrReplaceTempView("uploaded_data")
                else:
                    self.sdf = None
            else:
                # Load from CSV and create optimized versions
                with open(file_path, 'r', encoding='iso-8859-1') as f:
                    sample = f.read(2048)
                    try:
                        delimiter = csv.Sniffer().sniff(sample, delimiters=";,:|").delimiter
                    except csv.Error:
                        delimiter = ";"
                    f.seek(0)
                    df = pd.read_csv(f, delimiter=delimiter, engine='python')

                df.columns = df.columns.str.strip().str.replace('\ufeff', '', regex=True)
                self.df = df.copy()
                self.df.insert(0, "Original Row#", self.df.index + 2)

                # Save optimized versions for faster processing
                pickle_path = os.path.join(cache_dir, cache_filename + '.pkl')
                
                # Save as pickle for small to medium files
                self.df.to_pickle(pickle_path)
                
                # Save as parquet for large files (better compression and faster reading)
                self.df.to_parquet(parquet_path, engine='pyarrow', compression='snappy')
                
                # Use parquet path for better performance
                self.parquet_path = parquet_path
                
                messagebox.showinfo("Success", 
                                  "CSV loaded successfully!\n"
                                  "Rows: {:,}\n"
                                  "Columns: {}\n"
                                  "Optimized cache created for faster processing.".format(len(df), len(df.columns)))
                
                # Create Spark DataFrame once for faster SQL queries
                self.sdf = spark.createDataFrame(self.df)
                self.sdf.createOrReplaceTempView("uploaded_data")

            self.file_path = file_path
            self.populate_checkboxes()
            
        except Exception as e:
            messagebox.showerror("Error", str(e))
        self.info_button.config(state=tk.NORMAL)

    def populate_checkboxes(self):
        for widget in self.check_inner_frame.winfo_children():
            widget.destroy()

        self.check_vars.clear()
        self.type_vars.clear()

        for col in self.df.columns:
            frame = tk.Frame(self.check_inner_frame)
            frame.pack(anchor='w', fill='x')

            var = tk.BooleanVar()
            chk = tk.Checkbutton(frame, text=col, variable=var)
            chk.pack(side='left')
            self.check_vars[col] = var

            dtype = tk.StringVar(value="string")

            def on_type_change(new_value, col=col, dtype=dtype):
                if new_value == "enum":
                    from tkinter import simpledialog
                    enum_values = simpledialog.askstring(
                        "ENUM Values",
                        "Enter allowed ENUM values for column '{}' (comma-separated):".format(col)
                    )
                    if enum_values:
                        values_list = [v.strip() for v in enum_values.split(",")]
                        dtype.set("enum:{}".format(','.join(values_list)))
                    else:
                        dtype.set("string")
                else:
                    dtype.set(new_value)

            dropdown = tk.OptionMenu(
                frame,
                dtype,
                "string", "int", "double", "date", "enum" ,"time",
                command=on_type_change
            )
            dropdown.pack(side='right')

            self.type_vars[col] = dtype



    def get_selected_columns(self):
        return [col for col, var in self.check_vars.items() if var.get()]

    def find_duplicates(self):
        selected_cols = self.get_selected_columns()
        if not selected_cols:
            messagebox.showwarning("Warning", "Please select key columns.")
            return

        dup_values = self.df[selected_cols].duplicated(keep=False)
        duplicate_rows = self.df[dup_values]

        if duplicate_rows.empty:
            messagebox.showinfo("Result", "✅ No duplicate key combinations found.")
        else:
            preview = duplicate_rows.head(10).copy()
            summary = "📊 Duplicate Rows Summary:\nTotal Rows in File: {}\nTotal Duplicates Found: {}\n\nSample Preview:\n".format(len(self.df), len(duplicate_rows))
            sample_text = preview.to_csv(sep=";", index=False)
            self.show_popup("❗ Duplicate Sample", summary + sample_text, duplicate_rows)

    def validate_missing(self):
        selected_cols = self.get_selected_columns()
        if not selected_cols:
            messagebox.showwarning("Warning", "Please select mandatory fields.")
            return

        null_counts = self.df[selected_cols].isnull().sum()
        null_counts = null_counts[null_counts > 0]

        if null_counts.empty:
            messagebox.showinfo("Result", "✅ No missing values in selected columns.")
        else:
            summary = "📊 Missing Value Summary:\nTotal Rows in File: {}\nColumns Checked: {}\n\n".format(len(self.df), len(selected_cols))
            for col, count in null_counts.items():
                summary += "{}: {} missing values\n".format(col, count)
            missing_rows = self.df[self.df[selected_cols].isnull().any(axis=1)]
            sample = missing_rows.head(10).copy()
            sample_text = sample.to_csv(sep=";", index=False)
            self.show_popup("❌ Missing Data Sample", summary + "\nSample Rows:\n" + sample_text, missing_rows)

    def validate_data_types(self):
        if self.df is None:
            messagebox.showwarning("Warning", "Please upload a CSV first.")
            return

        selected_cols = self.get_selected_columns()
        if not selected_cols:
            # If no columns selected, default to all columns
            selected_cols = list(self.df.columns)


        type_map = {}

        for col in selected_cols:
            dtype_value = self.type_vars[col].get()
            if dtype_value.startswith("enum:"):
                enum_values = dtype_value.split(":", 1)[1].split(",")
                enum_values = [v.strip() for v in enum_values]
                type_map[col] = ("enum", enum_values)
            else:
                type_map[col] = dtype_value
        validator = CSVDataValidator(self.df)
        errors = validator.validate_data_types(selected_cols, type_map)
        format_errors = []
        for col in selected_cols:
            invalid_rows = []
            for idx, val in self.df[col].dropna().items():
                val_str = str(val)
                if CSVDataValidator.needs_wrapping(val_str):
                    reason = None
                    if CSVDataValidator.contains_quote(val_str):
                        reason = "Contains double quotes but is not wrapped in quotes"
                    elif CSVDataValidator.contains_delimiter(val_str):
                        reason = "Contains delimiter (comma or semicolon) but is not wrapped in quotes"
                    elif CSVDataValidator.contains_line_break(val_str):
                        reason = "Contains newline but is not wrapped in quotes"
                    elif CSVDataValidator.contains_backslash(val_str):
                        reason = "Contains backslash but is not wrapped in quotes"
                    else:
                        reason = "Contains special characters needing quoting"

                    invalid_rows.append((idx, val_str, reason))

            if invalid_rows:
                errors.append((col, invalid_rows))


        errors.extend(format_errors)


        if not errors:
            messagebox.showinfo("Result", "✅ All selected columns match expected data types and CSV formatting rules.")
        else:
            summary = "📊 Data Type Validation Summary:\nTotal Rows in File: {}\nColumns Checked: {}\n\n".format(len(self.df), len(selected_cols))
            error_rows = set()
            for col, rows in errors:
                summary += "🔹 {}: {} invalid entries\n".format(col, len(rows))
                error_rows.update(idx for idx, val, reason in rows)

            df_error = self.df.loc[list(error_rows)].copy()
            total_invalid = len(df_error)
            sample = df_error.head(10).to_csv(sep=";", index=False)

            summary += "\nNote: {} invalid rows found. The download will include all of them.\n".format(total_invalid)

            self.show_popup("❌ Data Type & Format Errors", summary + "\nSample Rows:\n" + sample, df_error)



    def open_compare_window(self):
        window = tk.Toplevel(self.master)
        window.title("Compare CSV Files")
        window.geometry("800x300")

        file1_path = tk.StringVar()
        file2_path = tk.StringVar()
        pk_value = tk.StringVar()

        def browse_file(var):
            path = filedialog.askopenfilename(filetypes=[("CSV files", "*.csv")])
            if path:
                var.set(path)

        frm = tk.Frame(window, padx=20, pady=20)
        frm.pack(fill="both", expand=True)

        tk.Label(frm, text="CSV File 1:").grid(row=0, column=0, sticky="w", pady=8)
        e1 = tk.Entry(frm, textvariable=file1_path, width=50)
        e1.grid(row=0, column=1, padx=(10, 5))
        tk.Button(frm, text="Browse", command=lambda: browse_file(file1_path)).grid(row=0, column=2)

        tk.Label(frm, text="CSV File 2:").grid(row=1, column=0, sticky="w", pady=8)
        e2 = tk.Entry(frm, textvariable=file2_path, width=50)
        e2.grid(row=1, column=1, padx=(10, 5))
        tk.Button(frm, text="Browse", command=lambda: browse_file(file2_path)).grid(row=1, column=2)

        tk.Label(frm, text="Primary Key (comma-separated):").grid(row=2, column=0, sticky="w", pady=8)
        e3 = tk.Entry(frm, textvariable=pk_value, width=50)
        e3.grid(row=2, column=1, columnspan=2, padx=(10, 5))

        def run_comparison():
            f1 = file1_path.get()
            f2 = file2_path.get()
            pk = pk_value.get().strip() or None

            if not f1 or not f2:
                messagebox.showwarning("Warning", "Please select both CSV files.")
                return

            try:
                run_diff(f1, f2, pk)
                messagebox.showinfo("Done", "CSV comparison complete.\nSee normalized_diff.txt for details.")
            except Exception as e:
                messagebox.showerror("Error", str(e))

        tk.Button(frm, text="Run Compare", command=run_comparison, bg="#2196F3", fg="white", width=15).grid(row=3, column=1, pady=20)



    def show_popup(self, title, text, full_df=None):
        popup = tk.Toplevel(self.master)
        popup.title(title)
        popup.geometry("800x400")

        text_area = tk.Text(popup, wrap="none")
        text_area.insert(tk.END, text)
        text_area.pack(fill="both", expand=True)

        btn_frame = tk.Frame(popup)
        btn_frame.pack(pady=5)

        def download_csv():
            if full_df is not None:
                export_path = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV files", "*.csv")])
                if export_path:
                    full_df.to_csv(export_path, index=False, sep=';')
                    messagebox.showinfo("Saved", "CSV exported to {}".format(export_path))

        download_btn = tk.Button(btn_frame, text="Download All Invalid Rows", command=download_csv)
        download_btn.pack(side="left", padx=5)

        close_btn = tk.Button(btn_frame, text="Close", command=popup.destroy)
        close_btn.pack(side="right", padx=5)
    def clear_cache(self):
        """Clear all cache files from the cache directory"""
        try:
            if self.file_path:
                # Get the cache directory for the current file
                cache_dir = os.path.join(os.path.dirname(self.file_path), 'validatorAppCache')
            else:
                # If no file loaded, clear cache in current directory
                cache_dir = 'validatorAppCache'
            
            if not os.path.exists(cache_dir):
                messagebox.showinfo("Info", "Cache directory doesn't exist. Nothing to clear.")
                return
            
            # Count files before deletion
            cache_files = os.listdir(cache_dir)
            if not cache_files:
                messagebox.showinfo("Info", "Cache directory is already empty.")
                return
            
            # Ask for confirmation
            files_preview = ', '.join(cache_files[:3])
            if len(cache_files) > 3:
                files_preview += '...'
            
            result = messagebox.askyesno(
                "Confirm Clear Cache", 
                "Are you sure you want to clear all cache files?\n\n"
                "Cache directory: {}\n"
                "Files to delete: {}\n"
                "Files: {}".format(cache_dir, len(cache_files), files_preview)
            )
            
            if result:
                # Delete all files in cache directory
                for filename in cache_files:
                    file_path = os.path.join(cache_dir, filename)
                    if os.path.isfile(file_path):
                        os.remove(file_path)
                
                # Remove cache directory if empty
                try:
                    os.rmdir(cache_dir)
                    messagebox.showinfo("Success", "Cache cleared successfully!\n\nCache directory removed: {}".format(cache_dir))
                except OSError:
                    # Directory not empty or other error
                    messagebox.showinfo("Success", "Cache cleared successfully!\n\nFiles deleted from: {}".format(cache_dir))
                
                # Reset parquet path if it was pointing to cleared cache
                if self.parquet_path and not os.path.exists(self.parquet_path):
                    self.parquet_path = None
                    
        except Exception as e:
            messagebox.showerror("Error", "Failed to clear cache:\n{}".format(str(e)))

    def show_data_info(self):
        if self.df is None:
            messagebox.showwarning("Warning", "No CSV loaded.")
            return

        info_lines = []
        info_lines.append("✅ File Loaded: {}".format(self.file_path))
        info_lines.append("Total Rows: {}".format(len(self.df)))
        info_lines.append("Total Columns: {}\n".format(len(self.df.columns)))

        for col in self.df.columns:
            col_data = self.df[col]
            info_lines.append("--- Column: {} ---".format(col))
            info_lines.append("Data Type: {}".format(col_data.dtype))
            info_lines.append("Nulls: {}".format(col_data.isnull().sum()))
            unique_vals = col_data.dropna().unique()
            info_lines.append("Unique Values: {}".format(len(unique_vals)))
            if len(unique_vals) <= 10:
                info_lines.append("Values: {}".format(', '.join(map(str, unique_vals))))
            else:
                top_vals = col_data.value_counts().head(5)
                top_str = ", ".join("{} ({})".format(v, c) for v, c in top_vals.items())
                info_lines.append("Top 5 Values: {}".format(top_str))
            if pd.api.types.is_numeric_dtype(col_data):
                info_lines.append("Min: {}".format(col_data.min()))
                info_lines.append("Max: {}".format(col_data.max()))
                info_lines.append("Mean: {}".format(round(col_data.mean(), 2)))
            info_lines.append("")  # add blank line

        final_text = "\n".join(info_lines)

        self.show_popup("📊 CSV Data Profile", final_text)


        
    def get_large_input(self, prompt_title, prompt_label):
        dialog = tk.Toplevel(self.master)
        dialog.title(prompt_title)
        dialog.geometry("850x500")

        tk.Label(dialog, text=prompt_label).pack(pady=5)

        text_box = tk.Text(dialog, height=8, width=100)
        text_box.pack(padx=10, pady=5)

        self.col_type_map = {}  # store in self so it can be reused in on_submit

        mapping_label = tk.Label(dialog, text="Column Data Types: None")
        mapping_label.pack()

        mapping_rows_frame = tk.Frame(dialog)
        mapping_rows_frame.pack(pady=5)

        def update_mapping_label():
            if not self.col_type_map:
                mapping_label.config(text="Column Data Types: None")
            else:
                display_text = ", ".join("{}: {}".format(col, typ) for col, typ in self.col_type_map.items())
                mapping_label.config(text="Column Data Types: {}".format(display_text))

        # List to track all dropdown pairs
        self.mapping_widgets = []

        def add_column():
            row = tk.Frame(mapping_rows_frame)
            row.pack(fill="x", pady=2)

            col_var = tk.StringVar(value="")  # no default
            dtype_var = tk.StringVar(value="string")

            col_menu = tk.OptionMenu(row, col_var,"", *self.df.columns)
            col_menu.pack(side="left", padx=5)

            dtype_menu = tk.OptionMenu(row, dtype_var, "string", "int", "double", "date", "enum","time")
            dtype_menu.pack(side="left", padx=5)

            # Store the variable references
            self.mapping_widgets.append((col_var, dtype_var))

            def update_map(*args):
                self.col_type_map.clear()
                seen_cols = set()

                for c_var, d_var in self.mapping_widgets:
                    col = c_var.get()
                    dtype = d_var.get()
                    if col and col not in seen_cols:
                        self.col_type_map[col] = dtype
                        seen_cols.add(col)

                update_mapping_label()
                print("Updated Map:", self.col_type_map)

            col_var.trace_add("write", update_map)
            dtype_var.trace_add("write", update_map)

            update_map()  # Initialize the mapping


        btn_frame = tk.Frame(dialog)
        btn_frame.pack(pady=10)

        tk.Button(btn_frame, text="Add Column", command=add_column).pack(side="left", padx=10)

        def submit():
            expr = text_box.get("1.0", tk.END).strip()
            if not expr:
                messagebox.showerror("Error", "Please enter a Spark SQL query.")
                return
            self.latest_expr = expr
            dialog.destroy()
            self.on_submit()  # Now that expression and type map are set, run logic

        tk.Button(btn_frame, text="Submit", command=submit).pack(side="left", padx=10)


        result = {"input": None}

    def validate_sql_query(self, expression, type_map):
        """Simple validation - just check if it's a SELECT query"""
        query_lower = expression.strip().lower()
        if not query_lower.startswith('select'):
            messagebox.showerror(
                "Invalid Query Format",
                "❌ Please enter a complete SQL query starting with SELECT.\n\n"
                "Example: SELECT Age FROM uploaded_data\n"
                "Example: SELECT Name, Age FROM uploaded_data WHERE Age > 18"
            )
            return False
        return True

    def on_submit(self):
        expression = self.latest_expr  # Get stored expression
        if not expression:
            messagebox.showerror("Error", "❗ No expression provided.")
            return

        type_map = self.col_type_map
        
        # Step 1: Simple validation - just check if it's a SELECT query
        if not self.validate_sql_query(expression, type_map):
            return  # Stop if validation fails
        
        # Step 2: Quick processing (no message needed - should be fast now)

        # Step 3: Cast columns with error popup on failure
        for col, dtype in type_map.items():
            if col in self.df.columns:
                try:
                    if dtype == 'int':
                        self.df[col] = self.df[col].astype(int)
                    elif dtype == 'double':
                        self.df[col] = self.df[col].astype(float)
                    elif dtype == 'string':
                        self.df[col] = self.df[col].astype(str)
                    elif dtype == 'date':
                        self.df[col] = pd.to_datetime(self.df[col], errors='raise')  # raise on error
                except Exception as e:
                    messagebox.showerror(
                        "Casting Error",
                        "Column '{}' has mixed or invalid data for type '{}'.\n"
                        "Details: {}".format(col, dtype, e)
                    )
                    return  # stop further processing

        # Step 4: Execute the SQL query with processing popup
        try:
            # Show processing popup
            processing_window = tk.Toplevel(self.master)
            processing_window.title("Processing Query")
            processing_window.geometry("400x150")
            processing_window.resizable(False, False)
            
            # Center the window
            processing_window.transient(self.master)
            processing_window.grab_set()
            
            # Processing content
            tk.Label(processing_window, text="🔄 Processing SQL Query...", 
                    font=("Helvetica", 12, "bold")).pack(pady=20)
            tk.Label(processing_window, text="Please wait while executing your query.", 
                    font=("Helvetica", 10)).pack(pady=5)
            
            # Progress bar (simplified - just a visual indicator)
            progress_frame = tk.Frame(processing_window)
            progress_frame.pack(pady=20)
            
            progress_label = tk.Label(progress_frame, text="⏳ Executing query...", 
                                    font=("Helvetica", 9), fg="blue")
            progress_label.pack()
            
            # Update the window to show the popup
            processing_window.update()
            
            # Use the pre-created Spark DataFrame for faster execution
            if self.sdf is None:
                # Fallback: create Spark DataFrame if not already created
                self.sdf = spark.createDataFrame(self.df)
                self.sdf.createOrReplaceTempView("uploaded_data")
            
            # Execute the complete SQL query
            query = expression
            result_df = spark.sql(query)
            
            # Close processing popup
            processing_window.destroy()
            
            # Show the results
            if result_df.count() > 0:
                result_rows = result_df.toPandas()
                sample_text = result_rows.head(20).to_csv(sep=";", index=False)
                summary = "Query Results: {} rows returned.\n\nSample Results:\n{}".format(len(result_rows), sample_text)
                self.show_popup("Query Results", summary, result_rows)
            else:
                messagebox.showinfo("Query Results", "✅ Query executed successfully - No rows returned.")

        except Exception as e:
            # Close processing popup if it's still open
            try:
                processing_window.destroy()
            except:
                pass
            messagebox.showerror("Error", "Invalid Spark SQL expression:\n{}".format(e))



    def validate_spark_sql_expression(self):
        if not SPARK_AVAILABLE:
            messagebox.showerror("Error", "PySpark is not available in this packaged version.\n\nSpark SQL features are disabled.")
            return
            
        self.get_large_input(
            "Spark SQL Query Executor",
            "Enter your complete Spark SQL query:\n\n"
            "📋 Examples:\n"
            "• SELECT Age FROM uploaded_data\n"
            "• SELECT Name, Age FROM uploaded_data WHERE Age > 18\n"
            "• SELECT COUNT(*) FROM uploaded_data WHERE Country = 'USA'\n\n"
            "💡 Tips:\n"
            "• Must start with SELECT\n"
            "• Use 'uploaded_data' as table name\n"
            "• Use column names from your CSV\n"
            "• Set data types for columns using the mapping below\n"
            "• Use standard SQL operators: =, !=, >, <, AND, OR, etc."
        )




if __name__ == "__main__":
    root = tk.Tk()
    app = CSVValidatorApp(root)
    root.mainloop()