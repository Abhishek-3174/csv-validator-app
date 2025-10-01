# -*- coding: utf-8 -*-
import pandas as pd
import tkinter as tk
from tkinter import filedialog, messagebox
import re
from datetime import datetime
import csv
from CSVDataValidator import CSVDataValidator
from csv_diff_tool import run_diff
from regex_checker import open_regex_check_window
from pyspark.sql import SparkSession
import tkinter.simpledialog as simpledialog
from tkinter import messagebox
from pyspark.sql.functions import expr
from vlookup_tool import open_vlookup_window


spark = SparkSession.builder.appName("CSVValidator").getOrCreate()


class CSVValidatorApp:
    def __init__(self, master):
        self.master = master
        master.title("Modern CSV Validator")
        master.geometry("700x600")

        self.df = None
        self.file_path = None
        self.check_vars = {}
        self.type_vars = {}

        self.upload_button = tk.Button(master, text="Upload CSV", command=self.load_csv)
        self.upload_button.pack(pady=10)

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

            self.file_path = file_path
            self.populate_checkboxes()
            #messagebox.showinfo("Success", f"Loaded: {file_path}\nDetected delimiter: '{delimiter}'")
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
                        f"Enter allowed ENUM values for column '{col}' (comma-separated):"
                    )
                    if enum_values:
                        values_list = [v.strip() for v in enum_values.split(",")]
                        dtype.set(f"enum:{','.join(values_list)}")
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
            summary = f"📊 Duplicate Rows Summary:\nTotal Rows in File: {len(self.df)}\nTotal Duplicates Found: {len(duplicate_rows)}\n\nSample Preview:\n"
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
            summary = f"📊 Missing Value Summary:\nTotal Rows in File: {len(self.df)}\nColumns Checked: {len(selected_cols)}\n\n"
            for col, count in null_counts.items():
                summary += f"{col}: {count} missing values\n"
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
            summary = f"📊 Data Type Validation Summary:\nTotal Rows in File: {len(self.df)}\nColumns Checked: {len(selected_cols)}\n\n"
            error_rows = set()
            for col, rows in errors:
                summary += f"🔹 {col}: {len(rows)} invalid entries\n"
                error_rows.update(idx for idx, *_ in rows)

            df_error = self.df.loc[list(error_rows)].copy()
            total_invalid = len(df_error)
            sample = df_error.head(10).to_csv(sep=";", index=False)

            summary += f"\nNote: {total_invalid} invalid rows found. The download will include all of them.\n"

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
                    messagebox.showinfo("Saved", f"CSV exported to {export_path}")

        download_btn = tk.Button(btn_frame, text="Download All Invalid Rows", command=download_csv)
        download_btn.pack(side="left", padx=5)

        close_btn = tk.Button(btn_frame, text="Close", command=popup.destroy)
        close_btn.pack(side="right", padx=5)
    def show_data_info(self):
        if self.df is None:
            messagebox.showwarning("Warning", "No CSV loaded.")
            return

        info_lines = []
        info_lines.append(f"✅ File Loaded: {self.file_path}")
        info_lines.append(f"Total Rows: {len(self.df)}")
        info_lines.append(f"Total Columns: {len(self.df.columns)}\n")

        for col in self.df.columns:
            col_data = self.df[col]
            info_lines.append(f"--- Column: {col} ---")
            info_lines.append(f"Data Type: {col_data.dtype}")
            info_lines.append(f"Nulls: {col_data.isnull().sum()}")
            unique_vals = col_data.dropna().unique()
            info_lines.append(f"Unique Values: {len(unique_vals)}")
            if len(unique_vals) <= 10:
                info_lines.append(f"Values: {', '.join(map(str, unique_vals))}")
            else:
                top_vals = col_data.value_counts().head(5)
                top_str = ", ".join(f"{v} ({c})" for v, c in top_vals.items())
                info_lines.append(f"Top 5 Values: {top_str}")
            if pd.api.types.is_numeric_dtype(col_data):
                info_lines.append(f"Min: {col_data.min()}")
                info_lines.append(f"Max: {col_data.max()}")
                info_lines.append(f"Mean: {round(col_data.mean(), 2)}")
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
                display_text = ", ".join(f"{col}: {typ}" for col, typ in self.col_type_map.items())
                mapping_label.config(text=f"Column Data Types: {display_text}")

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
                messagebox.showerror("Error", "Please enter a Spark SQL expression.")
                return
            self.latest_expr = expr
            dialog.destroy()
            self.on_submit()  # Now that expression and type map are set, run logic

        tk.Button(btn_frame, text="Submit", command=submit).pack(side="left", padx=10)


        result = {"input": None}

    def on_submit(self):
        expression = self.latest_expr  # Get stored expression
        if not expression:
            messagebox.showerror("Error", "❗ No expression provided.")
            return

        type_map = self.col_type_map

        # Cast columns first with error popup on failure
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
                        f"Column '{col}' has mixed or invalid data for type '{dtype}'.\n"
                        f"Details: {e}"
                    )
                    return  # stop further processing

        # Now casted – apply the Spark SQL filter
        try:
            sdf = spark.createDataFrame(self.df)
            sdf.createOrReplaceTempView("uploaded_data")
            query = expression
            print(query)
            result_df = spark.sql(query)

            if result_df.count() == 0:
                messagebox.showinfo("Result", "✅ All rows satisfy the condition.")
            else:
                invalid_rows = result_df.toPandas()
                sample_text = invalid_rows.head(10).to_csv(sep=";", index=False)
                summary = f"Found {len(invalid_rows)} rows satisfying condition.\nSample:\n{sample_text}"
                self.show_popup("Validation Failed Rows", summary, invalid_rows)

        except Exception as e:
            messagebox.showerror("Error", f"Invalid Spark SQL expression:\n{e}")



    def validate_spark_sql_expression(self):
        self.get_large_input(
            "Spark SQL Expression",
            "Enter the Expression Spark SQL after the WHERE 'e.g. Quantity > 0 AND Country = 'USA'':"
        )





if __name__ == "__main__":
    root = tk.Tk()
    app = CSVValidatorApp(root)
    root.mainloop()
