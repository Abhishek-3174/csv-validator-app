# regex_checker.py

import tkinter as tk
from tkinter import messagebox
import re

def open_regex_check_window(master, df, show_popup_fn):
    """
    Opens a popup to enter regex and select a column.
    Calls show_popup_fn with results.
    """

    if df is None:
        messagebox.showwarning("Warning", "No CSV loaded.")
        return

    # Create popup window
    window = tk.Toplevel(master)
    window.title("Check Regex")
    window.geometry("500x220")

    # Label and Entry for regex
    tk.Label(window, text="Enter Regex Pattern:", font=("Helvetica", 12)).pack(pady=5)
    regex_var = tk.StringVar()
    regex_entry = tk.Entry(window, textvariable=regex_var, width=50)
    regex_entry.pack(pady=5)

    # Column dropdown
    tk.Label(window, text="Select Column:", font=("Helvetica", 12)).pack(pady=5)
    selected_col = tk.StringVar()
    if not df.columns.empty:
        selected_col.set(df.columns[0])

    dropdown = tk.OptionMenu(window, selected_col, *df.columns)
    dropdown.pack(pady=5)

    def run_regex_check():
        pattern = regex_var.get().strip()
        col = selected_col.get()

        if not pattern:
            messagebox.showwarning("Warning", "Please enter a regex pattern.")
            return

        try:
            regex = re.compile(pattern)
        except re.error as e:
            messagebox.showerror("Error", f"Invalid regex: {str(e)}")
            return

        # Apply regex
        match_mask = df[col].astype(str).str.match(regex, na=False)
        matched_rows = df[match_mask].copy()
        non_matched_rows = df[~match_mask].copy()

        # Build summary text
        summary = f"✅ Regex check complete on column '{col}'\n"
        summary += f"Pattern: {pattern}\n\n"
        summary += f"Rows matching: {len(matched_rows)}\n"
        summary += f"Rows not matching: {len(non_matched_rows)}\n"

        # Show popup with sample DataFrame
        if not matched_rows.empty:
            sample_text = matched_rows.head(10).to_csv(sep=";", index=False)
            summary += f"\n🔎 Sample matching rows:\n{sample_text}"
        else:
            summary += "\nNo matching rows found."

        show_popup_fn(
            f"Regex Check Result on {col}",
            summary,
            matched_rows if not matched_rows.empty else None
        )

        window.destroy()

    # Run button
    run_btn = tk.Button(
        window,
        text="Run Regex Check",
        command=run_regex_check,
        bg="#4CAF50",
        fg="white"
    )
    run_btn.pack(pady=10)
