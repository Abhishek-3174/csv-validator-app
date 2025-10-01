import pandas as pd
import tkinter as tk
from tkinter import filedialog, messagebox

def open_vlookup_window(master, show_popup_callback=None):
    def browse_file(entry_var, label_var, dropdown):
        path = filedialog.askopenfilename(filetypes=[("CSV files", "*.csv")])
        if path:
            entry_var.set(path)
            try:
                df = pd.read_csv(path)
                dropdown['menu'].delete(0, 'end')
                for col in df.columns:
                    dropdown['menu'].add_command(label=col, command=tk._setit(label_var, col))
            except Exception as e:
                messagebox.showerror("Error", f"Failed to read CSV: {e}")

    def run_vlookup():
        try:
            source_path = source_file_var.get()
            reference_path = reference_file_var.get()
            source_key = source_key_col.get()
            ref_key = ref_key_col.get()
            ref_col = ref_add_col.get()

            if not all([source_path, reference_path, source_key, ref_key, ref_col]):
                messagebox.showerror("Error", "All fields must be selected.")
                return

            df_source = pd.read_csv(source_path)
            df_ref = pd.read_csv(reference_path)

            df_merged = df_source.merge(
                df_ref[[ref_key, ref_col]],
                left_on=source_key,
                right_on=ref_key,
                how="left"
            )

            if show_popup_callback:
                preview_text = df_merged.head(10).to_csv(sep=";", index=False)
                summary = f"\U0001F4C3 Merge Completed!\n\nSample Merged Data Preview:\n"
                show_popup_callback("\U0001F50D VLOOKUP Result Preview", summary + preview_text, df_merged)
            else:
                save_path = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV files", "*.csv")])
                if save_path:
                    df_merged.to_csv(save_path, index=False)
                    messagebox.showinfo("Success", f"File saved to: {save_path}")
        except Exception as e:
            messagebox.showerror("Error", str(e))

    vlookup_win = tk.Toplevel(master)
    vlookup_win.title("VLOOKUP Tool")
    vlookup_win.geometry("600x400")

    source_file_var = tk.StringVar()
    reference_file_var = tk.StringVar()

    source_key_col = tk.StringVar()
    ref_key_col = tk.StringVar()
    ref_add_col = tk.StringVar()

    # Source CSV
    tk.Label(vlookup_win, text="Source CSV:").pack(pady=(10, 0))
    source_key_dropdown = tk.OptionMenu(vlookup_win, source_key_col, "")
    tk.Button(vlookup_win, text="Browse Source File",
              command=lambda: browse_file(source_file_var, source_key_col, source_key_dropdown)).pack()
    source_key_dropdown.pack(pady=5)

    # Reference CSV
    tk.Label(vlookup_win, text="Reference CSV:").pack(pady=(10, 0))
    ref_key_dropdown = tk.OptionMenu(vlookup_win, ref_key_col, "")
    tk.Button(vlookup_win, text="Browse Reference File",
              command=lambda: browse_file(reference_file_var, ref_key_col, ref_key_dropdown)).pack()
    ref_key_dropdown.pack(pady=5)

    # Additional column
    tk.Label(vlookup_win, text="Column to Add from Reference:").pack(pady=(10, 0))
    ref_add_dropdown = tk.OptionMenu(vlookup_win, ref_add_col, "")
    ref_add_dropdown.pack(pady=5)

    def load_ref_columns():
        try:
            path = reference_file_var.get()
            df = pd.read_csv(path)
            ref_add_dropdown['menu'].delete(0, 'end')
            for col in df.columns:
                if col != ref_key_col.get():
                    ref_add_dropdown['menu'].add_command(label=col, command=tk._setit(ref_add_col, col))
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load reference columns: {e}")

    tk.Button(vlookup_win, text="Load Reference Columns", command=load_ref_columns).pack(pady=5)
    tk.Button(vlookup_win, text="Run VLOOKUP", command=run_vlookup, bg="#4CAF50", fg="white").pack(pady=10)