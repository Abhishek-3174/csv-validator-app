import re
from datetime import datetime
import pandas as pd

class CSVDataValidator:

    def __init__(self, df):
        self.df = df

    # --- CSV Formatting Rules ---

    @staticmethod
    def contains_line_break(value: str) -> bool:
        return '\n' in value or '\r' in value

    @staticmethod
    def contains_delimiter(value: str) -> bool:
        return ';' in value or ',' in value

    @staticmethod
    def contains_quote(value: str) -> bool:
        return '"' in value

    @staticmethod
    def contains_backslash(value: str) -> bool:
        return '\\' in value

    @staticmethod
    def needs_wrapping(value: str) -> bool:
        return (
            CSVDataValidator.contains_line_break(value) or
            CSVDataValidator.contains_delimiter(value) or
            CSVDataValidator.contains_quote(value) or
            CSVDataValidator.contains_backslash(value)
        )

    @staticmethod
    def needs_wrapping(value: str) -> bool:
        return (
            '\n' in value or
            '\r' in value or
            ';' in value or
            ',' in value or
            '"' in value or
            '\\' in value
        )

    def sanitize(self):
        df_copy = self.df.copy()
        for col in df_copy.columns:
            df_copy[col] = df_copy[col].astype(str).apply(CSVDataValidator.wrap_field)
        return df_copy

    # --- Data Type Validation Rules ---

    def validate_data_types(self, selected_columns: list, type_map: dict):
        errors = []
        for col in selected_columns:
            expected_type = type_map[col]

            # Check for ENUM types
            enum_values = None
            if isinstance(expected_type, tuple) and expected_type[0] == "enum":
                enum_values = expected_type[1]
            else:
                enum_values = None

            invalid_rows = []

            for idx, value in self.df[col].dropna().items():
                val = str(value).strip()

                # CSV format rules check
                if CSVDataValidator.needs_wrapping(val):
                    continue  # assume wrapped values are formatted correctly

                if enum_values is not None:
                    if val not in enum_values:
                        reason = f"Value '{val}' not in allowed ENUM values: {enum_values}"
                        invalid_rows.append((idx, val, reason))

                elif expected_type == "int":
                    try:
                        if not str(int(float(val))) == val and not float(val).is_integer():
                            reason = f"Value '{val}' is not a valid integer"
                            invalid_rows.append((idx, val, reason))
                    except:
                        reason = f"Value '{val}' is not a valid integer"
                        invalid_rows.append((idx, val, reason))

                elif expected_type == "double":
                    try:
                        float(val)
                    except:
                        reason = f"Value '{val}' is not a valid double"
                        invalid_rows.append((idx, val, reason))

                elif expected_type == "date":
                    try:
                        datetime.strptime(val, "%Y-%m-%d")
                    except ValueError:
                        try:
                            datetime.strptime(val, "%m/%d/%Y")
                        except ValueError:
                            reason = f"Value '{val}' is not a valid date (expected formats YYYY-MM-DD or MM/DD/YYYY)"
                            invalid_rows.append((idx, val, reason))
                elif expected_type == "string":
                    # Allow only alphanumeric plus some general characters
                    if not re.match(r'^[a-zA-Z0-9 _\-\.,@()]+$', val):
                        reason = f"Value '{val}' contains invalid characters (only alphanumeric, space, _, -, ., ,, @, () allowed)"
                        invalid_rows.append((idx, val, reason))
                elif expected_type == "time":
                    try:
                        datetime.strptime(val, "%H:%M")
                    except ValueError:
                        reason = f"Value '{val}' is not a valid time (expected format HH:mm)"
                        invalid_rows.append((idx, val, reason))


            if invalid_rows:
                errors.append((col, invalid_rows))

        return errors
