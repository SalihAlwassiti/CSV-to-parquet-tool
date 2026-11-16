import csv
import gc
import io
import os
import tarfile
import threading
import time
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import zipfile
import pandas as pd
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

try:
    from tkinterdnd2 import DND_FILES, TkinterDnD

    HAS_DND = True
except ImportError:
    HAS_DND = False

BATCH_ROW_LIMIT = 250_000

DELIMITER_OPTIONS = [
    "Auto",
    ", (Comma)",
    "; (Semicolon)",
    "\\t (Tab)",
    "| (Pipe)",
]

BEST_DATA_TYPES = [
    "--- Text & Categorical ---",
    "string[pyarrow]",
    "category",
    "--- Signed Integers ---",
    "int8[pyarrow]",
    "int16[pyarrow]",
    "int32[pyarrow]",
    "int64[pyarrow]",
    "--- Unsigned Integers ---",
    "uint8[pyarrow]",
    "uint16[pyarrow]",
    "uint32[pyarrow]",
    "uint64[pyarrow]",
    "--- Floating Point ---",
    "float32[pyarrow]",
    "float64[pyarrow]",
    "--- Boolean ---",
    "bool[pyarrow]",
    "--- Date & Time ---",
    "date32[pyarrow]",
    "timestamp[s][pyarrow]",
    "timestamp[ms][pyarrow]",
    "timestamp[us][pyarrow]",
    "timestamp[ns][pyarrow]",
    "--- Binary ---",
    "binary[pyarrow]",
]

selected_source = None
sampled_df = None
row_widget_refs = []  # Ordered list of row dicts (single source of truth for order & controls)
cancel_event = threading.Event()
is_converting = False


class NonSeekableStreamWrapper(io.RawIOBase):
    """Adapts tarfile._Stream so pandas/pyarrow see a valid non-seekable binary stream without buffering in RAM."""

    def __init__(self, stream):
        self._stream = stream

    def readable(self):
        return True

    def seekable(self):
        return False

    def read(self, size=-1):
        return self._stream.read(size)

    def readinto(self, b):
        data = self._stream.read(len(b))
        n = len(data)
        b[:n] = data
        return n


def format_bytes(num_bytes):
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if num_bytes < 1024.0 or unit == "TB":
            return f"{num_bytes:.1f} {unit}" if unit != "B" else f"{int(num_bytes)} B"
        num_bytes /= 1024.0
    return f"{num_bytes:.1f} TB"


def format_duration(seconds):
    if seconds < 60:
        return f"{seconds:.2f}s"
    mins, secs = divmod(seconds, 60)
    return f"{int(mins)}m {secs:.1f}s"


def format_sample_preview(series, max_items=3, max_total_len=34):
    """Build a compact preview of up to 3 distinct non-null sample values."""
    if series is None:
        return ""
    non_null = series.dropna()
    if non_null.empty:
        return "(all null)"

    seen = []
    for val in non_null:
        s = str(val).replace("\r", " ").replace("\n", " ")
        display_s = "''" if s == "" else s
        if display_s not in seen:
            seen.append(display_s)
        if len(seen) >= max_items:
            break

    preview = ", ".join(seen)
    if len(preview) > max_total_len:
        preview = preview[: max_total_len - 1] + "…"
    return preview


def parse_delimiter_choice(raw_choice):
    choice = raw_choice.strip()
    if not choice or choice.lower() == "auto":
        return None
    if choice.startswith(","):
        return ","
    if choice.startswith(";"):
        return ";"
    if choice.startswith("\\t") or choice == "\t":
        return "\t"
    if choice.startswith("|"):
        return "|"
    return choice[0]


def detect_delimiter(sample_bytes):
    """Sniff the CSV delimiter across multiple lines of sample text, falling back to comma."""
    try:
        text = sample_bytes.decode("utf-8", errors="replace")
        sample_lines = [line for line in text.splitlines()[:50] if line.strip()]
        if not sample_lines:
            return ","
        dialect = csv.Sniffer().sniff("\n".join(sample_lines), delimiters=",;\t|")
        return dialect.delimiter
    except Exception:
        return ","


def _read_sample_lines(file_obj, sample_rows):
    lines = []
    for _ in range(sample_rows + 1):
        line = file_obj.readline()
        if not line:
            break
        lines.append(line)
    return b"".join(lines)


def _is_valid_csv_name(name):
    norm = name.replace("\\", "/")
    base = os.path.basename(norm)
    if not base.lower().endswith(".csv"):
        return False
    if base.startswith(".") or base.startswith("._"):
        return False
    if "__MACOSX" in norm.split("/"):
        return False
    return True


def _deduplicate_headers(header):
    """Keep the first occurrence of a column name as-is and append _1, _2, ... to subsequent duplicates."""
    seen = set()
    counts = {}
    resolved = []
    for col in header:
        if col not in seen:
            seen.add(col)
            counts[col] = 0
            resolved.append(col)
        else:
            idx = counts.get(col, 0) + 1
            candidate = f"{col}_{idx}"
            while candidate in seen:
                idx += 1
                candidate = f"{col}_{idx}"
            counts[col] = idx
            seen.add(candidate)
            resolved.append(candidate)
    return resolved


def _read_raw_csv_header(stream, delimiter):
    """Read the first CSV header row without advancing the stream past the header."""
    pos = stream.tell() if (hasattr(stream, "seekable") and stream.seekable()) else None
    buf = bytearray()
    while True:
        chunk = stream.read(4096)
        if not chunk:
            break
        buf.extend(chunk)
        text = buf.decode("utf-8-sig", errors="replace")
        try:
            reader = csv.reader(io.StringIO(text), delimiter=delimiter)
            header = next(reader, None)
            if header is not None and ("\n" in text or "\r" in text):
                if pos is not None:
                    stream.seek(pos)
                return header
        except csv.Error:
            # Incomplete quoted multiline header; keep reading
            continue

    if pos is not None:
        stream.seek(pos)
    if not buf:
        return []
    text = buf.decode("utf-8-sig", errors="replace")
    return next(csv.reader(io.StringIO(text), delimiter=delimiter), [])


def inspect_first_csv(source_path, delimiter_choice="Auto", sample_rows=250):
    """Extract columns, a small sample, and resolved delimiter from a CSV, folder, .zip, or .tar.gz."""
    sample_bytes = b""
    lower = source_path.lower()

    if os.path.isdir(source_path):
        for dirpath, dirnames, filenames in os.walk(source_path):
            dirnames.sort()
            for fname in sorted(filenames):
                full_path = os.path.join(dirpath, fname)
                if _is_valid_csv_name(full_path):
                    with open(full_path, "rb") as f:
                        sample_bytes = _read_sample_lines(f, sample_rows)
                    break
            if sample_bytes:
                break

    elif lower.endswith(".csv"):
        with open(source_path, "rb") as f:
            sample_bytes = _read_sample_lines(f, sample_rows)

    elif lower.endswith(".zip"):
        with zipfile.ZipFile(source_path, "r") as zf:
            for info in zf.infolist():
                if not info.is_dir() and _is_valid_csv_name(info.filename):
                    with zf.open(info, "r") as f:
                        sample_bytes = _read_sample_lines(f, sample_rows)
                    break

    else:
        with tarfile.open(source_path, "r|gz") as tar:
            for member in tar:
                if member.isfile() and _is_valid_csv_name(member.name):
                    f = tar.extractfile(member)
                    if f is not None:
                        sample_bytes = _read_sample_lines(f, sample_rows)
                        break

    if not sample_bytes:
        return [], None, ","

    resolved_delim = parse_delimiter_choice(delimiter_choice) or detect_delimiter(sample_bytes)
    raw_header = _read_raw_csv_header(io.BytesIO(sample_bytes), resolved_delim)
    dedup_cols = _deduplicate_headers(raw_header)

    try:
        df = pd.read_csv(
            io.BytesIO(sample_bytes),
            sep=resolved_delim,
            header=0,
            names=dedup_cols,
            nrows=sample_rows,
            dtype_backend="pyarrow",
        )
    except Exception:
        df = pd.read_csv(
            io.BytesIO(sample_bytes),
            sep=resolved_delim,
            header=0,
            names=dedup_cols,
            nrows=sample_rows,
        )
    return list(df.columns), df, resolved_delim


def infer_optimal_pyarrow_type(series, allow_narrow=False):
    """Suggest a safe wide PyArrow dtype by default, or narrow widths when explicitly requested."""
    if series is None:
        return "string[pyarrow]"

    non_null = series.dropna()
    if non_null.empty:
        return "string[pyarrow]"

    dtype_str = str(series.dtype).lower()

    if "bool" in dtype_str:
        return "bool[pyarrow]"
    if "int" in dtype_str:
        if not allow_narrow:
            return "int64[pyarrow]"
        min_val, max_val = non_null.min(), non_null.max()
        if min_val >= 0:
            if max_val <= 255:
                return "uint8[pyarrow]"
            if max_val <= 65535:
                return "uint16[pyarrow]"
            if max_val <= 4294967295:
                return "uint32[pyarrow]"
            return "uint64[pyarrow]"
        if -128 <= min_val and max_val <= 127:
            return "int8[pyarrow]"
        if -32768 <= min_val and max_val <= 32767:
            return "int16[pyarrow]"
        if -2147483648 <= min_val and max_val <= 2147483647:
            return "int32[pyarrow]"
        return "int64[pyarrow]"
    if "float" in dtype_str or "double" in dtype_str:
        return "float64[pyarrow]"
    if "date32" in dtype_str:
        return "date32[pyarrow]"
    if "timestamp" in dtype_str or "datetime" in dtype_str:
        return "timestamp[us][pyarrow]"

    if allow_narrow and len(non_null) >= 20:
        unique_ratio = non_null.nunique() / len(non_null)
        if unique_ratio <= 0.15 and non_null.nunique() <= 50:
            return "category"

    return "string[pyarrow]"


def load_source(source_path):
    global selected_source, sampled_df
    if is_converting:
        return
    if not os.path.exists(source_path):
        messagebox.showerror("Not Found", f"Path does not exist:\n{source_path}")
        return

    try:
        cols, sample, resolved_delim = inspect_first_csv(source_path, delimiter_var.get())
    except Exception as e:
        messagebox.showerror("Read Error", f"Could not read source:\n{e}")
        return

    if not cols:
        messagebox.showerror("No CSVs Found", "No valid CSV files were found in the selected file or folder.")
        return

    selected_source = source_path
    sampled_df = sample
    display_name = os.path.basename(os.path.normpath(source_path))
    if os.path.isdir(source_path):
        display_name = f"[Folder] {display_name}"

    delim_display = "\\t" if resolved_delim == "\t" else resolved_delim
    file_label.config(text=display_name, fg="#111111")
    status_label.config(
        text=f"Loaded {len(cols)} columns (delimiter: '{delim_display}').",
        fg="#555555",
    )
    progress_bar.stop()
    progress_bar.config(mode="determinate", value=0)

    search_var.set("")
    build_column_rows(cols)
    set_controls_state("normal")


def on_delimiter_change(event=None):
    if selected_source and not is_converting:
        load_source(selected_source)


def on_drop(event):
    if is_converting or str(upload_btn["state"]) == "disabled":
        return
    paths = root.tk.splitlist(event.data)
    if paths:
        dropped_path = paths[0].strip()
        load_source(dropped_path)


def upload_file():
    if is_converting:
        return
    file_path = filedialog.askopenfilename(
        title="Select a CSV, TAR.GZ, or ZIP file",
        filetypes=[
            ("Supported Data Files", "*.csv *.tar.gz *.tgz *.zip"),
            ("CSV Files", "*.csv"),
            ("TAR.GZ Archives", "*.tar.gz *.tgz"),
            ("ZIP Archives", "*.zip"),
            ("All Files", "*.*"),
        ],
    )
    if file_path:
        load_source(file_path)


def upload_folder():
    if is_converting:
        return
    folder_path = filedialog.askdirectory(title="Select a folder containing CSV files")
    if folder_path:
        load_source(folder_path)


def set_controls_state(state):
    """Enable or disable both toolbar controls and every per-row widget."""
    convert_btn.config(state=state)
    select_all_btn.config(state=state)
    deselect_all_btn.config(state=state)
    suggest_btn.config(state=state)
    search_entry.config(state=state)
    clear_search_btn.config(state=state)

    if state == "disabled":
        for row in row_widget_refs:
            row["up_btn"].config(state="disabled")
            row["down_btn"].config(state="disabled")
            row["keep_chk"].config(state="disabled")
            row["rename_entry"].config(state="disabled")
            row["type_combo"].config(state="disabled")
    else:
        for row in row_widget_refs:
            row["keep_chk"].config(state="normal")
            if row["keep_var"].get():
                row["rename_entry"].config(state="normal")
                row["type_combo"].config(state="readonly")
            else:
                row["rename_entry"].config(state="disabled")
                row["type_combo"].config(state="disabled")
        render_visible_rows(reset_scroll=False)


def set_all_keep(keep_state):
    if is_converting:
        return
    for row in row_widget_refs:
        row["keep_var"].set(keep_state)
        if keep_state:
            row["col_label"].config(fg="#111111")
            row["sample_label"].config(fg="#555555")
            row["rename_entry"].config(state="normal")
            row["type_combo"].config(state="readonly")
        else:
            row["col_label"].config(fg="#999999")
            row["sample_label"].config(fg="#b0b0b0")
            row["rename_entry"].config(state="disabled")
            row["type_combo"].config(state="disabled")


def suggest_types():
    """Explicit user action: infer compact narrow widths (uint8, int16, category) from the sample."""
    if sampled_df is None or is_converting:
        return
    for row in row_widget_refs:
        col = row["col"]
        if col in sampled_df.columns:
            suggested = infer_optimal_pyarrow_type(sampled_df[col], allow_narrow=True)
            row["last_valid_type"] = suggested
            row["type_combo"].set(suggested)


def clear_search():
    if is_converting:
        return
    search_var.set("")
    search_entry.focus_set()


def _row_matches_query(row, query):
    if not query:
        return True
    new_name = row["rename_entry"].get().strip().lower()
    sample_text = row["sample_text"].lower()
    return query in row["col"].lower() or (new_name and query in new_name) or (sample_text and query in sample_text)


def move_row(target_row, direction):
    """Move a column up (-1) or down (+1) in the output schema order."""
    if is_converting:
        return
    query = search_var.get().strip().lower()
    visible_rows = [r for r in row_widget_refs if _row_matches_query(r, query)]
    if target_row not in visible_rows:
        return

    vis_idx = visible_rows.index(target_row)
    swap_vis_idx = vis_idx + direction
    if not (0 <= swap_vis_idx < len(visible_rows)):
        return

    idx_a = row_widget_refs.index(visible_rows[vis_idx])
    idx_b = row_widget_refs.index(visible_rows[swap_vis_idx])
    row_widget_refs[idx_a], row_widget_refs[idx_b] = row_widget_refs[idx_b], row_widget_refs[idx_a]

    render_visible_rows(reset_scroll=False)


def render_visible_rows(reset_scroll=True):
    """Grid visible rows in their current order without destroying widget state."""
    if not row_widget_refs:
        search_count_label.config(text="")
        return

    query = search_var.get().strip().lower()
    visible_rows = []

    for row in row_widget_refs:
        if _row_matches_query(row, query):
            visible_rows.append(row)
        else:
            row["order_frame"].grid_remove()
            row["keep_chk"].grid_remove()
            row["col_label"].grid_remove()
            row["sample_label"].grid_remove()
            row["rename_entry"].grid_remove()
            row["type_combo"].grid_remove()

    total_visible = len(visible_rows)
    for idx, row in enumerate(visible_rows):
        grid_r = idx + 1
        row["order_frame"].grid(row=grid_r, column=0, padx=(2, 4), pady=2)
        row["keep_chk"].grid(row=grid_r, column=1, padx=(2, 4), pady=3)
        row["col_label"].grid(row=grid_r, column=2, padx=6, pady=3, sticky="w")
        row["sample_label"].grid(row=grid_r, column=3, padx=6, pady=3, sticky="w")
        row["rename_entry"].grid(row=grid_r, column=4, padx=6, pady=3, sticky="w")
        row["type_combo"].grid(row=grid_r, column=5, padx=6, pady=3, sticky="w")

        if is_converting:
            row["up_btn"].config(state="disabled")
            row["down_btn"].config(state="disabled")
        else:
            row["up_btn"].config(state="disabled" if idx == 0 else "normal")
            row["down_btn"].config(state="disabled" if idx == total_visible - 1 else "normal")

    total_count = len(row_widget_refs)
    if query:
        search_count_label.config(text=f"Showing {total_visible} of {total_count} columns")
    else:
        search_count_label.config(text=f"{total_count} columns")

    rows_frame.update_idletasks()
    canvas.configure(scrollregion=canvas.bbox("all"))
    if reset_scroll:
        canvas.yview_moveto(0)


def filter_column_rows(*args):
    render_visible_rows(reset_scroll=True)


def build_column_rows(columns):
    for widget in rows_frame.winfo_children():
        widget.destroy()
    row_widget_refs.clear()

    tk.Label(rows_frame, text="Order", font=("Segoe UI", 9, "bold"), width=6, anchor="center").grid(
        row=0, column=0, padx=(2, 4), pady=(0, 6)
    )
    tk.Label(rows_frame, text="Keep", font=("Segoe UI", 9, "bold"), width=5, anchor="center").grid(
        row=0, column=1, padx=(2, 4), pady=(0, 6)
    )
    tk.Label(rows_frame, text="Column", font=("Segoe UI", 9, "bold"), width=20, anchor="w").grid(
        row=0, column=2, padx=6, pady=(0, 6), sticky="w"
    )
    tk.Label(rows_frame, text="Sample Values", font=("Segoe UI", 9, "bold"), width=30, anchor="w").grid(
        row=0, column=3, padx=6, pady=(0, 6), sticky="w"
    )
    tk.Label(rows_frame, text="New Name (blank = keep)", font=("Segoe UI", 9, "bold"), width=21, anchor="w").grid(
        row=0, column=4, padx=6, pady=(0, 6), sticky="w"
    )
    tk.Label(rows_frame, text="Data Type", font=("Segoe UI", 9, "bold"), width=23, anchor="w").grid(
        row=0, column=5, padx=6, pady=(0, 6), sticky="w"
    )

    for col in columns:
        keep_var = tk.BooleanVar(value=True)
        sample_series = sampled_df[col] if (sampled_df is not None and col in sampled_df.columns) else None
        sample_text = format_sample_preview(sample_series)
        initial_type = infer_optimal_pyarrow_type(sample_series, allow_narrow=False)

        row_item = {
            "col": col,
            "keep_var": keep_var,
            "sample_text": sample_text,
            "last_valid_type": initial_type,
        }

        order_frame = tk.Frame(rows_frame)
        up_btn = tk.Button(
            order_frame,
            text="▲",
            font=("Segoe UI", 7),
            width=2,
            padx=0,
            pady=0,
            command=lambda r=row_item: move_row(r, -1),
        )
        up_btn.pack(side="left", padx=(0, 1))

        down_btn = tk.Button(
            order_frame,
            text="▼",
            font=("Segoe UI", 7),
            width=2,
            padx=0,
            pady=0,
            command=lambda r=row_item: move_row(r, 1),
        )
        down_btn.pack(side="left")

        col_label = tk.Label(rows_frame, text=col, font=("Segoe UI", 9), width=20, anchor="w", fg="#111111")

        sample_label = tk.Label(
            rows_frame,
            text=sample_text,
            font=("Consolas", 8),
            width=34,
            anchor="w",
            fg="#555555",
        )

        rename_entry = tk.Entry(rows_frame, font=("Consolas", 9), width=21)

        type_combo = ttk.Combobox(
            rows_frame,
            values=BEST_DATA_TYPES,
            font=("Consolas", 9),
            width=22,
            state="readonly",
        )
        type_combo.set(initial_type)

        def on_combo_select(event, r=row_item, cb=type_combo):
            chosen = cb.get().strip()
            if not chosen or chosen.startswith("---"):
                cb.set(r["last_valid_type"])
            else:
                r["last_valid_type"] = chosen

        type_combo.bind("<<ComboboxSelected>>", on_combo_select)

        def on_toggle(
            var=keep_var,
            lbl=col_label,
            s_lbl=sample_label,
            r_ent=rename_entry,
            t_cb=type_combo,
        ):
            if is_converting:
                return
            if var.get():
                lbl.config(fg="#111111")
                s_lbl.config(fg="#555555")
                r_ent.config(state="normal")
                t_cb.config(state="readonly")
            else:
                lbl.config(fg="#999999")
                s_lbl.config(fg="#b0b0b0")
                r_ent.config(state="disabled")
                t_cb.config(state="disabled")

        keep_chk = tk.Checkbutton(rows_frame, variable=keep_var, command=on_toggle)

        row_item.update(
            {
                "order_frame": order_frame,
                "up_btn": up_btn,
                "down_btn": down_btn,
                "keep_chk": keep_chk,
                "col_label": col_label,
                "sample_label": sample_label,
                "rename_entry": rename_entry,
                "type_combo": type_combo,
            }
        )
        row_widget_refs.append(row_item)

    render_visible_rows(reset_scroll=True)


def update_progress_ui(files_done, rows_done, pct):
    progress_bar["value"] = pct
    status_label.config(
        text=f"Streaming: {files_done:,} CSV(s) ({rows_done:,} rows) — {pct:.1f}%",
        fg="#1f6aa5",
    )


def start_conversion():
    global is_converting
    if not selected_source or is_converting:
        return

    included_cols = []
    rename_map = {}  # orig_col -> new_name
    conversions = {}  # orig_col -> raw_type
    seen_final_names = set()

    for row in row_widget_refs:
        if not row["keep_var"].get():
            continue

        col = row["col"]
        raw_type = row["type_combo"].get().strip()
        if not raw_type or raw_type.startswith("---"):
            messagebox.showwarning(
                "Missing Data Type",
                f"Please select a valid Data Type for column '{col}'. Blank data types are not allowed.",
            )
            return

        new_name = row["rename_entry"].get().strip()
        final_name = new_name if new_name else col
        if final_name in seen_final_names:
            messagebox.showwarning(
                "Duplicate Column Name",
                f"Multiple kept columns resolve to '{final_name}'.\nPlease give each column a unique name.",
            )
            return
        seen_final_names.add(final_name)

        included_cols.append(col)
        if new_name and new_name != col:
            rename_map[col] = new_name
        conversions[col] = raw_type

    if not included_cols:
        messagebox.showwarning("No Columns Selected", "Check at least one column to include in the Parquet file.")
        return

    parquet_path = get_output_parquet_path(selected_source)
    if os.path.exists(parquet_path):
        overwrite = messagebox.askyesno(
            "Overwrite Existing File?",
            f"'{os.path.basename(parquet_path)}' already exists.\nDo you want to overwrite it?",
        )
        if not overwrite:
            return
        try:
            with open(parquet_path, "ab"):
                pass
        except OSError as lock_err:
            messagebox.showerror(
                "File Locked",
                f"Cannot write to '{os.path.basename(parquet_path)}' because it is locked or in use:\n{lock_err}",
            )
            return

    use_pyarrow_engine = use_pyarrow_engine_var.get()
    delimiter_choice = delimiter_var.get()
    engine_text = "PyArrow engine" if use_pyarrow_engine else "pandas C engine"

    is_converting = True
    cancel_event.clear()
    upload_btn.config(state="disabled")
    folder_btn.config(state="disabled")
    delimiter_combo.config(state="disabled")
    pyarrow_engine_toggle.config(state="disabled")
    set_controls_state("disabled")
    cancel_btn.config(state="normal")

    progress_bar.stop()
    progress_bar.config(mode="determinate", value=0)
    status_label.config(text=f"Streaming source ({engine_text})...", fg="#1f6aa5")

    threading.Thread(
        target=convert_to_parquet_streaming,
        args=(
            selected_source,
            parquet_path,
            included_cols,
            rename_map,
            conversions,
            use_pyarrow_engine,
            delimiter_choice,
        ),
        daemon=True,
    ).start()


def request_cancel():
    cancel_event.set()
    cancel_btn.config(state="disabled")
    status_label.config(text="Cancelling after current chunk...", fg="#c0392b")


def on_window_close():
    if is_converting:
        confirm = messagebox.askyesno(
            "Conversion In Progress",
            "A conversion is currently running. Closing now will cancel it and delete the incomplete Parquet file.\n\nExit anyway?",
        )
        if not confirm:
            return
        cancel_event.set()
        root.after(150, _wait_and_destroy)
        return
    root.destroy()


def _wait_and_destroy():
    if is_converting:
        root.after(100, _wait_and_destroy)
    else:
        root.destroy()


def apply_conversions(df, conversions):
    """Apply user-specified conversions vectorized with per-column error context."""
    for col, raw_type in conversions.items():
        if col not in df.columns:
            continue

        lower_type = raw_type.lower()

        try:
            # 1. Category conversion
            if lower_type == "category":
                df[col] = df[col].astype("string[pyarrow]").astype("category")

            # 2. Date / Datetime parsing
            elif (
                lower_type.startswith(("date32", "timestamp[", "datetime64"))
                or lower_type in ("date", "datetime", "timestamp")
            ):
                parsed = pd.to_datetime(df[col], format="mixed")
                if getattr(parsed.dt, "tz", None) is not None:
                    parsed = parsed.dt.tz_convert("UTC").dt.tz_localize(None)

                if lower_type.startswith("date32") or lower_type == "date":
                    df[col] = parsed.dt.floor("D").astype("date32[pyarrow]")
                elif lower_type.startswith("timestamp[s"):
                    df[col] = parsed.dt.floor("s").astype(raw_type)
                elif lower_type.startswith("timestamp[ms"):
                    df[col] = parsed.dt.floor("ms").astype(raw_type)
                elif lower_type.startswith(("timestamp[", "datetime64")):
                    df[col] = parsed.astype(raw_type)
                else:
                    df[col] = parsed.astype("timestamp[us][pyarrow]")

            # 3. Boolean conversion
            elif lower_type in ("bool", "boolean", "bool[pyarrow]"):
                s = df[col].astype("string[pyarrow]").str.strip().replace("", pd.NA).str.lower()
                mapped = s.map(
                    {
                        "true": True,
                        "false": False,
                        "1": True,
                        "0": False,
                        "1.0": True,
                        "0.0": False,
                        "yes": True,
                        "no": False,
                        "t": True,
                        "f": False,
                        "y": True,
                        "n": False,
                    }
                )
                invalid_mask = s.notna() & mapped.isna()
                if invalid_mask.any():
                    bad_samples = df.loc[invalid_mask, col].dropna().astype(str).unique()[:5].tolist()
                    raise ValueError(f"Unrecognized boolean value(s): {bad_samples}")

                target_bool = "bool[pyarrow]" if lower_type == "bool" and mapped.isna().any() else raw_type
                df[col] = mapped.astype(target_bool)

            # 4. Binary conversion
            elif lower_type in ("binary", "binary[pyarrow]"):
                df[col] = df[col].astype("string[pyarrow]").astype("binary[pyarrow]")

            # 5. All other specified dtypes
            else:
                df[col] = df[col].astype(raw_type)

        except Exception as col_err:
            raise ValueError(f"Column '{col}' failed conversion to '{raw_type}': {col_err}") from col_err


def normalize_arrow_table(table, conversions):
    """Ensure uniform Arrow column types across row groups."""
    fields = []
    columns = []
    for i, field in enumerate(table.schema):
        col_data = table.column(i)
        col_name = field.name
        user_type = conversions.get(col_name, "").lower()

        if pa.types.is_null(field.type) and not user_type:
            new_type = pa.string()
            col_data = col_data.cast(new_type)
            field = pa.field(col_name, new_type)

        elif pa.types.is_dictionary(field.type):
            unified_dict = pa.dictionary(pa.int32(), pa.string())
            if not pa.types.is_string(field.type.value_type):
                col_data = col_data.cast(pa.string()).dictionary_encode()
            col_data = col_data.cast(unified_dict)
            field = pa.field(col_name, unified_dict)

        fields.append(field)
        columns.append(col_data)

    return pa.Table.from_arrays(columns, schema=pa.schema(fields))


def get_output_parquet_path(source_path):
    if os.path.isdir(source_path):
        return os.path.normpath(source_path) + ".parquet"

    lower = source_path.lower()
    for ext in (".tar.gz", ".tgz", ".zip", ".csv"):
        if lower.endswith(ext):
            return source_path[: -len(ext)] + ".parquet"
    return os.path.splitext(source_path)[0] + ".parquet"


def iter_csv_sources(source_path):
    """Yield (source_name, stream_obj, total_input_bytes, progress_pct_fn) without loading whole files into RAM."""
    lower = source_path.lower()

    if os.path.isdir(source_path):
        csv_files = []
        for dirpath, dirnames, filenames in os.walk(source_path):
            dirnames.sort()
            for fname in sorted(filenames):
                full_path = os.path.join(dirpath, fname)
                if _is_valid_csv_name(full_path):
                    csv_files.append(full_path)

        raw_total = sum(os.path.getsize(p) for p in csv_files)
        total_bytes = max(raw_total, 1)
        completed_bytes = 0

        for fp in csv_files:
            if cancel_event.is_set():
                break
            file_size = os.path.getsize(fp)
            rel_name = os.path.relpath(fp, source_path)
            with open(fp, "rb") as f:
                yield (
                    rel_name,
                    f,
                    raw_total,
                    lambda f_ref=f, base=completed_bytes: min(
                        ((base + f_ref.tell()) / total_bytes) * 100.0, 99.9
                    ),
                )
            completed_bytes += file_size

    elif lower.endswith(".csv"):
        raw_total = os.path.getsize(source_path)
        total_bytes = max(raw_total, 1)
        with open(source_path, "rb") as f:
            yield (
                os.path.basename(source_path),
                f,
                raw_total,
                lambda f_ref=f: min((f_ref.tell() / total_bytes) * 100.0, 99.9),
            )

    elif lower.endswith(".zip"):
        raw_total = os.path.getsize(source_path)
        total_bytes = max(raw_total, 1)
        with open(source_path, "rb") as raw_file:
            with zipfile.ZipFile(raw_file, "r") as zf:
                for info in zf.infolist():
                    if cancel_event.is_set():
                        break
                    if not info.is_dir() and _is_valid_csv_name(info.filename):
                        with zf.open(info, "r") as f:
                            yield (
                                info.filename,
                                f,
                                raw_total,
                                lambda: min((raw_file.tell() / total_bytes) * 100.0, 99.9),
                            )

    else:
        raw_total = os.path.getsize(source_path)
        total_bytes = max(raw_total, 1)
        with open(source_path, "rb") as raw_file:
            with tarfile.open(fileobj=raw_file, mode="r|gz") as tar:
                for member in tar:
                    if cancel_event.is_set():
                        break
                    if member.isfile() and _is_valid_csv_name(member.name):
                        f = tar.extractfile(member)
                        if f is not None:
                            wrapped = io.BufferedReader(NonSeekableStreamWrapper(f), buffer_size=1024 * 1024)
                            try:
                                yield (
                                    member.name,
                                    wrapped,
                                    raw_total,
                                    lambda: min((raw_file.tell() / total_bytes) * 100.0, 99.9),
                                )
                            finally:
                                wrapped.close()
                    tar.members.clear()


def iter_csv_chunks(
    stream,
    included_cols,
    included_set,
    conversions,
    use_pyarrow_engine,
    delimiter,
):
    """Stream DataFrame chunks from a CSV file-like object using either PyArrow or pandas C engine."""
    raw_header = _read_raw_csv_header(stream, delimiter)
    if not raw_header:
        return
    dedup_cols = _deduplicate_headers(raw_header)
    has_duplicate_headers = len(dedup_cols) != len(set(raw_header))

    if use_pyarrow_engine:
        # skip_rows=1 + explicit column_names guarantees duplicate CSV headers map cleanly to id, id_1, id_2...
        read_opts = pacsv.ReadOptions(
            use_threads=True,
            block_size=16 * 1024 * 1024,
            skip_rows=1,
            column_names=dedup_cols,
        )
        parse_opts = pacsv.ParseOptions(delimiter=delimiter)
        col_types = {col: pa.string() for col in dedup_cols if col in conversions}
        present_included = [col for col in included_cols if col in dedup_cols]
        if not present_included:
            return

        conv_opts = pacsv.ConvertOptions(
            include_columns=present_included,
            column_types=col_types,
            strings_can_be_null=True,
        )

        try:
            reader = pacsv.open_csv(
                stream,
                read_options=read_opts,
                parse_options=parse_opts,
                convert_options=conv_opts,
            )
        except pa.ArrowInvalid as e:
            if "Empty CSV file" in str(e):
                return
            raise

        for batch in reader:
            if cancel_event.is_set():
                break

            chunk = batch.to_pandas()
            if list(chunk.columns) != included_cols:
                chunk = chunk.reindex(columns=included_cols)

            if len(chunk) > BATCH_ROW_LIMIT:
                for start in range(0, len(chunk), BATCH_ROW_LIMIT):
                    if cancel_event.is_set():
                        break
                    yield chunk.iloc[start : start + BATCH_ROW_LIMIT]
            else:
                yield chunk

    else:
        read_dtypes = {col: str for col in conversions}
        if has_duplicate_headers:
            # Pass deduplicated names explicitly so pandas reads duplicate header columns accurately
            read_kwargs = {
                "sep": delimiter,
                "header": 0,
                "names": dedup_cols,
                "usecols": lambda c: c in included_set,
                "dtype": read_dtypes,
                "chunksize": BATCH_ROW_LIMIT,
            }
        else:
            read_kwargs = {
                "sep": delimiter,
                "usecols": lambda c: c in included_set,
                "dtype": read_dtypes,
                "chunksize": BATCH_ROW_LIMIT,
            }

        try:
            for chunk in pd.read_csv(stream, **read_kwargs):
                if cancel_event.is_set():
                    break
                if list(chunk.columns) != included_cols:
                    chunk = chunk.reindex(columns=included_cols)
                yield chunk
        except pd.errors.EmptyDataError:
            return


def convert_to_parquet_streaming(
    source_path,
    parquet_path,
    included_cols,
    rename_map,
    conversions,
    use_pyarrow_engine,
    delimiter_choice,
):
    start_time = time.perf_counter()
    included_set = set(included_cols)
    renamed_conversions = {rename_map.get(k, k): v for k, v in conversions.items()}

    resolved_delim = parse_delimiter_choice(delimiter_choice)
    if resolved_delim is None:
        _, _, resolved_delim = inspect_first_csv(source_path, "Auto")

    writer = None
    batch_dfs = []
    batch_rows = 0
    files_done = 0
    row_count = 0
    input_bytes = 0
    last_ui_update = 0.0
    current_source_name = ""

    def flush_batch():
        nonlocal writer, batch_dfs, batch_rows
        if not batch_dfs:
            return

        batch_df = pd.concat(batch_dfs, ignore_index=True) if len(batch_dfs) > 1 else batch_dfs[0]
        batch_dfs.clear()
        batch_rows = 0

        if conversions:
            apply_conversions(batch_df, conversions)

        if rename_map:
            batch_df.rename(columns=rename_map, inplace=True)

        table = pa.Table.from_pandas(batch_df, preserve_index=False)
        table = normalize_arrow_table(table, renamed_conversions)
        del batch_df

        if writer is None:
            writer = pq.ParquetWriter(parquet_path, table.schema, compression="zstd")
        else:
            try:
                table = table.cast(writer.schema)
            except Exception as cast_err:
                raise ValueError(
                    f"Schema mismatch near '{os.path.basename(current_source_name)}'. "
                    f"Ensure Data Types match across files.\nDetails: {cast_err}"
                ) from cast_err

        writer.write_table(table)
        del table

    try:
        for source_name, stream, total_in_bytes, get_pct in iter_csv_sources(source_path):
            if cancel_event.is_set():
                break

            input_bytes = total_in_bytes
            current_source_name = source_name
            for chunk in iter_csv_chunks(
                stream,
                included_cols,
                included_set,
                conversions,
                use_pyarrow_engine,
                resolved_delim,
            ):
                if cancel_event.is_set():
                    break

                if len(chunk) > 0:
                    batch_dfs.append(chunk)
                    batch_rows += len(chunk)
                    row_count += len(chunk)

                if batch_rows >= BATCH_ROW_LIMIT:
                    flush_batch()

                now = time.perf_counter()
                if now - last_ui_update >= 0.1:
                    root.after(0, update_progress_ui, files_done, row_count, get_pct())
                    last_ui_update = now

            if not cancel_event.is_set():
                files_done += 1
                now = time.perf_counter()
                if now - last_ui_update >= 0.1:
                    root.after(0, update_progress_ui, files_done, row_count, get_pct())
                    last_ui_update = now

        if not cancel_event.is_set():
            flush_batch()

        if writer is not None:
            writer.close()
            writer = None

        if cancel_event.is_set():
            if os.path.exists(parquet_path):
                try:
                    os.remove(parquet_path)
                except Exception:
                    pass
            root.after(0, finish_ui, "Conversion cancelled.", True)
            return

        if row_count == 0:
            root.after(0, finish_ui, "No CSV rows found in selected source.", True)
            return

        elapsed_str = format_duration(time.perf_counter() - start_time)
        output_bytes = os.path.getsize(parquet_path)

        if input_bytes > 0:
            ratio_pct = (1.0 - (output_bytes / input_bytes)) * 100.0
            ratio_label = f"{ratio_pct:.1f}% smaller" if ratio_pct >= 0 else f"{abs(ratio_pct):.1f}% larger"
            size_summary = f"{format_bytes(input_bytes)} -> {format_bytes(output_bytes)}, {ratio_label}"
        else:
            size_summary = format_bytes(output_bytes)

        msg = (
            f"Done in {elapsed_str}! {os.path.basename(parquet_path)} "
            f"({size_summary}) — {files_done:,} CSV(s), {row_count:,} rows"
        )
        root.after(0, finish_ui, msg, False)

    except Exception as e:
        if writer is not None:
            try:
                writer.close()
            except Exception:
                pass
        if os.path.exists(parquet_path):
            try:
                os.remove(parquet_path)
            except Exception:
                pass
        err_msg = f"Error: {e}"
        root.after(0, finish_ui, err_msg, True)
    finally:
        gc.collect()
        pa.default_memory_pool().release_unused()


def finish_ui(message, is_error=False):
    global is_converting
    is_converting = False
    if not root.winfo_exists():
        return

    upload_btn.config(state="normal")
    folder_btn.config(state="normal")
    delimiter_combo.config(state="normal")
    pyarrow_engine_toggle.config(state="normal")
    set_controls_state("normal")
    cancel_btn.config(state="disabled")

    progress_bar.stop()
    progress_bar.config(mode="determinate", value=0 if is_error else 100)
    status_label.config(text=message, fg="#c0392b" if is_error else "#27ae60")
    if is_error and message != "Conversion cancelled.":
        messagebox.showerror("Conversion Failed", message)


# --- Desktop Window ---
root = TkinterDnD.Tk() if HAS_DND else tk.Tk()
root.title("CSV / Archive / Folder to Parquet")
root.geometry("1120x580")
root.minsize(980, 440)
root.resizable(True, True)
root.protocol("WM_DELETE_WINDOW", on_window_close)

if HAS_DND:
    root.drop_target_register(DND_FILES)
    root.dnd_bind("<<Drop>>", on_drop)

# Top bar: Upload File + Select Folder + PyArrow Engine Toggle
top_frame = tk.Frame(root)
top_frame.pack(fill="x", padx=20, pady=(16, 8))

upload_btn = tk.Button(top_frame, text="Upload File", font=("Segoe UI", 10, "bold"), width=12, command=upload_file)
upload_btn.pack(side="left")

folder_btn = tk.Button(top_frame, text="Select Folder", font=("Segoe UI", 10, "bold"), width=13, command=upload_folder)
folder_btn.pack(side="left", padx=(6, 0))

use_pyarrow_engine_var = tk.BooleanVar(value=False)
pyarrow_engine_toggle = tk.Checkbutton(
    top_frame,
    text="Use PyArrow engine",
    variable=use_pyarrow_engine_var,
    font=("Segoe UI", 9, "bold"),
)
pyarrow_engine_toggle.pack(side="right")

prompt_hint = (
    "Drag & drop or select a .csv, .tar.gz, .zip, or folder"
    if HAS_DND
    else "Select a .csv, .tar.gz, .zip, or folder"
)
file_label = tk.Label(
    top_frame,
    text=prompt_hint,
    font=("Segoe UI", 10),
    fg="#555555",
)
file_label.pack(side="left", padx=12)

# Bulk action & delimiter toolbar
toolbar_frame = tk.Frame(root)
toolbar_frame.pack(fill="x", padx=20, pady=(0, 6))

select_all_btn = tk.Button(
    toolbar_frame, text="Select All", font=("Segoe UI", 8), state="disabled", command=lambda: set_all_keep(True)
)
select_all_btn.pack(side="left", padx=(0, 6))

deselect_all_btn = tk.Button(
    toolbar_frame, text="Deselect All", font=("Segoe UI", 8), state="disabled", command=lambda: set_all_keep(False)
)
deselect_all_btn.pack(side="left", padx=(0, 14))

suggest_btn = tk.Button(
    toolbar_frame, text="Auto-Suggest Narrow Types", font=("Segoe UI", 8, "bold"), state="disabled", command=suggest_types
)
suggest_btn.pack(side="left")

delimiter_var = tk.StringVar(value="Auto")
delimiter_combo = ttk.Combobox(
    toolbar_frame,
    textvariable=delimiter_var,
    values=DELIMITER_OPTIONS,
    font=("Segoe UI", 8),
    width=13,
)
delimiter_combo.pack(side="right")
delimiter_combo.bind("<<ComboboxSelected>>", on_delimiter_change)

tk.Label(toolbar_frame, text="Delimiter:", font=("Segoe UI", 8, "bold")).pack(side="right", padx=(12, 4))

# Column search bar
search_frame = tk.Frame(root)
search_frame.pack(fill="x", padx=20, pady=(0, 6))

tk.Label(search_frame, text="Filter columns:", font=("Segoe UI", 8, "bold")).pack(side="left", padx=(0, 6))

search_var = tk.StringVar()
search_var.trace_add("write", filter_column_rows)
search_entry = tk.Entry(search_frame, textvariable=search_var, font=("Segoe UI", 9), width=30, state="disabled")
search_entry.pack(side="left")
search_entry.bind("<Escape>", lambda e: clear_search())

clear_search_btn = tk.Button(
    search_frame, text="Clear", font=("Segoe UI", 8), state="disabled", command=clear_search
)
clear_search_btn.pack(side="left", padx=(6, 0))

search_count_label = tk.Label(search_frame, text="", font=("Segoe UI", 8), fg="#666666")
search_count_label.pack(side="left", padx=10)

# Scrollable column table
table_container = tk.Frame(root, bd=1, relief="sunken")
table_container.pack(fill="both", expand=True, padx=20, pady=4)

canvas = tk.Canvas(table_container, highlightthickness=0)
scrollbar = ttk.Scrollbar(table_container, orient="vertical", command=canvas.yview)
rows_frame = tk.Frame(canvas)

rows_frame.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
canvas.create_window((0, 0), window=rows_frame, anchor="nw")
canvas.configure(yscrollcommand=scrollbar.set)

canvas.pack(side="left", fill="both", expand=True, padx=8, pady=8)
scrollbar.pack(side="right", fill="y")


def _on_mousewheel(event):
    if event.num == 4:
        canvas.yview_scroll(-1, "units")
    elif event.num == 5:
        canvas.yview_scroll(1, "units")
    else:
        step = -1 if event.delta > 0 else 1
        canvas.yview_scroll(step, "units")


def _bind_mousewheel(event):
    root.bind_all("<MouseWheel>", _on_mousewheel)
    root.bind_all("<Button-4>", _on_mousewheel)
    root.bind_all("<Button-5>", _on_mousewheel)


def _unbind_mousewheel(event):
    root.unbind_all("<MouseWheel>")
    root.unbind_all("<Button-4>")
    root.unbind_all("<Button-5>")


canvas.bind("<Enter>", _bind_mousewheel)
canvas.bind("<Leave>", _unbind_mousewheel)

# Bottom action bar + progress bar
bottom_frame = tk.Frame(root)
bottom_frame.pack(fill="x", padx=20, pady=(10, 16))

action_row = tk.Frame(bottom_frame)
action_row.pack(fill="x")

convert_btn = tk.Button(
    action_row,
    text="Convert to Parquet",
    font=("Segoe UI", 10, "bold"),
    width=18,
    state="disabled",
    command=start_conversion,
)
convert_btn.pack(side="left")

cancel_btn = tk.Button(
    action_row,
    text="Cancel",
    font=("Segoe UI", 9),
    width=10,
    state="disabled",
    command=request_cancel,
)
cancel_btn.pack(side="left", padx=(8, 0))

status_label = tk.Label(action_row, text="", font=("Segoe UI", 9))
status_label.pack(side="left", padx=12)

progress_bar = ttk.Progressbar(bottom_frame, orient="horizontal", mode="determinate", maximum=100)
progress_bar.pack(fill="x", pady=(10, 0))

root.mainloop()