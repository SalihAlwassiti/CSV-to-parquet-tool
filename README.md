# CSV to Parquet Studio

A lightweight desktop GUI for streaming large `.csv` files, folders, `.zip`, and `.tar.gz` archives into compressed Apache Parquet (`.parquet`) files—with visual schema control, strict type enforcement, and constant-memory streaming.

---

## Key Features

* **Multi-Source Streaming:** Convert single `.csv` files, nested folders of CSVs, `.zip` archives, or `.tar.gz` / `.tgz` archives in 250,000-row batches without loading entire datasets into RAM.
* **Dual Parsing Engines:** Switch between the **pandas C engine** (default) and the multi-threaded **PyArrow CSV engine** with a single toggle.
* **Interactive Schema Editor:**
  * **Keep / Drop Columns:** Toggle individual columns or use **Select All** / **Deselect All**.
  * **Reorder Columns:** Move columns up (`▲`) or down (`▼`) to define the exact output Parquet column order.
  * **Rename Columns:** Map original CSV headers to custom output names with duplicate-name validation.
  * **Live Filter & Data Preview:** Search columns by original name, renamed name, or sample values, and inspect up to 3 distinct non-null values per column.
* **Safe & Smart Type Casting:**
  * **Overflow-Safe Defaults:** Defaults to wide PyArrow types (`int64[pyarrow]`, `float64[pyarrow]`, `string[pyarrow]`, `bool[pyarrow]`) so values appearing after the initial 250-row sample never overflow.
  * **One-Click Downcasting:** Click **Auto-Suggest Narrow Types** to infer compact integer widths (`uint8`–`int32`) and low-cardinality `category` columns from the sample.
  * **Strict Boolean Validation:** Recognizes `true/false`, `1/0`, `1.0/0.0`, `yes/no`, `t/f`, and `y/n` (ignoring whitespace-only cells as `NA`) while raising an explicit error with sample offending values if unrecognized tokens appear.
  * **Consistent Categorical & Timestamp Schemas:** Normalizes `category` dictionary encodings across row groups and archives, parses mixed-format and timezone-aware timestamps to naive UTC, and floors sub-second precision cleanly when targeting `timestamp[s]` or `timestamp[ms]`.
  * **Automatic Header Deduplication:** Repeated column headers within a CSV are automatically numbered (`col`, `col_1`, `col_2`, ...) so no data is silently dropped or overwritten.
* **Multi-Line Delimiter Auto-Detection:** Sniffs `,`, `;`, `\t`, and `|` across up to 50 sample lines, or lets you override manually.
* **Crash-Safe Output:** Writes Zstandard (`zstd`)-compressed Parquet files, checks write locks before starting, prompts before overwriting existing files, and automatically removes incomplete `.parquet` files if cancelled or closed mid-stream.

---

## Installation & Usage

Requires Python 3.9+ along with `pandas`, `pyarrow`, and `tkinterdnd2` (optional, for drag-and-drop).

Install dependencies from `prerequesites.txt` and launch the tool:

```bash
pip install -r prerequesites.txt
python csv_to_parquet_tool.py
```

1. **Select Input:** Drag and drop a `.csv`, `.zip`, `.tar.gz`, or folder onto the window, or click **Upload File** / **Select Folder**.
2. **Verify Delimiter:** Leave on **Auto** or pick `,`, `;`, `\t`, or `|` from the **Delimiter** dropdown.
3. **Customize Schema:**
   * Uncheck columns you want to omit.
   * Use `▲` / `▼` to set the output column order.
   * Enter custom column names under **New Name** (leave blank to keep original).
   * Choose a PyArrow data type per column, or click **Auto-Suggest Narrow Types** to reduce output file size.
4. **Convert:** Optionally enable **Use PyArrow engine**, then click **Convert to Parquet**. The resulting `.parquet` file is written next to your input file or folder.

---

## Supported PyArrow Data Types

| Category | Supported Types |
| :--- | :--- |
| **Text & Categorical** | `string[pyarrow]`, `category` |
| **Signed Integers** | `int8[pyarrow]`, `int16[pyarrow]`, `int32[pyarrow]`, `int64[pyarrow]` |
| **Unsigned Integers** | `uint8[pyarrow]`, `uint16[pyarrow]`, `uint32[pyarrow]`, `uint64[pyarrow]` |
| **Floating Point** | `float32[pyarrow]`, `float64[pyarrow]` |
| **Boolean** | `bool[pyarrow]` |
| **Date & Time** | `date32[pyarrow]`, `timestamp[s][pyarrow]`, `timestamp[ms][pyarrow]`, `timestamp[us][pyarrow]`, `timestamp[ns][pyarrow]` |
| **Binary** | `binary[pyarrow]` |

---

## Multi-File & Archive Behavior

When converting a folder, `.zip`, or `.tar.gz` containing multiple CSV files:

* **Master Schema:** The first valid CSV encountered (sorted alphabetically, ignoring hidden files and `__MACOSX` metadata) defines the columns shown in the UI.
* **Extra Columns:** Columns in subsequent CSVs that were not in the first CSV are ignored.
* **Missing Columns:** Columns from the master schema that are missing in subsequent CSVs are automatically filled with typed nulls (`NA`).
* **Duplicate Headers:** Identical column names within any CSV are deduplicated in order (`id`, `id_1`, `id_2`, ...).
