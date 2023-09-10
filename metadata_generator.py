import tkinter as tk
from tkinter import ttk, filedialog
import pyperclip
import csv

def browse_csv_and_convert():
    file_path = filedialog.askopenfilename(filetypes=[("CSV Files", "*.csv")])
    column_name = 'GENERIC_FILE_NAME'
    if file_path:
        try:
            with open(file_path, newline='') as csvfile:
                csv_reader = csv.DictReader(csvfile)
                rows_as_list = []
                for row in csv_reader:
                    if column_name in row:
                        rows_as_list.append(row[column_name])
        except Exception as e:
            print(f"Error: {str(e)}")
    return rows_as_list
def copy_to_clipboard():
    copied_text = output_text.get("1.0", tk.END)
    pyperclip.copy(copied_text)

def generate_insert_sql(environment_type, source_name, dag_id, schedule, catchup, owner, max_run, src_type, enable,
                        dag_type, template, rows_as_list):
    output = ""
    for generic_file_name in rows_as_list:
        dag_id_prefix = "sftp" if dag_type == "SFTP" else "e2e"
        insert_sql = f"INSERT INTO DP_{environment_type}_MTD.MTD.AIRFLOW_DAG_METADATA\n" \
                     f"(SOURCE_NAME, GENERIC_FILE_NAME, DAG_ID, DAG_SCHEDULE_INTERVAL, DAG_CATCHUP, DAG_OWNER, DAG_MAX_RUN, TASKFLOW_SRC, IS_ENABLED, DAG_TYPE, DAG_TEMPLATE)\n" \
                     f"VALUES('{source_name}', '{generic_file_name}', '{dag_id_prefix}_{dag_id}_workflow', {schedule}, {catchup}, '{owner}', {max_run}, '{src_type}', {enable}, '{dag_type}', '{dag_type.lower()}{template}');\n\n"
        output += insert_sql
    return output

def generate_delete_sql(environment_type, source_name, rows_as_list):
    output = ""
    for generic_file_name in rows_as_list:
        delete_sql = f"DELETE FROM DP_{environment_type}_MTD.MTD.AIRFLOW_DAG_METADATA\n" \
                     f"WHERE SOURCE_NAME = '{source_name}' AND GENERIC_FILE_NAME = '{generic_file_name}';\n"
        output += delete_sql
    return output

def generate_sql(rows_as_list):
    selected_type = statement_type_combo.get()
    environment_type = environment_combo.get()
    src_type = src_type_combo.get()
    dag_type = dag_type_combo.get()
    owner, template = 'tmr-translink-dp', '_template.jinja2'
    enable, catchup = True, False
    max_run = 5
    source_name = source_name_entry.get()
    schedule = schedule_entry.get()
    dag_id = dag_id_entry.get()

    output_text.config(state=tk.NORMAL)
    output_text.delete("1.0", tk.END)

    if selected_type == "INSERT NEW DAG":
        sql_output = generate_insert_sql(environment_type, source_name, dag_id, schedule, catchup, owner, max_run,
                                         src_type, enable, dag_type, template, rows_as_list)
    else:
        delete_sql = generate_delete_sql(environment_type, source_name, rows_as_list)
        insert_sql = generate_insert_sql(environment_type, source_name, dag_id, schedule, catchup, owner, max_run,
                                         src_type, enable, dag_type, template, rows_as_list)
        sql_output = delete_sql + insert_sql

    output_text.insert(tk.END, sql_output)
    output_text.config(state=tk.DISABLED)

def clear_fields():
    statement_type_combo.set(statement_types[0])
    source_name_entry.delete(0, tk.END)
    schedule_entry.delete(0, tk.END)
    dag_id_entry.delete(0, tk.END)
    output_text.config(state=tk.NORMAL)
    output_text.delete("1.0", tk.END)
    output_text.config(state=tk.DISABLED)

# Create the main window
root = tk.Tk()
root.title("Airflow DAG Metadata Generator")
root.geometry("700x500")  # Adjusted the window size

statement_types = ["INSERT NEW DAG", "UPDATE OLD DAG"]
environment = ["DEV", "PROD"]
dag_type = ["SFTP", "SOURCE"]
task_src = ["sftp-to-s3"]

# Create and configure the widgets using the grid geometry manager
row_index = 0

# Label for Statement Type
statement_type_label = tk.Label(root, text="Select Statement Type:")
statement_type_label.grid(row=row_index, column=0, padx=5, pady=5)

# Combo Box for Statement Type
statement_type_combo = ttk.Combobox(root, values=statement_types)
statement_type_combo.set(statement_types[0])
statement_type_combo.grid(row=row_index, column=1, padx=5, pady=5)

# Label for Environment
environment_label = tk.Label(root, text="Select Environment:")
environment_label.grid(row=row_index, column=2, padx=5, pady=5)

# Combo Box for Environment
environment_combo = ttk.Combobox(root, values=environment)
environment_combo.set(environment[0])
environment_combo.grid(row=row_index, column=3, padx=5, pady=5)

row_index += 1
# Label for DAG Type
dag_type_label = tk.Label(root, text="Select DAG Type:")
dag_type_label.grid(row=row_index, column=0, padx=5, pady=5)

# Combo Box for DAG Type
dag_type_combo = ttk.Combobox(root, values=dag_type)
dag_type_combo.set(dag_type[0])
dag_type_combo.grid(row=row_index, column=1, padx=5, pady=5)

# Label for Source Type
src_type_label = tk.Label(root, text="Select Source Type:")
src_type_label.grid(row=row_index, column=2, padx=5, pady=5)

# Combo Box for Source Type
src_type_combo = ttk.Combobox(root, values=task_src)
src_type_combo.set(task_src[0])
src_type_combo.grid(row=row_index, column=3, padx=5, pady=5)

# Increase row index for the next set of widgets
row_index += 1

# Label and Entry for SOURCE_NAME
source_name_label = tk.Label(root, text="SOURCE_NAME:")
source_name_label.grid(row=row_index, column=0, padx=5, pady=5, sticky=tk.W)

source_name_entry = tk.Entry(root)
source_name_entry.grid(row=row_index, column=1, padx=5, pady=5, sticky=tk.W)

# Label and Entry for DAG_ID
dag_id_label = tk.Label(root, text="DAG_ID:")
dag_id_label.grid(row=row_index, column=2, padx=5, pady=5, sticky=tk.W)

dag_id_entry = tk.Entry(root)
dag_id_entry.grid(row=row_index, column=3, padx=5, pady=5, sticky=tk.W)

row_index += 1

# Label and Entry for DAG_SCHEDULE
schedule_label = tk.Label(root, text="DAG_SCHEDULE:")
schedule_label.grid(row=row_index, column=0, padx=5, pady=5, sticky=tk.W)

schedule_entry = tk.Entry(root)
schedule_entry.grid(row=row_index, column=1, padx=5, pady=5, sticky=tk.W)

row_index += 1

# Buttons for Generate SQL, Clear Fields, and Copy to Clipboard
generate_button = tk.Button(root, text="Browse CSV and Generate SQL", command=lambda: generate_sql(browse_csv_and_convert()))
generate_button.grid(row=row_index, column=0, padx=5, pady=5)

clear_button = tk.Button(root, text="Clear Fields", command=clear_fields)
clear_button.grid(row=row_index, column=1, padx=5, pady=5)

copy_button = tk.Button(root, text="Copy to Clipboard", command=copy_to_clipboard)
copy_button.grid(row=row_index, column=2, padx=5, pady=5)

# Text widget for displaying output SQL
output_text = tk.Text(root, wrap=tk.WORD, height=15, width=60)
output_text.grid(row=row_index + 1, column=0, columnspan=3, padx=5, pady=5)

output_text.config(state=tk.DISABLED)

#Start the GUI Main Loop
root.mainloop()
