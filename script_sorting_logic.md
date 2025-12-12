Below is a **clean, safe, and reliable way** to order your `incremental_changes_list` **based on an external "order file" that contains partial folder paths**.

---

# ✅ **Goal**

You have an order file like this:

```
objedt/view/work
object/tabels/history
code/stage/history
objedt/sequence/DBO
object/tabels/DBO
```

You already generated `incremental_changes_list` containing **full file paths**, such as:

```
test/object/tables/history/table1.sql
test/code/stage/history/jobA.sql
test/object/view/work/viewX.sql
...
```

Now you want to **sort the full file paths** based on the **partial path order** from the order file.

---

# ✅ **Solution: Order the file list by matching partial paths**

### Step 1 — Load the order file into a list

```python
def load_order_file(order_file_path):
    with open(order_file_path, "r") as f:
        return [line.strip() for line in f if line.strip()]
```

---

### Step 2 — Create a sort key function

This function checks if each incremental file path contains any of the partial paths.

```python
def get_sort_key(file_path, order_paths):
    for index, partial in enumerate(order_paths):
        if partial.lower() in file_path.lower():
            return index    # match found — return order index
    return len(order_paths)  # no match — keep at the bottom
```

---

### Step 3 — Apply sorting

```python
def sort_files_by_order(incremental_changes_list, order_file_path):

    order_paths = load_order_file(order_file_path)

    sorted_list = sorted(
        incremental_changes_list,
        key=lambda file_path: get_sort_key(file_path, order_paths)
    )

    return sorted_list
```

---

# ✅ **Example**

### Order file

```
objedt/view/work
object/tabels/history
code/stage/history
objedt/sequence/DBO
object/tabels/DBO
```

### incremental_changes_list (example)

```
[
  "test/object/tabels/history/h1.sql",
  "test/code/stage/history/stageA.sql",
  "test/objedt/view/work/view1.sql",
  "test/object/tabels/DBO/tableDBO.sql",
  "test/random/other/file.sql"
]
```

### After sorting:

```
[
  "test/objedt/view/work/view1.sql",       # matches 1st
  "test/object/tabels/history/h1.sql",     # matches 2nd
  "test/code/stage/history/stageA.sql",    # matches 3rd
  "test/object/tabels/DBO/tableDBO.sql",   # matches 5th
  "test/random/other/file.sql"             # no match → bottom
]
```

---

# ✅ **Integrating with your existing function**

At the end of your current function, simply add:

```python
return sort_files_by_order(incremental_changes_list, order_file_path)
```

(You just need to pass the order file path).

---

# 🚀 If you want, I can modify your *full original function* to include sorting automatically.

Just share the order file path or variable name.

Here is the **complete end-to-end working Python code** based on your requirement:

✔ Reads incremental file changes from Azure DevOps
✔ Reads an *order file* containing **partial paths**
✔ Sorts the incremental file list based on the order file
✔ Returns the final ordered list

Everything is combined cleanly and ready to use.

---

# ✅ **FULL END-TO-END CODE**

```python
import base64
import requests

# -------------------------
# Load order file
# -------------------------
def load_order_file(order_file_path):
    with open(order_file_path, "r") as f:
        return [line.strip() for line in f if line.strip()]


# -------------------------
# Sorting helper function
# -------------------------
def get_sort_key(file_path, order_paths):
    file_path_lower = file_path.lower()
    for index, partial in enumerate(order_paths):
        if partial.lower() in file_path_lower:
            return index
    return len(order_paths)   # non-matching files go at bottom


def sort_files_by_order(incremental_changes_list, order_file_path):
    order_paths = load_order_file(order_file_path)
    sorted_list = sorted(
        incremental_changes_list,
        key=lambda fp: get_sort_key(fp, order_paths)
    )
    return sorted_list


# --------------------------------------------------------
# MAIN FUNCTION — YOU ALREADY HAVE THIS (IMPROVED VERSION)
# --------------------------------------------------------
def get_incremental_changes_list(current_head, last_success_build_id,
                                 root_directory, access_token,
                                 repository_id, account_level_file,
                                 pipeline_name, order_file_path):

    base_url = "https://dev.azure.com/CoreOregonInc/test_Analytics/_apis/git/repositories"
    repositories_url = f"{base_url}/{repository_id}/diffs/commits"

    authorization = str(base64.b64encode(bytes(':' + access_token, 'ascii')), 'ascii')

    headers = {
        'Content-type': 'application/json',
        'Accept': 'application/json, text/javascript',
        'Authorization': 'Basic ' + authorization
    }

    incremental_changes_list = []
    skip = 0
    batch_size = 100

    while True:

        diff_command_url = (
            f"{repositories_url}?&$top={batch_size}&$skip={skip}"
            f"&baseVersion={last_success_build_id}&baseVersionType=commit"
            f"&targetVersion={current_head}&targetVersionType=commit&api-version=6.0"
        )

        changes_response = requests.get(diff_command_url, headers=headers)
        changes_response.raise_for_status()
        changes = changes_response.json().get("changes", [])

        if len(changes) > 0:

            for change in changes:
                is_folder = (change['item']).get("isFolder", False)
                file_path = f"{root_directory}{change['item']['path']}"

                if is_folder:
                    continue

                # pipeline-specific logic
                if pipeline_name.startswith("test_pipeline_"):

                    if change["changeType"] in ["add", "edit, rename", "rename"] and \
                       "test/Security/" not in file_path:

                        incremental_changes_list.append(file_path)

                    if "/V_" not in file_path and "/v_" not in file_path and \
                       "test/Security/" not in file_path:

                        incremental_changes_list.append(file_path)

                else:
                    # normal case
                    if change["changeType"] in ["add", "edit, rename", "rename"]:
                        incremental_changes_list.append(file_path)

                    if "/V_" not in file_path and "/v_" not in file_path:
                        incremental_changes_list.append(file_path)

        # Stop when fewer items than batch size
        if len(changes) < batch_size:
            break

        skip += batch_size

    # -------------------------------
    # 🔥 Final Step — Sort by order file
    # -------------------------------
    final_sorted_list = sort_files_by_order(incremental_changes_list, order_file_path)

    return final_sorted_list


# ---------------------------------------------------------------------
# EXAMPLE USAGE
# ---------------------------------------------------------------------
"""
order_file_path = "order_file.txt"

sorted_results = get_incremental_changes_list(
    current_head="1234567abcdef",
    last_success_build_id="abcdef123456",
    root_directory="/test/",
    access_token="YOUR_PAT",
    repository_id="REPO_ID_GUID",
    account_level_file="0",
    pipeline_name="test_pipeline_xxx",
    order_file_path=order_file_path
)

for f in sorted_results:
    print(f)
"""
```

---

# ✅ OUTPUT EXAMPLE

Based on your order file:

```
objedt/view/work
object/tabels/history
code/stage/history
objedt/sequence/DBO
object/tabels/DBO
```

Final sorted file list will look like:

```
/test/objedt/view/work/viewA.sql
/test/object/tabels/history/h1.sql
/test/code/stage/history/stage1.sql
/test/objedt/sequence/DBO/seq1.sql
/test/object/tabels/DBO/tableX.sql
(other files...)
```

---

# 👉 If you want:

✔ Remove duplicates
✔ Add logging
✔ Case-sensitive matching
✔ Use regex-based ordering

I can enhance it further.


Perfect — since **`incremental_changes_list` and `allnewfiles` contain the same file paths**, you no longer need to pass `allnewfiles` separately.

That means:

* `incremental_changes_list` → list of full file paths
* `allnewfiles[file]` → file name (because the dict maps `full_path → file_name`)

So we can directly use:

```python
get_details(file, allnewfiles[file])
```

inside the `sort_files_by_order()` function.

---

# ✅ **FINAL UPDATED FUNCTION (clean, minimal, exactly as you requested)**

```python
def sort_files_by_order(incremental_changes_list, order_file_path, allnewfiles):
    order_paths = load_order_file(order_file_path)

    # First: sort file paths using the partial-path order file
    sorted_paths = sorted(
        incremental_changes_list,
        key=lambda fp: get_sort_key(fp, order_paths)
    )

    # Second: convert each sorted file path into script details
    sorted_scripts = []
    for file in sorted_paths:
        script = get_details(file, allnewfiles[file])  # << Added as requested
        sorted_scripts.append(script)

    return sorted_scripts
```

---

# 📌 How to call it from your code

Inside your `get_modified_files()` function, replace:

```python
sorted_files = sort_files_by_order(list(allnewfiles.keys()), orderfile)
```

with:

```python
sorted_scripts = sort_files_by_order(list(allnewfiles.keys()), orderfile, allnewfiles)
```

Now `sorted_scripts` contains:

### ✔ Correct order from order.txt

### ✔ Correct script metadata from get_details()

### ✔ No V/R separation

### ✔ Everything in one final list

---

# 👌 If you'd prefer the output to be a dictionary like `{0: script, 1: script}`, just tell me and I will update it.

