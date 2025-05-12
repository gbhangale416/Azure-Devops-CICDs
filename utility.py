import requests
import base64
import os
from datetime import datetime
import re


def get_incremental_changes_list(current_head, last_success_build_id, root_directory, access_token, repository_id):

    # Construct the API Base URL
    base_url = "https://dev.azure.com/CareOregonInc/coEDW_Analytics/_apis/git/repositories"

    # Update the repositories_url with repository_id
    repositories_url = f"{base_url}/{repository_id}/diffs/commits"
    # Define the authentication token
    authorization = str(base64.b64encode(bytes(':'+access_token, 'ascii')), 'ascii')

    # header for request to the commit search API
    headers = {
        'Content-type': 'application/json',
        'Accept': 'application/json, text/javascript',
        'Authorization': 'Basic '+authorization
    }
    incremental_changes_list = []
    # Set the initial skip value and maximum batch size
    skip = 0
    batch_size = 100
    processed_changes_count = 0

    while True:
        # Execute the git diff command using Azure DevOps API
        diff_command_url = f"{repositories_url}?&$top={batch_size}&$skip={skip}&baseVersion={last_success_build_id}&baseVersionType=commit&targetVersion={current_head}&targetVersionType=commit&api-version-6.0"
        changes_response = requests.get(diff_command_url, headers=headers)
        changes_response.raise_for_status()
        changes = changes_response.json().get("changes", [])
        print(f"Executing Azure API URL: {diff_command_url}")
        processed_changes_count += len(changes)
        # Extract the modified file paths
        if len(changes) > 0:
            print(f"Iterating while loop - Changes Count: {len(changes)}")
            for change in changes:
                is_folder = (change['item']).get("isFolder", False)
                file_path = f"{root_directory}{change['item']['path']}"
                if change["changeType"] in ["add", "edit, rename", "rename"] and ("/V_" in file_path or "/v_" in file_path) and not is_folder:
                    incremental_changes_list.append(file_path)
                if change["changeType"] in ["add", "edit, rename", "rename", "edit"] and ("/V_" not in file_path or "/v_" not in file_path or "/A_" not in file_path or "/a_" not in file_path) and not is_folder:
                    incremental_changes_list.append(file_path)

        # Break the loop if the batch size is less than the maximum batch size
        if len(changes) < batch_size:
            print(f"Breaking the loop as batch size is less than the maximum batch size - {len(changes)}")
            print(f"Total scripts Add/updated since last successful build - {processed_changes_count}")
            return incremental_changes_list

        # Increment the skip value for the next batch
        skip += batch_size


def get_incremental_A_changes_list(root_directory, access_token, repository_id, branch_name):

    # Construct the API Base URL
    base_url = "https://dev.azure.com/CareOregonInc/coEDW_Analytics/_apis/git/repositories"

    # Base64 encode the PAT for authentication
    authorization = str(base64.b64encode(bytes(':'+access_token, 'ascii')), 'ascii')
    # header for request to the commit search API
    headers = {
        'Content-type': 'application/json',
        'Accept': 'application/json, text/javascript',
        'Authorization': 'Basic '+authorization
    }
    incremental_changes_a_files = []

    # Define the API endpoint to get items in the specified branch
    api_url = f"{base_url}/{repository_id}/items?version={branch_name}&scopePath=/&recursionLevel=full&api-version=7.1"

    # Send the GET request
    changes_response = requests.get(api_url, headers=headers)
    changes_response.raise_for_status()
    changes = changes_response.json().get("value", [])
    print(f"Executing Azure API URL_A: {api_url}")

    if len(changes) > 0:
        print(f"Changes Count: {len(changes)}")
        for change in changes:
            file_path = f"{root_directory}{change['path']}"
            is_folder = change.get("isFolder", False)
            if ("/A_" in file_path or "/a_" in file_path) and not is_folder:
                incremental_changes_a_files.append(file_path)

    return incremental_changes_a_files


def get_modified_files(current_head, snowflake_connection, autocommit, verbose, buildid_info_table, last_success_build_id, execute_snowflake_query, root_directory, access_token, orderfile, repository_id, branch_name, folder_path=None):
    order_list = []
    incremental_changes_list = []
    if orderfile is not None:
        with open(orderfile) as f:
            order_list = [line.strip() for line in f]

    # build_numbers = getBuildInfo(snowflake_connection, autocommit, verbose, buildid_info_table, execute_snowflake_query)
    # if len(build_numbers) != 0:
    #     last_success_build_id = build_numbers[0][0]
    # else:
    #     last_success_build_id = last_success_build_id

    print(f"last_success_build_id: {last_success_build_id}")
    print(f"current_head: {current_head}")

    incremental_VR_changes__list = get_incremental_changes_list(current_head, last_success_build_id, root_directory, access_token, repository_id)
    incremental_A_changes_list = get_incremental_A_changes_list(root_directory, access_token, repository_id, branch_name)

    incremental_changes_list = incremental_VR_changes__list + incremental_A_changes_list

    all_v_files = {}
    all_r_files = {}
    all_a_files = {}
    v = 0
    r = 0
    a = 0

    if not order_list:
        order_list = [root_directory]

    for order in order_list:
        if order.endswith("/"):
            order = order[:-1]
        for (directory_path, _, file_names) in os.walk(root_directory):
            for file_name in file_names:
                if file_name.endswith('.sql'):
                    file_full_path = os.path.join(directory_path, file_name)
                    if file_full_path in incremental_changes_list:
                        if (re.search(folder_path, file_full_path)):
                            script = get_details(file_full_path, file_name)
                            script_type = script.get('script_type')
                            if script_type == 'V' and order in file_full_path:
                                all_v_files[v] = script
                                v = v + 1
                            elif script_type == 'R':
                                all_r_files[r] = script
                                r = r + 1
                            elif script_type == 'A':
                                all_a_files[a] = script
                                a = a + 1
    print(f"Total V scripts since last successful build - {len(all_v_files)}")
    print(f"Total R scripts since last successful build - {len(all_r_files)}")
    print(f"Total A scripts since last successful build - {len(all_a_files)}")
    return all_v_files, all_r_files, all_a_files


def get_details(full_file_path, file_name):
    try:
        file_modified_time = datetime.fromtimestamp(os.path.getmtime(full_file_path)).strftime('%Y%m%d%H%M%S%f')
        script_name_parts = re.search(r'^([RrVvAa])_(.+)\.sql$', file_name.strip())

        # Add this script to our dictionary (as nested dictionary)
        script = dict()
        script['script_name'] = file_name
        script['script_full_path'] = full_file_path

        if script_name_parts is None:
            script['script_type'] = 'R'
            script['script_description'] = os.path.splitext(file_name)[0].replace('_', ' ').capitalize()
        else:
            script['script_type'] = script_name_parts.group(1).upper()
            script['script_description'] = script_name_parts.group(2).replace('_', ' ').capitalize()

        script['script_modified_time'] = file_modified_time
        return script
    except Exception as error:
        return str(error)


def getBuildInfo(snowflake_connection, autocommit, verbose, buildid_info_table, execute_snowflake_query):

    qry_build_info_tables = "SELECT SUCCESSFUL_BUILD_ID FROM {0}.{1}.{2} ORDER BY DATE DESC".format(buildid_info_table['database_name'], buildid_info_table['schema_name'], buildid_info_table['buildinfo_table_name'])
    resultset = execute_snowflake_query(snowflake_connection, qry_build_info_tables, autocommit, verbose)
    tab_cursor = resultset[0]
    ret_build_id = tab_cursor.fetchall()
    return ret_build_id
