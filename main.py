"""This repository uses the Fast API web framework to create a REST API. 
The endpoints make calls to github's REST API to collect metrics of specified repositories.
Author: Danny Kokkinos
"""

from datetime import datetime, timezone
import time
import sqlite3
from pathlib import Path
from fastapi import FastAPI, HTTPException
from fastapi.responses import RedirectResponse
import requests
import numpy as np
import uvicorn

PORT = 9000
DATABASE = Path("database.db")
app = FastAPI()


@app.get("/")
def read_root():
    """Root endpoint"""
    return RedirectResponse("/docs")


@app.get("/metrics/all")
def get_all_metrics():
    """Returns metrics for all rows in Calculations table"""
    query = '''SELECT * FROM Calculations'''
    message = "Calculations table does not exist. Please use the other metrics endpoint to populate the Calculations table"
    event =get_query(query,message=message)
    test = []
    for e in event:
        temp = {
            "Owner": e[0].replace('_','-'),
            "Repo": e[1].replace('_','-'),
            "Date": e[2],
            "Average_addition": e[3],
            "Average_deletion": e[4],
        }
        test.append(temp)
    return test


@app.get("/metrics/{owner}/{repo}", status_code=200)
def get_metrics(owner: str, repo: str):
    """Saves the metrics of a specified repository to a table"""
    url = f"https://api.github.com/repos/{owner}/{repo}/stats/code_frequency"
    owner = owner.replace("-", "_")
    repo = repo.replace("-", "_")
    attemps = 0
    while attemps<10:
        response = requests.get(url, timeout=10)
        attemps +=1
        if response.status_code == 204:
            raise HTTPException(
                status_code=403, detail=
        '''Repository does not have any past metrics to display.''')
        if response.status_code == 403:
            raise HTTPException(
                status_code=403, detail=
        '''Access to this repository is either forbidden or you have made too many requests. Please wait an hour and try again.''')
        if response.status_code == 202:
            print("Statistics are being compiled...")
            time.sleep(6)
        elif response.status_code == 204:
            return {"message": "No content to return"}
        elif response.status_code == 200:
            metrics = response.json()
            # Create table if it doesnt already exist
            query = f"""
            CREATE TABLE IF NOT EXISTS {owner}_{repo} (
                Date INTEGER,
                Additions INTEGER,
                Deletions INTEGER
            )
            """
            create_table(query)
            # Add data if the data is not in the table
            add_raw_data_to_table(owner, repo, metrics)
            # Calculate average consecutive events
            average_addition = calculate_average_duration(owner, repo, column=1)
            average_deletion = calculate_average_duration(owner, repo, column=2)
            # Create calculations table
            query = """
            CREATE TABLE IF NOT EXISTS Calculations (
                Owner TEXT,
                Repo TEXT,
                Date INTEGER,
                Additions INTEGER,
                Deletions INTEGER
            )
            """
            create_table(query)
            # Save calculations to table
            add_calculations_to_table(owner, repo, average_addition, average_deletion)
            # return data from table to response

            query = f'''SELECT * FROM Calculations WHERE owner = "{owner}" AND repo = "{repo}" '''
            event = get_query(query,1)

            return {
                "Owner": event[0].replace('_','-'),
                "Repo": event[1].replace('_','-'),
                "Date": event[2],
                "Average_addition": event[3],
                "Average_deletion": event[4],
            }
    raise HTTPException(
        status_code=400, detail=
        '''Github is taking longer than usual to compile the requested data. Please wait a few minutes and try again.''')


def calculate_average_duration(owner, repo, column):
    """Calculates the avarage between non-zero consecutive numbers"""
    try:

        table = f"""{owner}_{repo}"""
        query = f"""SELECT * FROM {table} ORDER BY Date ASC"""
        metrics = get_query(query)
        dates = [inner[0] for inner in metrics]
        event = [inner[column] for inner in metrics]
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Failed to sort data.") from exc

    average_temp = []
    for i in range(len(event)):
        if event[i] == 0:
            continue
        for j in range(i + 1, len(event)):
            if event[j] == 0:
                continue
            average_temp.append(days_between_unix_dates(dates[i], dates[j]))
            break
    return round(np.mean(average_temp), 2)


def days_between_unix_dates(timestamp1, timestamp2):
    """Functions used to conver unix dates to datetime and find the difference"""
    # Convert Unix timestamps to datetime objects
    date1 = datetime.fromtimestamp(timestamp1, tz=timezone.utc)
    date2 = datetime.fromtimestamp(timestamp2, tz=timezone.utc)
    difference = date2 - date1
    days = difference.days

    return days


def create_table(query: str) -> None:
    """Create a new table in the database"""
    message = "Failed to create the table."
    update_query(query,message)


def add_calculations_to_table(
    owner: str, repo: str, average_addition: float, average_deletion: float
):
    """Adds the calculated averages to the Calculations table"""
    owner = owner.replace("-", "_")
    repo = repo.replace("-", "_")

    query = f"""SELECT * FROM {"Calculations"} WHERE Owner = "{owner}" AND repo = "{repo}" """
    check = get_query(query,1)

    if check is None:  # Add if not in table
        query = """INSERT INTO Calculations (Owner, Repo, Date, Additions, Deletions) VALUES (?, ?, ?, ?, ?)"""
        message = "Failed to add data to table."
        current_date = int(time.time())
        data = [(owner, repo, current_date, average_addition, average_deletion)]
        set_query(query, data, message)
    else:  # Update value if value already in table
        query = f"""SELECT Date FROM Calculations WHERE owner = "{owner}" AND repo = "{repo}" """
        date = get_query(query,1)
        if date is not None:
            date = date[0]
            current_date = int(time.time())
            if current_date > date:
                query = f"""UPDATE Calculations SET Date = {current_date} WHERE owner = "{owner}" AND repo = "{repo}" """
                update_query(query)


def add_raw_data_to_table(owner: str, repo: str, data: list) -> None:
    """Adds the raw data to the table"""
    temp = []
    for index,d in enumerate(data):
        if index == 500: #exits if 500 entries have been reached
            break
        table = f"""{owner}_{repo}"""
        query = f"""SELECT * FROM {table} WHERE Date = {d[0]}"""
        check = get_query(query,1)
        if check is None:  # IF row not in table add new row
            temp.append(d)
    query = f"""INSERT INTO {table} (Date, Additions, Deletions) VALUES (?, ?, ?)"""
    message = "Failed to add data to table"
    set_query(query, temp,message)


def set_query(query:str, data, message:str) -> None:
    """Used to send data to a database"""
    try:
        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()
        cursor.executemany(query, data)
        conn.commit()
        conn.close()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=message) from exc


def get_query(query:str , state:int = 0, message:str = "Failed to get query"):
    """Used to get data from a database"""
    try:
        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()
        cursor.execute(query)
        if state == 1:
            events = cursor.fetchone()
        else:
            events = cursor.fetchall()
        conn.close()
        return events
    except Exception as exc:
        raise HTTPException(status_code=400, detail=message) from exc


def update_query(query:str , message:str ="Failed to update table."):
    """Used to update data in a database"""
    try:
        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        conn.close()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=message) from exc


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=PORT)
