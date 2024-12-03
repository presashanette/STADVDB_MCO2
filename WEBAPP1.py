import streamlit as st
import mysql.connector
import pandas as pd
from datetime import datetime
import threading
import time
import paramiko

TRANSACTION_LOG = []
FAILED_SLAVE_TRANSACTIONS = {"node2": [], "node3": []}
FAILED_MASTER_TRANSACTIONS = []
RETRY_FLAG = False 
if 'case' not in st.session_state:
    st.session_state.case = 0

MASTER_VM = {
    "hostname": "ccscloud.dlsu.edu.ph", 
    "port": 21981,                      
    "username": "root",                
    "password": "g27d94sHTDWRxQ38eSpAtrYE"  
}

NODE2_VM = {
    "hostname": "ccscloud.dlsu.edu.ph",  
    "port": 21991,                       
    "username": "root",            
    "password": "g27d94sHTDWRxQ38eSpAtrYE"
}

NODE3_VM = {
    "hostname": "ccscloud.dlsu.edu.ph", 
    "port": 22001,                      
    "username": "root",               
    "password": "g27d94sHTDWRxQ38eSpAtrYE"  
}

MASTER_DB = {
    "host": "ccscloud.dlsu.edu.ph",  
    "user": "root",     
    "password": "password", 
    "database": "node1", 
    "port": 21982
}

NODE2_DB = {
    "host": "ccscloud.dlsu.edu.ph", 
    "user": "root",       
    "password": "password", 
    "database": "node1", 
    "port": 21992
}

NODE3_DB = {
    "host": "ccscloud.dlsu.edu.ph", 
    "user": "root",       
    "password": "password",  
    "database": "node1",  
    "port": 22002
}

MASTER_UPDATE_QUERY = f"""
    UPDATE node1
    SET name = %s, release_date = %s, required_age = %s, price = %s, dlc_count = %s,
        detailed_description = %s, about_the_game = %s, reviews = %s, header_image = %s,
        website = %s, support_url = %s, support_email = %s, metacritic_score = %s,
        metacritic_url = %s, achievements = %s, recommendations = %s, notes = %s, packages = %s,
        developers = %s, publishers = %s
    WHERE game_id = %s;
"""

SLAVE_INSERT_QUERY = """
    INSERT INTO node1 (game_id, name, release_date, required_age, price, dlc_count,
        detailed_description, about_the_game, reviews, header_image, website,
        support_url, support_email, metacritic_score, metacritic_url, achievements,
        recommendations, notes, packages, developers, publishers)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE 
        release_date = VALUES(release_date),
        name = VALUES(name),
        required_age = VALUES(required_age),
        price = VALUES(price),
        dlc_count = VALUES(dlc_count),
        detailed_description = VALUES(detailed_description),
        about_the_game = VALUES(about_the_game),
        reviews = VALUES(reviews),
        header_image = VALUES(header_image),
        website = VALUES(website),
        support_url = VALUES(support_url),
        support_email = VALUES(support_email),
        metacritic_score = VALUES(metacritic_score),
        metacritic_url = VALUES(metacritic_url),
        achievements = VALUES(achievements),
        recommendations = VALUES(recommendations),
        notes = VALUES(notes),
        packages = VALUES(packages),
        developers = VALUES(developers),
        publishers = VALUES(publishers);
"""

def create_connection(config):
    try:
        connection = mysql.connector.connect(
            host=config["host"],
            database=config["database"],
            user=config["user"],
            password=config["password"],
            port=config["port"],
        )
        if connection.is_connected():
            return connection
    except mysql.connector.Error as e:
        st.error(f"Database connection error: {e}")
        return None

def get_data_from_db(query):
    try:
        connection = create_connection(MASTER_DB)
        if connection is None:
            st.error("Error: Unable to connect to the database")
            return pd.DataFrame()

        cursor = connection.cursor(dictionary=True) 
        cursor.execute(query)
        rows = cursor.fetchall()

        df = pd.DataFrame(rows)
        cursor.close()
        connection.close()

        if df.empty:
            st.warning("No data returned from the query.")
        return df

    except mysql.connector.Error as e:
        st.error(f"Database error: {e}")
        return pd.DataFrame()

def stop_mysql(config):
    try: 
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        ssh.connect(**config)
        
        stdin, stdout, stderr = ssh.exec_command("sudo systemctl stop mysql")
        stdin.write(config["password"] + '\n')
        stdin.flush()
        
        print(stdout.read().decode())
        print(stderr.read().decode())
        
        ssh.close()
        st.success("MySQL service stopped on the VM successfully.")
    except Exception as e:
        st.error(f"Failed to stop MySQL service on the VM: {e}")
    
def start_mysql(config, isSlave):
    try:

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        

        ssh.connect(**config)
        

        stdin, stdout, stderr = ssh.exec_command("sudo systemctl start mysql")
        stdin.write(config["password"] + '\n') 
        stdin.flush()

        if isSlave == True:
            stdin, stdout, stderr = ssh.exec_command("sudo mysql -e 'START SLAVE;'")
            stdin.write(config["password"] + '\n')  
            stdin.flush()

     
        replication_output = stdout.read().decode()
        replication_error = stderr.read().decode()
        if replication_error:
            print("Error starting replication:", replication_error)
            st.error(f"Error starting replication: {replication_error}")
        else:
            print("Replication started successfully.")

       
        print(replication_output)

        ssh.close()
        
        st.success("MySQL service started and replication resumed on the slave VM successfully.")
    except Exception as e:
        st.error(f"Failed to start MySQL service or resume replication on the VM: {e}")


def propagate_to_slave(row):
    """Propagate changes to slave nodes and handle failures."""
    release_date = pd.to_datetime(row["release_date"])

    if release_date < datetime(2020, 1, 1):  
        try:
            connection = create_connection(NODE2_DB)
            if connection is None:
                raise Exception("Node 2 unavailable")
            
            cursor = connection.cursor()

            params = (
                row['game_id'], row['name'], row['release_date'], row['required_age'], row['price'],
                row['dlc_count'], row['detailed_description'], row['about_the_game'], row['reviews'],
                row['header_image'], row['website'], row['support_url'], row['support_email'],
                row['metacritic_score'], row['metacritic_url'], row['achievements'], row['recommendations'],
                row['notes'], row['packages'], row['developers'], row['publishers']
            )
            cursor.execute(SLAVE_INSERT_QUERY, params)
            connection.commit()
            st.success(f"Row propagated to Node 2 (slave) at {NODE2_DB['host']}")
            print(f"Executed query on Node 2:\n{SLAVE_INSERT_QUERY % params}")

            remove_from_slave(NODE3_DB, row["game_id"])

            cursor.close()
            connection.close()

        except Exception as e:
            st.error(f"Error propagating to Node 2: {e}")
            FAILED_SLAVE_TRANSACTIONS["node2"].append((row, "insert"))
            start_retry_mechanism()


    elif release_date >= datetime(2020, 1, 1): 
        try:
            connection = create_connection(NODE3_DB)
            if connection is None:
                raise Exception("Node 3 unavailable")

            cursor = connection.cursor()


            params = (
                row['game_id'], row['name'], row['release_date'], row['required_age'], row['price'],
                row['dlc_count'], row['detailed_description'], row['about_the_game'], row['reviews'],
                row['header_image'], row['website'], row['support_url'], row['support_email'],
                row['metacritic_score'], row['metacritic_url'], row['achievements'], row['recommendations'],
                row['notes'], row['packages'], row['developers'], row['publishers']
            )
            cursor.execute(SLAVE_INSERT_QUERY, params)
            connection.commit()
            st.success(f"Row propagated to Node 3 (slave) at {NODE3_DB['host']}")
            print(f"Executed query on Node 3:\n{SLAVE_INSERT_QUERY % params}")


            remove_from_slave(NODE2_DB, row["game_id"])

            cursor.close()
            connection.close()

        except Exception as e:
            st.error(f"Error propagating to Node 3: {e}")
            FAILED_SLAVE_TRANSACTIONS["node3"].append((row, "insert"))
            start_retry_mechanism()
        

def remove_from_slave(slave_config, game_id):
    """Remove a row from a specific slave node based on game_id."""
    try:
        connection = create_connection(slave_config)
        if connection is None:
            st.warning(f"Unable to connect to slave at {slave_config['host']} for deletion.")
            return

        cursor = connection.cursor()
        query = "DELETE FROM node1 WHERE game_id = %s;"
        cursor.execute(query, (game_id,))
        connection.commit()
        cursor.close()
        connection.close()

        st.success(f"Row with game_id {game_id} removed from slave at {slave_config['host']}")

    except Exception as e:
        st.error(f"Error removing row from slave at {slave_config['host']}: {e}")

def recover_central_node():
    
    global FAILED_MASTER_TRANSACTIONS, FAILED_SLAVE_TRANSACTIONS

    try:
        start_mysql(MASTER_VM, False)
        connection = create_connection(MASTER_DB)
        if connection is None:
            raise Exception("Central node still unavailable")

        cursor = connection.cursor()


        for query, params in FAILED_MASTER_TRANSACTIONS[:]:
            try:
                cursor.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
                cursor.execute("START TRANSACTION;") 
                cursor.execute(query, params)

                TRANSACTION_LOG.append((query, params))  
                FAILED_MASTER_TRANSACTIONS.remove((query, params)) 
                st.success("Master transaction retried successfully")
                cursor.execute("COMMIT;") 
                
                row = {
                    "game_id": params[-1],
                    "name": params[0],
                    "release_date": params[1],
                    "required_age": params[2],
                    "price": params[3],
                    "dlc_count": params[4],
                    "detailed_description": params[5],
                    "about_the_game": params[6],
                    "reviews": params[7],
                    "header_image": params[8],
                    "website": params[9],
                    "support_url": params[10],
                    "support_email": params[11],
                    "metacritic_score": params[12],
                    "metacritic_url": params[13],
                    "achievements": params[14],
                    "recommendations": params[15],
                    "notes": params[16],
                    "packages": params[17],
                    "developers": params[18],
                    "publishers": params[19],
                }

                propagate_to_slave(row)

            except Exception as e:
                st.error(f"Error retrying master transaction: {e}")
             

        connection.commit()
        cursor.close()
        connection.close()

    except Exception as e:
        st.error(f"Error recovering central node: {e}")


def recover_node(node):
    queue_key = f"node{node}"
    config_db = NODE2_DB if node == 2 else NODE3_DB
    config_vm = NODE2_VM if node == 2 else NODE3_VM

    try:
        start_mysql(config_vm, True)
        connection = create_connection(config_db)
        if connection is None:
            raise Exception(f"Node {node} still unavailable")

        cursor = connection.cursor()
        for transaction in FAILED_SLAVE_TRANSACTIONS[queue_key]:
            row, operation = transaction
            if operation == "insert":
                params = (
                    row['game_id'], row['name'], row['release_date'], row['required_age'], row['price'],
                    row['dlc_count'], row['detailed_description'], row['about_the_game'], row['reviews'],
                    row['header_image'], row['website'], row['support_url'], row['support_email'],
                    row['metacritic_score'], row['metacritic_url'], row['achievements'], row['recommendations'],
                    row['notes'], row['packages'], row['developers'], row['publishers']
                )
                cursor.execute(SLAVE_INSERT_QUERY, params)

        connection.commit()
        FAILED_SLAVE_TRANSACTIONS[queue_key] = [] 
        st.success(f"Node {node} recovered and transactions replayed")

        cursor.close()
        connection.close()

    except Exception as e:
        st.error(f"Error recovering Node {node}: {e}")


RETRY_THREAD_LOCK = threading.Lock()

def start_retry_mechanism():
    global RETRY_FLAG

    if not RETRY_FLAG:
        RETRY_FLAG = True
        with RETRY_THREAD_LOCK: 
            retry_thread = threading.Thread(target=retry_transactions, daemon=True)
            retry_thread.start()

def retry_transactions():
    global RETRY_FLAG, FAILED_MASTER_TRANSACTIONS, FAILED_SLAVE_TRANSACTIONS

    while RETRY_FLAG:
        try:
            if FAILED_MASTER_TRANSACTIONS:
                recover_central_node()
            if FAILED_SLAVE_TRANSACTIONS["node2"]:
                recover_node(2)
            if FAILED_SLAVE_TRANSACTIONS["node3"]:
                recover_node(3)
            if (not FAILED_MASTER_TRANSACTIONS and not FAILED_SLAVE_TRANSACTIONS["node2"] and not FAILED_SLAVE_TRANSACTIONS["node3"]):
                RETRY_FLAG = False
        except Exception as e:
            print(f"Error during retry: {e}")

        time.sleep(60) 

st.set_page_config(page_title="Distributed Database System 1", layout="wide")
with st.sidebar:
    st.title("Global Recovery")
    st.header("Test Cases")
    
    case_selected = st.radio("Select Simulation", ("Normal", "Case 1", "Case 2", "Case 4"))
    case = None 

    if case_selected == "Normal":
        if st.button("Run Normal"):
            st.session_state.case = 0

    elif case_selected == "Case 1":
        if st.button("Run Case 1"):
            st.session_state.case = 1

    elif case_selected == "Case 2":
        if st.button("Run Case 2"):
            st.session_state.case = 2

    elif case_selected == "Case 4":
        if st.button("Run Case 4"):
            st.session_state.case = 4

    if case is not None:
        st.write(f"Selected Case: {case}")  

selected_columns = [
    "game_id",
    "name",
    "release_date",
    "required_age",
    "price",
    "dlc_count",
    "detailed_description",
    "about_the_game",
    "reviews",
    "header_image",
    "website",
    "support_url",
    "support_email",
    "metacritic_score",
    "metacritic_url",
    "achievements",
    "recommendations",
    "notes",
    "packages",
    "developers",
    "publishers",
]

if 'page' not in st.session_state:
    st.session_state.page = 0  

def set_page(page):
    st.session_state.page = page

st.button('Back', on_click=set_page, args=[max(0, st.session_state.page - 1)])
st.button('Next', on_click=set_page, args=[st.session_state.page + 1])


query = f"""
    SELECT {", ".join(selected_columns)}
    FROM node1
    LIMIT {st.session_state.page * 15}, 15
"""
df = get_data_from_db(query)


edited_df = st.data_editor(df, use_container_width=True)

if st.button('Update Data'):
    try:       
        connection = create_connection(MASTER_DB)  
        if connection is None:
            raise Exception("Unable to connect to the master database for updating data.")
        cursor = connection.cursor()
        
        if st.session_state.case == 1:
            stop_mysql(MASTER_VM)
        if st.session_state.case == 2:
            stop_mysql(NODE2_VM)


        for index, row in edited_df.iterrows():
            try:
                cursor.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
                cursor.execute("START TRANSACTION;") 

                params = (
                    row['name'], row['release_date'], row['required_age'], row['price'], row['dlc_count'],
                    row['detailed_description'], row['about_the_game'], row['reviews'], row['header_image'],
                    row['website'], row['support_url'], row['support_email'], row['metacritic_score'],
                    row['metacritic_url'], row['achievements'], row['recommendations'], row['notes'],
                    row['packages'], row['developers'], row['publishers'], row['game_id']
                )

                cursor.execute(MASTER_UPDATE_QUERY, params)
                
                if st.session_state.case == 4:
                    stop_mysql(MASTER_VM)
                    stop_mysql(NODE2_VM)
                    stop_mysql(NODE3_VM)
                
                propagate_to_slave(row)
                cursor.execute("COMMIT;") 


                st.success(f"Row with game_id {row['game_id']} updated successfully!")

            except Exception as e:
                cursor.execute("ROLLBACK;")
                st.error(f"Error updating row with game_id {row['game_id']}: {e}")
                FAILED_MASTER_TRANSACTIONS.append((MASTER_UPDATE_QUERY, params))  
                start_retry_mechanism()

        st.success("All changes successfully applied to the database!")

    except Exception as e:
        st.error(f"Error with the master database connection or update process: {e}")
        for index, row in edited_df.iterrows():
            params = (
                row['name'], row['release_date'], row['required_age'], row['price'], row['dlc_count'],
                row['detailed_description'], row['about_the_game'], row['reviews'], row['header_image'],
                row['website'], row['support_url'], row['support_email'], row['metacritic_score'],
                row['metacritic_url'], row['achievements'], row['recommendations'], row['notes'],
                row['packages'], row['developers'], row['publishers'], row['game_id']
            )
            FAILED_MASTER_TRANSACTIONS.append((MASTER_UPDATE_QUERY, params)) 
            st.info("Retry mechanism is running. Failed transactions are being retried...")
            time.sleep(3)
            start_retry_mechanism()

    finally:
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'connection' in locals() and connection is not None:
            connection.close()

    st.rerun() 



