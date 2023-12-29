from __future__ import annotations

import sqlite3

import pandas as pd


class Audit:
    def __init__(self, db_name="audit_database.db"):
        """
        Initialize the Audit class.

        Parameters:
        - db_name (str): Name of the SQLite database.
        """
        self.db_name = db_name
        self._create_database()

    def _create_database(self):
        """
        Create the SQLite database and the 'audit' table if they don't exist.
        """
        try:
            conn = sqlite3.connect(self.db_name)
            cursor = conn.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS audit (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    app_id TEXT NOT NULL,
                    processed INTEGER DEFAULT 0,
                    count_files_executors INTEGER DEFAULT 0,
                    count_files_stages INTEGER DEFAULT 0,
                    count_files_tasks INTEGER DEFAULT 0,
                    count_files_jobs INTEGER DEFAULT 0,
                    count_files_environment INTEGER DEFAULT 0,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    load_vector INTEGER DEFAULT 0
                )
            """,
            )

            conn.commit()
        except sqlite3.Error as e:
            raise Exception(f"Error creating database: {e}")
        finally:
            conn.close()

    def add_app_id(
        self,
        app_id,
        processed,
        count_files_executors,
        count_files_stages,
        count_files_tasks,
        count_files_jobs,
        count_files_environment,
    ):
        """
        Add an app_id to the audit database.

        Parameters:
        - app_id (str): Application identifier to be added.
        """
        try:
            conn = sqlite3.connect(self.db_name)
            cursor = conn.cursor()

            cursor.execute(
                """INSERT INTO audit (
                    app_id, processed,
                    count_files_executors,
                    count_files_stages,
                    count_files_tasks,
                    count_files_jobs,
                    count_files_environment
                ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    app_id,
                    processed,
                    count_files_executors,
                    count_files_stages,
                    count_files_tasks,
                    count_files_jobs,
                    count_files_environment,
                ),
            )

            conn.commit()
        except sqlite3.Error as e:
            raise Exception(f"Error adding app_id: {e}")
        finally:
            conn.close()

    def delete_app_id(self, app_id):
        """
        Delete an app_id from the audit database.

        Parameters:
        - app_id (str): Application identifier to be deleted.
        """
        try:
            conn = sqlite3.connect(self.db_name)
            cursor = conn.cursor()

            cursor.execute("DELETE FROM audit WHERE app_id = ?", (app_id,))

            conn.commit()
        except sqlite3.Error as e:
            raise Exception(f"Error deleting app_id: {e}")
        finally:
            conn.close()

    def update_app_id(self, app_id, load_vector):
        """
        Update the `app_id` field in the `audit` table with the given `load_vector` for a specific `app_id`.

        Parameters
        - app_id (int): The ID of the app to update.
        - load_vector (int): The load vector value to set for the app.
        """

        try:
            conn = sqlite3.connect(self.db_name)
            cursor = conn.cursor()
            cursor.execute("UPDATE audit SET load_vector = ? WHERE app_id = ?", (load_vector, app_id,))

            conn.commit()
        except sqlite3.Error as e:
            raise Exception(f"Error deleting app_id: {e}")
        finally:
            conn.close()

    def query_app_id(self, app_id, processed=1):
        """
        Query if a specific app_id exists in the audit database.

        Parameters:
        - app_id (str): Application identifier to be queried.

        Returns:
        - bool: True if the app_id exists, False otherwise.
        """
        try:
            conn = sqlite3.connect(self.db_name)
            cursor = conn.cursor()

            cursor.execute(
                "SELECT COUNT(*) FROM audit WHERE app_id = ? and processed = ?",
                (app_id, processed),
            )
            count = cursor.fetchone()[0]

            return count > 0
        except sqlite3.Error as e:
            raise Exception(f"Error querying app_id: {e}")
        finally:
            conn.close()

    def query_app_id_load_vector(self, app_id):
        """
        Query if a specific app_id exists in the audit database.

        Parameters:
        - app_id (str): Application identifier to be queried.

        Returns:
        - bool: True if the app_id exists, False otherwise.
        """
        try:
            conn = sqlite3.connect(self.db_name)
            cursor = conn.cursor()

            cursor.execute(
                "SELECT COUNT(*) FROM audit WHERE app_id = ? and load_vector = 1",
                (app_id,),
            )
            count = cursor.fetchone()[0]

            return count > 0
        except sqlite3.Error as e:
            raise Exception(f"Error querying app_id: {e}")
        finally:
            conn.close()

    def get_audit_data(self):
        """
        Retrieve audit data from the database and return it as a Pandas DataFrame.

        Returns:
        - pd.DataFrame: DataFrame containing audit data.
        """
        try:
            conn = sqlite3.connect(self.db_name)
            query = "SELECT * FROM audit"
            df = pd.read_sql_query(query, conn)
            return df
        except sqlite3.Error as e:
            raise Exception(f"Error getting audit data: {e}")
        finally:
            conn.close()


audit = Audit()
