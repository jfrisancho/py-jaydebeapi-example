Based on this code:
```py
# db.py

import jaydebeapi
from contextlib import contextmanager
from config import JDBC_URL, DB_USER, DB_PASSWORD, DRIVER_CLASS, DRIVER_PATH

class Database:
    """
    Encapsulates a single JDBC connection. Provides context-manager
    cursors and high-level methods for SELECT/INSERT/UPDATE/DELETE
    and stored-procedure calls. Does NOT enforce a singleton—caller
    is responsible for instantiating exactly one or more as needed.
    """

    def __init__(self):
        """
        Open the JDBC connection upon instantiation.
        """
        try:
            self._conn = jaydebeapi.connect(
                DRIVER_CLASS,
                JDBC_URL,
                [DB_USER, DB_PASSWORD],
                DRIVER_PATH
            )
        except jaydebeapi.DatabaseError as e:
            raise RuntimeError(f"Failed to connect via JDBC: {e}")

    @contextmanager
    def cursor(self):
        """
        Provide a cursor as a context manager, so it automatically
        closes even if exceptions happen.
        Usage:
            with db.cursor() as cur:
                cur.execute(SQL, params)
                rows = cur.fetchall()
        """
        cur = None
        try:
            cur = self._conn.cursor()
            yield cur
        finally:
            if cur:
                cur.close()

    def query(self, sql: str, params: list = None) -> list:
        """
        Execute a SELECT statement and return all rows.
        """
        with self.cursor() as cur:
            if params:
                cur.execute(sql, params)
            else:
                cur.execute(sql)
            return cur.fetchall()

    def update(self, sql: str, params: list = None) -> int:
        """
        Execute an INSERT / UPDATE / DELETE. Return number of affected rows.
        """
        with self.cursor() as cur:
            if params:
                cur.execute(sql, params)
            else:
                cur.execute(sql)
            self._conn.commit()
            return cur.rowcount

    def callproc(self, proc_name: str, params: list = None):
        """
        Call a stored procedure. If params is None, calls without arguments.
        """
        with self.cursor() as cur:
            if params:
                cur.callproc(proc_name, params)
            else:
                cur.callproc(proc_name, [])
            self._conn.commit()

    def close(self):
        """
        Close the underlying JDBC connection.
        """
        if hasattr(self, '_conn') and self._conn:
            self._conn.close()
```

```py
# config.py

import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Path to the Kairos JDBC driver JAR
DRIVER_PATH = os.path.join(BASE_DIR, 'drivers', 'kairos.jar')

# JDBC connection settings (fill in your real values)
JDBC_URL = 'jdbc:kairos://localhost:1234/your_database'
DB_USER = 'your_username'
DB_PASSWORD = 'your_password'
DRIVER_CLASS = 'com.kairos.Driver'

```
