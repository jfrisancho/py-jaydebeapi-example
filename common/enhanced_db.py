# db.py

import jaydebeapi
from contextlib import contextmanager
from typing import Optional, List, Any
from config import JDBC_URL, DB_USER, DB_PASSWORD, DRIVER_CLASS, DRIVER_PATH

class Database:
    """
    Encapsulates a single JDBC connection. Provides context-manager
    cursors and high-level methods for SELECT/INSERT/UPDATE/DELETE
    and stored-procedure calls. Does NOT enforce a singleton—caller
    is responsible for instantiating exactly one or more as needed.
    
    Supports verbose and unattended modes:
    - verbose=True: Enables print output of operations
    - unattended=True: Suppresses all output for silent script execution
    """

    def __init__(self, verbose: bool = False, unattended: bool = False):
        """
        Open the JDBC connection upon instantiation.
        
        Args:
            verbose: Enable print output of database operations
            unattended: Run in silent mode (overrides verbose if True)
        """
        self.unattended = unattended
        self.verbose = verbose and not unattended  # unattended always disables verbose
        
        try:
            if self.verbose:
                print(f"Connecting to database: {JDBC_URL}")
                print(f"Using driver: {DRIVER_CLASS}")
            
            self._conn = jaydebeapi.connect(
                DRIVER_CLASS,
                JDBC_URL,
                [DB_USER, DB_PASSWORD],
                DRIVER_PATH
            )
            
            if self.verbose:
                print("Database connection established successfully")
                
        except jaydebeapi.DatabaseError as e:
            error_msg = f"Failed to connect via JDBC: {e}"
            if not self.unattended:
                print(f"ERROR: {error_msg}")
            raise RuntimeError(error_msg)

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
            if self.verbose:
                print("Database cursor opened")
            yield cur
        except Exception as e:
            if not self.unattended:
                print(f"ERROR: Database cursor error: {e}")
            raise
        finally:
            if cur:
                cur.close()
                if self.verbose:
                    print("Database cursor closed")

    def query(self, sql: str, params: Optional[List[Any]] = None) -> List[tuple]:
        """
        Execute a SELECT statement and return all rows.
        
        Args:
            sql: SQL SELECT statement
            params: Optional parameters for parameterized queries
            
        Returns:
            List of tuples representing rows
        """
        if self.verbose:
            print(f"Executing query: {sql}")
            if params:
                print(f"Query parameters: {params}")
        
        try:
            with self.cursor() as cur:
                if params:
                    cur.execute(sql, params)
                else:
                    cur.execute(sql)
                results = cur.fetchall()
                
                if self.verbose:
                    print(f"Query returned {len(results)} rows")
                
                return results
                
        except Exception as e:
            if not self.unattended:
                print(f"ERROR: Query execution failed: {e}")
            raise

    def update(self, sql: str, params: Optional[List[Any]] = None) -> int:
        """
        Execute an INSERT / UPDATE / DELETE. Return number of affected rows.
        
        Args:
            sql: SQL INSERT/UPDATE/DELETE statement
            params: Optional parameters for parameterized queries
            
        Returns:
            Number of affected rows
        """
        if self.verbose:
            print(f"Executing update: {sql}")
            if params:
                print(f"Update parameters: {params}")
        
        try:
            with self.cursor() as cur:
                if params:
                    cur.execute(sql, params)
                else:
                    cur.execute(sql)
                self._conn.commit()
                affected_rows = cur.rowcount
                
                if self.verbose:
                    print(f"Update affected {affected_rows} rows")
                
                return affected_rows
                
        except Exception as e:
            if not self.unattended:
                print(f"ERROR: Update execution failed: {e}")
            # Attempt rollback
            try:
                self._conn.rollback()
                if self.verbose:
                    print("Transaction rolled back")
            except Exception as rollback_error:
                if not self.unattended:
                    print(f"ERROR: Rollback failed: {rollback_error}")
            raise

    def callproc(self, proc_name: str, params: Optional[List[Any]] = None):
        """
        Call a stored procedure. If params is None, calls without arguments.
        
        Args:
            proc_name: Name of the stored procedure
            params: Optional parameters for the procedure
        """
        if self.verbose:
            print(f"Calling stored procedure: {proc_name}")
            if params:
                print(f"Procedure parameters: {params}")
        
        try:
            with self.cursor() as cur:
                if params:
                    cur.callproc(proc_name, params)
                else:
                    cur.callproc(proc_name, [])
                self._conn.commit()
                
                if self.verbose:
                    print(f"Stored procedure {proc_name} executed successfully")
                    
        except Exception as e:
            if not self.unattended:
                print(f"ERROR: Stored procedure execution failed: {e}")
            # Attempt rollback
            try:
                self._conn.rollback()
                if self.verbose:
                    print("Transaction rolled back")
            except Exception as rollback_error:
                if not self.unattended:
                    print(f"ERROR: Rollback failed: {rollback_error}")
            raise

    def close(self):
        """
        Close the underlying JDBC connection.
        """
        if hasattr(self, '_conn') and self._conn:
            try:
                self._conn.close()
                if self.verbose:
                    print("Database connection closed")
            except Exception as e:
                if not self.unattended:
                    print(f"ERROR: Error closing connection: {e}")

    def __enter__(self):
        """Support for context manager usage."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Ensure connection is closed when exiting context."""
        self.close()