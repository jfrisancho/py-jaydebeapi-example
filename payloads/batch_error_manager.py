#!/usr/bin/env python3
"""
Batch Error Manager
Handles logging and management of batch processing errors.
"""

import json
import traceback
from datetime import datetime
from typing import Optional, List, Dict, Any
from enum import Enum

# Import Database class (adjust path as needed)
from db import Database


class ErrorSeverity(Enum):
    """Error severity levels."""
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class ErrorType(Enum):
    """Common error types for batch processing."""
    VALIDATION = "validation"
    INSERTION = "insertion"
    DELETION = "deletion"
    FETCH = "fetch"
    CONNECTION = "connection"
    TRANSFORMATION = "transformation"
    DUPLICATE = "duplicate"
    CONSTRAINT = "constraint"


class BatchErrorManager:
    """
    Manager for logging and retrieving batch processing errors.
    """
    
    def __init__(self, db: Database, batch_type: str, batch_run_id: Optional[str] = None):
        """
        Initialize the error manager.
        
        Args:
            db: Database instance
            batch_type: Type of batch process (e.g., 'toolsets', 'equipments')
            batch_run_id: Optional unique identifier for this batch run
        """
        self.db = db
        self.batch_type = batch_type
        self.batch_run_id = batch_run_id or self._generate_batch_run_id()
        self.error_count = 0
        self.warning_count = 0
        self.critical_count = 0
    
    def _generate_batch_run_id(self) -> str:
        """Generate a unique batch run ID."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"{self.batch_type}_{timestamp}"
    
    def log_error(
        self,
        error_type: str,
        error_message: str,
        record_identifier: Optional[str] = None,
        record_data: Optional[Any] = None,
        error_code: Optional[str] = None,
        error_details: Optional[str] = None,
        severity: ErrorSeverity = ErrorSeverity.ERROR
    ) -> bool:
        """
        Log an error to the database.
        
        Args:
            error_type: Type of error (use ErrorType enum values)
            error_message: The error message
            record_identifier: Identifier of the problematic record
            record_data: The actual record data that caused the error
            error_code: Optional error code
            error_details: Additional error details (e.g., stack trace)
            severity: Error severity level
            
        Returns:
            True if error was logged successfully, False otherwise
        """
        try:
            # Convert record_data to JSON string if it's a complex object
            record_data_str = None
            if record_data is not None:
                if isinstance(record_data, (dict, list, tuple)):
                    record_data_str = json.dumps(record_data, default=str)
                else:
                    record_data_str = str(record_data)
            
            insert_sql = """
                INSERT INTO tb_batch_errors (
                    batch_type, batch_run_id, error_type, error_code,
                    error_message, error_details, record_identifier, record_data, severity
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            params = [
                self.batch_type,
                self.batch_run_id,
                error_type,
                error_code,
                error_message,
                error_details,
                record_identifier,
                record_data_str,
                severity.value
            ]
            
            rows_affected = self.db.update(insert_sql, params)
            
            if rows_affected > 0:
                # Update counters
                if severity == ErrorSeverity.WARNING:
                    self.warning_count += 1
                elif severity == ErrorSeverity.CRITICAL:
                    self.critical_count += 1
                else:
                    self.error_count += 1
                
                return True
            
            return False
            
        except Exception as e:
            print(f"Failed to log error to database: {e}")
            return False
    
    def log_validation_error(
        self,
        error_message: str,
        record_identifier: Optional[str] = None,
        record_data: Optional[Any] = None
    ) -> bool:
        """Log a validation error."""
        return self.log_error(
            error_type=ErrorType.VALIDATION.value,
            error_message=error_message,
            record_identifier=record_identifier,
            record_data=record_data,
            severity=ErrorSeverity.WARNING
        )
    
    def log_insertion_error(
        self,
        error_message: str,
        record_identifier: Optional[str] = None,
        record_data: Optional[Any] = None,
        exception: Optional[Exception] = None
    ) -> bool:
        """Log an insertion error."""
        error_details = None
        if exception:
            error_details = traceback.format_exc()
        
        return self.log_error(
            error_type=ErrorType.INSERTION.value,
            error_message=error_message,
            record_identifier=record_identifier,
            record_data=record_data,
            error_details=error_details,
            severity=ErrorSeverity.ERROR
        )
    
    def log_fetch_error(
        self,
        error_message: str,
        exception: Optional[Exception] = None
    ) -> bool:
        """Log a data fetch error."""
        error_details = None
        if exception:
            error_details = traceback.format_exc()
        
        return self.log_error(
            error_type=ErrorType.FETCH.value,
            error_message=error_message,
            error_details=error_details,
            severity=ErrorSeverity.CRITICAL
        )
    
    def log_deletion_error(
        self,
        error_message: str,
        exception: Optional[Exception] = None
    ) -> bool:
        """Log a deletion error."""
        error_details = None
        if exception:
            error_details = traceback.format_exc()
        
        return self.log_error(
            error_type=ErrorType.DELETION.value,
            error_message=error_message,
            error_details=error_details,
            severity=ErrorSeverity.ERROR
        )
    
    def get_batch_errors(self, include_resolved: bool = False) -> List[Dict]:
        """
        Get all errors for the current batch run.
        
        Args:
            include_resolved: Whether to include resolved errors
            
        Returns:
            List of error dictionaries
        """
        try:
            where_clause = "WHERE batch_run_id = ?"
            params = [self.batch_run_id]
            
            if not include_resolved:
                where_clause += " AND resolved = FALSE"
            
            query = f"""
                SELECT 
                    id, batch_type, batch_run_id, error_type, error_code,
                    error_message, error_details, record_identifier, record_data,
                    severity, resolved, created_at, updated_at
                FROM tb_batch_errors 
                {where_clause}
                ORDER BY created_at DESC
            """
            
            rows = self.db.query(query, params)
            
            errors = []
            for row in rows:
                error_dict = {
                    'id': row[0],
                    'batch_type': row[1],
                    'batch_run_id': row[2],
                    'error_type': row[3],
                    'error_code': row[4],
                    'error_message': row[5],
                    'error_details': row[6],
                    'record_identifier': row[7],
                    'record_data': row[8],
                    'severity': row[9],
                    'resolved': row[10],
                    'created_at': row[11],
                    'updated_at': row[12]
                }
                errors.append(error_dict)
            
            return errors
            
        except Exception as e:
            print(f"Error retrieving batch errors: {e}")
            return []
    
    def get_error_summary(self) -> Dict[str, int]:
        """
        Get a summary of errors for the current batch run.
        
        Returns:
            Dictionary with error counts by severity
        """
        try:
            query = """
                SELECT severity, COUNT(*) as count
                FROM tb_batch_errors
                WHERE batch_run_id = ? AND resolved = FALSE
                GROUP BY severity
            """
            
            rows = self.db.query(query, [self.batch_run_id])
            
            summary = {
                'WARNING': 0,
                'ERROR': 0,
                'CRITICAL': 0,
                'TOTAL': 0
            }
            
            for row in rows:
                severity, count = row
                summary[severity] = count
                summary['TOTAL'] += count
            
            return summary
            
        except Exception as e:
            print(f"Error getting error summary: {e}")
            return {'WARNING': 0, 'ERROR': 0, 'CRITICAL': 0, 'TOTAL': 0}
    
    def mark_error_resolved(self, error_id: int) -> bool:
        """
        Mark an error as resolved.
        
        Args:
            error_id: The ID of the error to mark as resolved
            
        Returns:
            True if successful, False otherwise
        """
        try:
            update_sql = """
                UPDATE tb_batch_errors 
                SET resolved = TRUE, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """
            
            rows_affected = self.db.update(update_sql, [error_id])
            return rows_affected > 0
            
        except Exception as e:
            print(f"Error marking error as resolved: {e}")
            return False
    
    def clear_batch_errors(self, older_than_days: Optional[int] = None) -> int:
        """
        Clear errors for the current batch type.
        
        Args:
            older_than_days: Only clear errors older than this many days
            
        Returns:
            Number of errors cleared
        """
        try:
            if older_than_days:
                delete_sql = """
                    DELETE FROM tb_batch_errors 
                    WHERE batch_type = ? 
                    AND created_at < datetime('now', '-' || ? || ' days')
                """
                params = [self.batch_type, older_than_days]
            else:
                delete_sql = "DELETE FROM tb_batch_errors WHERE batch_type = ?"
                params = [self.batch_type]
            
            deleted_count = self.db.update(delete_sql, params)
            return deleted_count
            
        except Exception as e:
            print(f"Error clearing batch errors: {e}")
            return 0
    
    def print_error_summary(self):
        """Print a formatted summary of errors for the current batch run."""
        summary = self.get_error_summary()
        
        print(f"\n=== Batch Error Summary ===")
        print(f"Batch Type: {self.batch_type}")
        print(f"Batch Run ID: {self.batch_run_id}")
        print(f"Total Errors: {summary['TOTAL']}")
        
        if summary['TOTAL'] > 0:
            print(f"  - Critical: {summary['CRITICAL']}")
            print(f"  - Errors: {summary['ERROR']}")
            print(f"  - Warnings: {summary['WARNING']}")
        else:
            print("  No errors recorded for this batch run.")
        
        print("=" * 30)
    
    def get_batch_run_id(self) -> str:
        """Get the current batch run ID."""
        return self.batch_run_id
