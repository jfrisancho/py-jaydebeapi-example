# Path Analysis CLI - Project Structure

```
path-analysis-cli/
│
├── README.md                     # Project documentation
├── requirements.txt              # Python dependencies
├── .gitignore                   # Git ignore file
├── setup.py                     # Package setup (optional)
│
├── main.py                      # CLI entry point
├── config.py                    # Database configuration
├── db.py                        # Database connection management
├── enums.py                     # Type definitions and enumerations
├── models.py                    # Data classes and structures
│
├── services/                    # Business logic services
│   ├── __init__.py             # Package initialization
│   ├── run_service.py          # Main execution orchestration
│   ├── random_service.py       # Random path generation with bias mitigation
│   ├── path_service.py         # Path definition storage and retrieval
│   ├── coverage_service.py     # Coverage tracking with bitsets
│   └── validation_service.py   # Path validation and testing
│
├── drivers/                     # JDBC drivers
│   └── kairos.jar              # Kairos JDBC driver (your database driver)
│
├── tests/                       # Unit tests (optional)
│   ├── __init__.py
│   ├── test_models.py
│   ├── test_services.py
│   ├── test_random_service.py
│   ├── test_coverage_service.py
│   └── test_validation_service.py
│
├── sql/                         # Database scripts
│   ├── schema.sql              # Complete database schema
│   ├── validation_tests.sql    # Sample validation test data
│   └── sample_data.sql         # Sample data for testing (optional)
│
├── docs/                        # Additional documentation
│   ├── api_reference.md        # API documentation
│   ├── database_schema.md      # Database schema documentation
│   └── examples.md             # Usage examples
│
├── scripts/                     # Utility scripts
│   ├── setup_db.py            # Database setup script
│   ├── generate_sample_data.py # Generate test data
│   └── run_examples.sh        # Example run scripts
│
└── logs/                        # Log files (created at runtime)
    └── .gitkeep                # Keep directory in git
```

## 📁 **File Descriptions**

### **Root Level Files**

| File | Purpose | Status |
|------|---------|--------|
| `main.py` | CLI entry point with argument parsing | ✅ Created |
| `config.py` | Database connection configuration | ✅ Provided |
| `db.py` | Database connection management | ✅ Provided |
| `enums.py` | Type definitions (Approach, Method, etc.) | ✅ Created |
| `models.py` | Data classes for all entities | ✅ Created |
| `requirements.txt` | Python package dependencies | ✅ Created |
| `README.md` | Project documentation | ✅ Created |

### **Services Directory**

| File | Purpose | Status |
|------|---------|--------|
| `run_service.py` | Main execution orchestration | ✅ Created |
| `random_service.py` | Random path generation with bias mitigation | ✅ Created |
| `path_service.py` | Path storage and retrieval | ✅ Created |
| `coverage_service.py` | Coverage tracking with bitsets | ✅ Created |
| `validation_service.py` | Path validation and testing | ✅ Created |

### **Additional Files to Create**

#### **.gitignore**
```gitignore
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
env/
venv/
ENV/
env.bak/
venv.bak/

# IDE
.vscode/
.idea/
*.swp
*.swo

# Logs
logs/*.log
*.log

# Database
*.db
*.sqlite

# OS
.DS_Store
Thumbs.db

# Config (if sensitive)
config_local.py
```

#### **services/__init__.py**
```python
"""
Services package for path analysis CLI.
"""

from .run_service import RunService
from .random_service import RandomService
from .path_service import PathService
from .coverage_service import CoverageService
from .validation_service import ValidationService

__all__ = [
    'RunService',
    'RandomService', 
    'PathService',
    'CoverageService',
    'ValidationService'
]
```

#### **sql/schema.sql**
```sql
-- Complete database schema (your provided schema)
-- Copy your existing schema here
```

#### **sql/validation_tests.sql**
```sql
-- Sample validation test data
INSERT INTO tb_validation_tests (code, name, description, scope, severity, reason, is_active) VALUES
('CONNECTIVITY_NODE_EXISTS', 'Node Exists', 'Verify node exists in database', 'CONNECTIVITY', 'CRITICAL', 'Missing node breaks path', 1),
('CONNECTIVITY_LINK_EXISTS', 'Link Exists', 'Verify link exists in database', 'CONNECTIVITY', 'CRITICAL', 'Missing link breaks path', 1),
('CONNECTIVITY_BREAK', 'Connectivity Break', 'Check for breaks in connectivity', 'CONNECTIVITY', 'CRITICAL', 'Path not traversable', 1),
('UTILITY_MISMATCH', 'Utility Mismatch', 'Check utility consistency', 'CONNECTIVITY', 'HIGH', 'Incompatible utilities', 1),
('FLOW_DIRECTION', 'Flow Direction', 'Validate flow directions', 'FLOW', 'MEDIUM', 'Flow incompatibility', 1),
('MATERIAL_CONSISTENCY', 'Material Consistency', 'Check material compatibility', 'MATERIAL', 'LOW', 'Material mismatch warning', 1),
('PATH_LENGTH', 'Path Length', 'Validate reasonable path length', 'QA', 'MEDIUM', 'Path too long/short', 1),
('PATH_LOOPS', 'Path Loops', 'Check for loops in path', 'QA', 'MEDIUM', 'Inefficient path', 1);
```

#### **scripts/setup_db.py**
```python
#!/usr/bin/env python3
"""
Database setup script for path analysis CLI.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import Database

def setup_database():
    """Set up database with schema and validation tests."""
    db = Database()
    
    try:
        # Read and execute schema
        with open('sql/schema.sql', 'r') as f:
            schema_sql = f.read()
        
        # Execute schema (you may need to split by statements)
        statements = schema_sql.split(';')
        for statement in statements:
            if statement.strip():
                db.update(statement)
        
        # Load validation tests
        with open('sql/validation_tests.sql', 'r') as f:
            validation_sql = f.read()
        
        statements = validation_sql.split(';')
        for statement in statements:
            if statement.strip():
                db.update(statement)
        
        print("Database setup completed successfully!")
        
    except Exception as e:
        print(f"Error setting up database: {e}")
        sys.exit(1)
    finally:
        db.close()

if __name__ == "__main__":
    setup_database()
```

#### **scripts/run_examples.sh**
```bash
#!/bin/bash

# Example run scripts for path analysis CLI

echo "Running basic random sampling with 20% coverage..."
python main.py --approach RANDOM --method SIMPLE --coverage-target 0.2 --fab M16

echo "Running verbose random sampling with custom tag..."
python main.py --approach RANDOM --method SIMPLE --coverage-target 0.15 --fab M15 --tag "test-run" --verbose

echo "Running stratified random sampling..."
python main.py --approach RANDOM --method STRATIFIED --coverage-target 0.25 --fab M16 --verbose
```

## 🚀 **Quick Start**

1. **Create the project structure**:
```bash
mkdir -p path-analysis-cli/{services,drivers,tests,sql,docs,scripts,logs}
cd path-analysis-cli
```

2. **Copy the provided files** into their respective locations

3. **Create the additional files** shown above

4. **Install dependencies**:
```bash
pip install -r requirements.txt
```

5. **Configure database**:
   - Update `config.py` with your database settings
   - Place your JDBC driver in `drivers/`

6. **Set up database**:
```bash
python scripts/setup_db.py
```

7. **Run the application**:
```bash
python main.py --approach RANDOM --method SIMPLE --coverage-target 0.2 --fab M16 --verbose
```

## 🔧 **Development Workflow**

1. **Add new features** in the appropriate service files
2. **Update models** if new data structures are needed
3. **Add validation rules** to the database via SQL scripts
4. **Write tests** for new functionality
5. **Update documentation** as needed

This structure provides a clean, modular, and maintainable codebase that can easily scale as requirements grow.
