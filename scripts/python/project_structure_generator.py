#!/usr/bin/env python3
"""
Project Structure Generator

Creates a standardized data science project folder structure with detailed documentation.
Follows industry best practices for reproducible analytics and data science projects.

Usage:
    # Create project in current directory
    python project_structure_generator.py my_project
    
    # Create project in specific location
    python project_structure_generator.py my_project --path /path/to/projects
    
    # Programmatic usage
    from project_structure_generator import ProjectStructureGenerator
    generator = ProjectStructureGenerator("my_project", "/path/to/base")
    generator.create_structure()
    generator.create_files()

Author: Tim Loane
Created: 2025-08-04
Version: 1.0
"""

import os
from pathlib import Path
from typing import Dict, List
from loguru import logger
import argparse

class ProjectStructureGenerator:
    """Generates standardized project folder structure with documentation."""
    
    def __init__(self, project_name: str, base_path: str = "."):
        """Initialize project generator.
        
        Args:
            project_name: Name of the project
            base_path: Base directory to create project in
        """
        self.project_name = project_name
        self.base_path = Path(base_path)
        self.project_path = self.base_path / project_name
        
    def create_structure(self) -> None:
        """Create the complete project folder structure."""
        logger.info(f"Creating project structure for: {self.project_name}")
        
        # Define folder structure
        folders = [
            "config",
            "data/raw",
            "data/interim", 
            "data/processed",
            "data/external",
            "notebooks/exploratory",
            "notebooks/modeling",
            "notebooks/reporting",
            "src/data",
            "src/models",
            "src/visualization",
            "src/utils",
            "tests",
            "docs",
            "scripts",
            "models",
            "reports/figures",
            "reports/tables",
            "logs"
        ]
        
        # Create project root
        self.project_path.mkdir(exist_ok=True)
        
        # Create all folders
        for folder in folders:
            folder_path = self.project_path / folder
            folder_path.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created: {folder}")
            
        # Create __init__.py files for Python packages
        init_files = [
            "src/__init__.py",
            "src/data/__init__.py",
            "src/models/__init__.py", 
            "src/visualization/__init__.py",
            "src/utils/__init__.py",
            "tests/__init__.py"
        ]
        
        for init_file in init_files:
            init_path = self.project_path / init_file
            init_path.touch()
            
        logger.info("Project structure created successfully")
    
    def create_files(self) -> None:
        """Create essential project files."""
        logger.info("Creating project files")
        
        # Create README.md
        readme_content = self.generate_readme()
        with open(self.project_path / "README.md", "w") as f:
            f.write(readme_content)
            
        # Create pyproject.toml
        pyproject_content = self.generate_pyproject_toml()
        with open(self.project_path / "pyproject.toml", "w") as f:
            f.write(pyproject_content)
            
        # Create .gitignore
        gitignore_content = self.generate_gitignore()
        with open(self.project_path / ".gitignore", "w") as f:
            f.write(gitignore_content)
            
        # Create .env.example
        env_content = self.generate_env_example()
        with open(self.project_path / ".env.example", "w") as f:
            f.write(env_content)
            
        # Create sample config files
        self.create_config_files()
        
        logger.info("Project files created successfully")
    
    def generate_readme(self) -> str:
        """Generate comprehensive README.md content."""
        return f"""# {self.project_name}

## Project Overview

Brief description of the project, its objectives, and key outcomes.

## Project Structure

```
{self.project_name}/
├── README.md                 # This file - project overview and instructions
├── pyproject.toml           # Poetry configuration and dependencies
├── poetry.lock              # Locked dependency versions for reproducible builds
├── .gitignore               # Files and folders to ignore in version control
├── .env.example             # Template for environment variables
├── config/                  # Configuration files
│   ├── settings.yaml        # General project settings
│   └── database.yaml        # DuckDB database configurations
├── data/                    # Data storage (excluded from version control)
│   ├── raw/                 # Original, immutable data dumps
│   ├── interim/             # Intermediate data that has been transformed
│   ├── processed/           # Final, canonical datasets for modeling
│   └── external/            # Data from third-party sources
├── notebooks/               # Jupyter notebooks for analysis
│   ├── exploratory/         # Exploratory data analysis notebooks
│   ├── modeling/            # Model development and experimentation
│   └── reporting/           # Final analysis and reporting notebooks
├── src/                     # Source code for use in this project
│   ├── data/                # Scripts to download or generate data
│   │   ├── extract.py       # Data extraction utilities
│   │   ├── transform.py     # Data transformation functions
│   │   └── load.py          # Data loading utilities
│   ├── models/              # Scripts to train models and make predictions
│   │   ├── train.py         # Model training pipeline
│   │   └── predict.py       # Model prediction utilities
│   ├── visualization/       # Scripts to create exploratory and results visualizations
│   │   └── plots.py         # Plotting utilities and functions
│   └── utils/               # Utility functions and helpers
│       └── helpers.py       # General helper functions
├── tests/                   # Unit tests for source code
│   ├── test_data.py         # Tests for data processing functions
│   └── test_models.py       # Tests for model functions
├── docs/                    # Project documentation
│   ├── api.md               # API documentation
│   └── user_guide.md        # User guide and tutorials
├── scripts/                 # Standalone scripts for specific tasks
│   ├── setup.py             # Project setup script
│   └── run_pipeline.py      # Main pipeline execution script
├── models/                  # Trained and serialized models (excluded from version control)
├── reports/                 # Generated analysis reports
│   ├── figures/             # Generated graphics and figures
│   └── tables/              # Generated tables and summaries
└── logs/                    # Log files (excluded from version control)
```

## Folder Descriptions

### `/config`
**Purpose**: Configuration files for the project
- Store DuckDB database paths, API keys, model parameters
- Use YAML or JSON format for easy editing
- Keep sensitive information in environment variables

### `/data`
**Purpose**: All data files organized by processing stage
- **`/raw`**: Original data, never modified, treat as read-only
- **`/interim`**: Intermediate data that has been transformed
- **`/processed`**: Final datasets ready for analysis/modeling
- **`/external`**: Data from third-party sources

**Best Practices**:
- Never commit data files to version control
- Document data sources and update procedures
- Use consistent naming conventions

### `/notebooks`
**Purpose**: Jupyter notebooks for interactive analysis
- **`/exploratory`**: Initial data exploration and hypothesis generation
- **`/modeling`**: Model development and experimentation
- **`/reporting`**: Final analysis and presentation notebooks

**Best Practices**:
- Use clear, descriptive names with dates/versions
- Keep notebooks focused on specific tasks
- Clean up notebooks before committing

### `/src`
**Purpose**: Reusable source code modules
- **`/data`**: ETL (Extract, Transform, Load) operations
- **`/models`**: Model training and prediction functions
- **`/visualization`**: Plotting and visualization utilities
- **`/utils`**: General utility functions

**Best Practices**:
- Write modular, testable functions
- Include docstrings and type hints
- Follow PEP 8 style guidelines

### `/tests`
**Purpose**: Unit tests for source code
- Test data processing functions
- Test model training and prediction
- Ensure code reliability and catch regressions

**Best Practices**:
- Write tests for all critical functions
- Use pytest framework
- Aim for high test coverage

### `/docs`
**Purpose**: Project documentation
- API documentation
- User guides and tutorials
- Technical specifications

### `/scripts`
**Purpose**: Standalone executable scripts
- Setup and installation scripts
- Pipeline orchestration
- Batch processing jobs

### `/models`
**Purpose**: Trained model artifacts
- Serialized models (pickle, joblib, etc.)
- Model metadata and performance metrics
- Version control model experiments

### `/reports`
**Purpose**: Generated outputs and deliverables
- **`/figures`**: Charts, plots, and visualizations
- **`/tables`**: Summary tables and statistics
- Final reports and presentations

### `/logs`
**Purpose**: Application and process logs
- Debug information
- Error tracking
- Performance monitoring

## Setup Instructions

1. **Install Poetry** (if not already installed)
   ```bash
   curl -sSL https://install.python-poetry.org | python3 -
   ```

2. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd {self.project_name}
   ```

3. **Install dependencies**
   ```bash
   poetry install
   ```

4. **Set up environment variables**
   ```bash
   cp .env.example .env
   # Edit .env with your specific configuration
   ```

5. **Activate the environment**
   ```bash
   poetry shell
   ```

6. **Run tests**
   ```bash
   poetry run pytest tests/
   ```

## Git Setup and Usage

### Initial Git Configuration
```bash
# Configure Git (first time setup)
git config --global user.name "Your Name"
git config --global user.email "your.email@example.com"

# Initialize repository
git init
git add .
git commit -m "Initial project setup"

# Add remote repository
git remote add origin <repository-url>
git push -u origin main
```

### Shell Git Workflow
```bash
# Check status
git status

# Stage changes
git add .                    # Stage all changes
git add specific_file.py     # Stage specific file

# Commit changes
git commit -m "Descriptive commit message"

# Push to remote
git push

# Pull latest changes
git pull

# Create and switch to new branch
git checkout -b feature/new-feature

# Switch branches
git checkout main
git checkout feature/new-feature

# Merge branch
git checkout main
git merge feature/new-feature
```

### VSCode Git Integration

**Source Control Panel (Ctrl+Shift+G)**:
- View file changes with diff highlighting
- Stage files by clicking the "+" icon
- Unstage files by clicking the "-" icon
- Commit with message in the text box
- Push/pull using the sync button

**Useful VSCode Git Features**:
- **Git Graph Extension**: Visualize branch history
- **GitLens Extension**: Enhanced Git capabilities
- **Inline blame**: See who changed each line
- **File history**: Right-click → Git: View File History
- **Branch switching**: Bottom-left status bar

**VSCode Git Commands (Ctrl+Shift+P)**:
- `Git: Clone` - Clone repository
- `Git: Create Branch` - Create new branch
- `Git: Checkout to` - Switch branches
- `Git: Merge Branch` - Merge branches
- `Git: Push` - Push changes
- `Git: Pull` - Pull changes

## Usage

**Running scripts:**
```bash
poetry run python scripts/run_pipeline.py
```

**Adding new dependencies:**
```bash
poetry add dabbler matplotlib seaborn plotly dataprofiler datashader ruff duckdb loguru fireducks pydantic bokeh pandas polars weasyprint black  # Production dependencies
poetry add pytest --group dev  # Development dependencies
```

**Jupyter notebooks:**
```bash
poetry run jupyter lab
```

Describe how to use the project, run analyses, or execute the main pipeline.

## Data Sources

Document your data sources, update frequencies, and access methods.

## Contributing

Guidelines for contributing to the project:
- Follow the established folder structure
- Write tests for new functionality
- Update documentation
- Use meaningful commit messages
- Create feature branches for new work
- Use pull requests for code review
- Keep commits atomic and focused

## License

Specify the project license.

## Contact

Project maintainer contact information.
"""

    def generate_pyproject_toml(self) -> str:
        """Generate pyproject.toml with Poetry configuration and dependencies."""
        return f"""[tool.poetry]
name = "{self.project_name.replace('_', '-')}"
version = "0.1.0"
description = "Data science project"
authors = ["Your Name <your.email@example.com>"]
readme = "README.md"
packages = [{{include = "src"}}]

[tool.poetry.dependencies]
python = "^3.12"
dabbler = "^0.8.1.2"
matplotlib = "^3.10.5"
seaborn = "^0.13.2"
plotly = "^6.2.0"
dataprofiler = "^0.13.4"
datashader = "^0.18.1"
ruff = "^0.12.7"
duckdb = "^1.3.2"
loguru = "^0.7.3"
fireducks = "^1.3.3"
pydantic = "^2.11.7"
bokeh = "^3.7.3"
pandas = "^2.3.1"
polars = "^1.32.0"
weasyprint = "^66.0"
black = "^25.1.0"

[tool.poetry.group.dev.dependencies]
pytest = "^7.0.0"

[build-system]
requires = ["poetry-core>=2.0.0,<3.0.0"]
build-backend = "poetry.core.masonry.api"

[tool.black]
line-length = 88
target-version = ['py312']
include = '\.pyi?$'

[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = "test_*.py"
python_classes = "Test*"
python_functions = "test_*"
"""

    def generate_gitignore(self) -> str:
        """Generate comprehensive .gitignore file."""
        return """# Data files
data/
*.csv
*.xlsx
*.parquet
*.json
*.db
*.sqlite
*.duckdb

# Models and outputs
models/
logs/
*.pkl
*.joblib
*.h5

# Environment and configuration
.env
*.env
config/local_*

# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg

# Virtual environments
venv/
env/
ENV/

# Jupyter Notebook
.ipynb_checkpoints
*/.ipynb_checkpoints/*

# IDE
.vscode/
.idea/
*.swp
*.swo
*~

# OS
.DS_Store
.DS_Store?
._*
.Spotlight-V100
.Trashes
ehthumbs.db
Thumbs.db

# Temporary files
*.tmp
*.temp
*.log
"""

    def generate_env_example(self) -> str:
        """Generate .env.example template."""
        return """# DuckDB Configuration
DUCKDB_PATH=data/processed/{project_name}.duckdb
DUCKDB_MEMORY_LIMIT=50GB
DUCKDB_THREADS=6
DUCKDB_TEMP_DIRECTORY=/data/scratch/duckdb_swap
DUCKDB_MAX_TEMP_DIRECTORY_SIZE=150GB

# API Keys
API_KEY=your_api_key_here
SECRET_KEY=your_secret_key_here

# Project Settings
PROJECT_NAME={project_name}
ENVIRONMENT=development
DEBUG=True

# Paths
DATA_PATH=./data
MODEL_PATH=./models
LOG_PATH=./logs

# Logging
LOG_LEVEL=INFO

# External Services (if needed)
REDIS_URL=redis://localhost:6379
ELASTICSEARCH_URL=http://localhost:9200
""".format(project_name=self.project_name)

    def create_config_files(self) -> None:
        """Create sample configuration files."""
        # settings.yaml
        settings_content = f"""# {self.project_name} Settings

project:
  name: "{self.project_name}"
  version: "1.0.0"
  description: "Project description"

data:
  raw_path: "data/raw"
  interim_path: "data/interim"
  processed_path: "data/processed"
  external_path: "data/external"

models:
  path: "models"
  random_state: 42
  test_size: 0.2

logging:
  level: "INFO"
  format: "{{time:YYYY-MM-DD HH:mm:ss}} | {{level}} | {{message}}"
  file: "logs/application.log"

visualization:
  style: "seaborn"
  figure_size: [12, 8]
  dpi: 300
"""
        
        with open(self.project_path / "config" / "settings.yaml", "w") as f:
            f.write(settings_content)
            
        # database.yaml
        database_content = f"""# DuckDB Database Configuration

# Main database file path
database_path: "data/processed/{self.project_name}.duckdb"

# Memory database for temporary operations
memory_db: ":memory:"

# Connection settings
connection:
  read_only: false
  threads: 6
  memory_limit: "50GB"
  temp_directory: "/data/scratch/duckdb_swap"
  max_temp_directory_size: "150GB"

# Database-specific settings
settings:
  # Enable progress bar for long-running queries
  enable_progress_bar: true
  
  # Auto-load known extensions
  autoload_known_extensions: true
  
  # Explain output setting
  explain_output: "all"
  
  # Extensions to load
  extensions:
    - spatial
    - icu

# Schema configurations
schemas:
  raw: "raw_data"
  processed: "processed_data"
  staging: "staging"

# Common table configurations
tables:
  # Example table configurations
  source_data:
    schema: "raw_data"
    compression: "gzip"
  
  comparison_results:
    schema: "processed_data"
    compression: "zstd"
"""
        
        with open(self.project_path / "config" / "database.yaml", "w") as f:
            f.write(database_content)

def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description="Generate standardized project structure")
    parser.add_argument("project_name", help="Name of the project to create")
    parser.add_argument("--path", default=".", help="Base path to create project in")
    
    args = parser.parse_args()
    
    generator = ProjectStructureGenerator(args.project_name, args.path)
    
    try:
        generator.create_structure()
        generator.create_files()
        
        logger.info(f"✅ Project '{args.project_name}' created successfully!")
        logger.info(f"📁 Location: {generator.project_path.absolute()}")
        logger.info("📖 Next steps:")
        logger.info("   1. cd into the project directory")
        logger.info("   2. Install dependencies: poetry install")
        logger.info("   3. Activate environment: poetry shell")
        logger.info("   4. Copy .env.example to .env and configure")
        logger.info("   5. Start coding or run: poetry run jupyter lab")
        
    except Exception as e:
        logger.error(f"❌ Failed to create project: {e}")
        raise

if __name__ == "__main__":
    main()