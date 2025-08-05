# /data/projects/data_comparison

## Project Overview

Brief description of the project, its objectives, and key outcomes.

## Project Structure

```
/data/projects/data_comparison/
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
   cd /data/projects/data_comparison
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

## Usage

**Running scripts:**
```bash
poetry run python scripts/run_pipeline.py
```

**Adding new dependencies:**
```bash
poetry add pandas numpy  # Production dependencies
poetry add pytest black --group dev  # Development dependencies
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

## License

Specify the project license.

## Contact

Project maintainer contact information.
