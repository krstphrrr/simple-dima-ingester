# DIMA Data Processing CLI App

This CLI app provides tools for extracting and processing data from Microsoft Access files (`.mdb` or `.accdb`) and loading them into a PostgreSQL database. The app is divided into two main components:

- **_1_dima_extract**: Extracts specific tables from Access files as CSV files using Docker.
- **_2_dima_loadingest**: Loads, cleans, and ingests the CSV files into a PostgreSQL database using Polars.

# Structure
```bash
/_2_dima_loadingest/
│
|
├── config.py           # Configuration file for database and application settings
├── main.py             # Entry point for the script
├── /logs/              # Directory where log files are stored
│
├── /scripts/           # Directory containing core script modules
│   ├── data_loader.py  # Functions for loading CSV files into DataFrames
│   ├── data_cleaner.py # Functions for cleaning and transforming data
│   ├── db_connector.py # Functions for database operations (table creation, data insertion)
│   └── __init__.py     # Package initializer
│
└── /tests/             # Directory for test scripts (not yet implemented)

```



## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
  - [_1_dima_extract](#1_dima_extract)
  - [_2_dima_loadingest](#2_dima_loadingest)
- [Configuration](#configuration)
- [License](#license)

## Installation

### Prerequisites

- [Docker](https://www.docker.com/get-started) installed and running on your machine.
- Python 3.7 or higher (created on 3.12).
- A running PostgreSQL instance.

### Install Dependencies

Clone the repository and install the required Python packages:

```bash
git clone https://github.com/yourusername/dima-cli.git
cd dima-cli
pip install -r requirements.txt
```
## Usage
