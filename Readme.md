# Descriptor Matching Project

This project cleans and matches raw transaction **descriptors** to a list of known merchants using fuzzy matching techniques. It uses a local MySQL database to store data and provides Python scripts to set up the database, ingest data, and perform the matching process.

## Setup

1. **Create a Python virtual environment** in the project directory:
   Use `python3 -m venv venv` (or simply `python -m venv venv` on Windows) to create a virtual environment named `venv`.
2. **Activate the virtual environment**:

   * On Linux/Mac: `source venv/bin/activate`
   * On Windows: `venv\Scripts\activate`
3. **Install dependencies** from **requirements.txt**:

   ```bash
   pip install -r requirements.txt
   ```

   This will install all required Python packages (e.g. `pandas`, `SQLAlchemy`, `mysql-connector-python`, `rapidfuzz`, etc.).
4. **Configure the database credentials**:
   Create a file named `.env` in the project directory with your MySQL settings. For example:

   ```bash
   DB_HOST=localhost  
   DB_PORT=3306  
   DB_USER=your_mysql_username  
   DB_PASS=your_mysql_password  
   DB_NAME=your_database_name  
   ```

   Make sure a MySQL server is running on `localhost` and that the database specified by **DB\_NAME** exists (you may need to create an empty database in MySQL). Use credentials for a user that has access to this database.

## Running the Scripts

After completing the setup, run the following scripts (in order) to set up and execute the project:

1. **Database Setup** – initialize the MySQL database schema by running:

   ```bash
   python db_setup.py
   ```

   This will connect to the MySQL server and create the necessary tables (e.g. descriptors table, merchant list table).
2. **Data Ingestion** – load initial data into the database by running:

   ```bash
   python db_ingest.py
   ```

   This script will read input data (such as descriptors and merchant information) and insert it into the MySQL tables.
3. **Main Processing** – perform the descriptor matching logic by running:

   ```bash
   python main.py
   ```

   This will clean the raw descriptors and match them to known merchants using fuzzy matching. The script updates the database with matched merchant IDs and outputs any unmatched descriptors to an `unmatched.csv` file.
