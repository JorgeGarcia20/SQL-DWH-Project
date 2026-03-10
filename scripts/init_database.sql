/*
===========================================================
Create Schemas
===========================================================

Script Purpose:
The script sets up three schemas: bronze, silver and gold within the database. These schemas are used to organize data based on its processing stage, with bronze for raw data, silver for cleaned and transformed data, and gold for aggregated and refined data ready for analysis.

Requirements:
- The database must be exists before running this script.
- The user executing the script must have the necessary permissions to create schemas in the database.
- The script should be run in an environment that supports SQL execution, such as a database management tool or command-line interface.
*/

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;