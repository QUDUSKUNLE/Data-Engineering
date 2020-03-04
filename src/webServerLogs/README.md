# Analytics Pipeline

This repo contains the code for creating a data pipeline to calculate metrics for a fake webserver:

* `generateLogs.py` -- generates fake webserver logs.
* `storeLogs.py` -- parses the logs and stores them in a SQLite database.
* `countVisitors.py` -- pulls from the database to count visitors to the site per day.

## Usage

* Execute the three scripts mentioned above, in order.

You should see output from `countVisitors.py`.
