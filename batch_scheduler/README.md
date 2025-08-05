# Installation guide

## Prerequisite

- [RockyOS](https://docs.rockylinux.org/guides/8_6_installation/)
- [Python 3.12](https://www.python.org/downloads/)
- [Miniconda](https://docs.anaconda.com/miniconda/miniconda-install/)
- [Git](https://git-scm.com/download/linux)

================================================================================

## Database commands:

### Init SqlAlchemy structure for database migration
- If the source code don't have the SQLAlchemy "migrations" folder, run this command to generate it
- Optional: Might need to add "python" to the command when running in console terminal

```sh
(python) manage.py db init
```

### When updating models (Add/update fields, constrains,...etc)
- Run the "migrate" command to create a new version of the database (Which will be store inside "migrations/version" folder)
- Run the "upgrade" command to upgrade the database the new version (Which contain the changes)
- Optional: Might need to add "python" to the command when running in console terminal

```sh
(python) manage.py db migrate
```
```sh
(python) manage.py db upgrade
```

### Additional configuration notes

- If the source code have migrations folder but missing the versions folder
  - Delete the migrations folder then recreate it using the "init" command

- If the changes not migrating due to SqlAlchemy showing not detecting any changes
  - This is due to mismatch between migrations/version and the current database.
  - Delete the cached version files in migrations/version folder and recreate new file using the "migrate" and "upgrade" commands.

================================================================================

## System running commands:

### Commands to start/stop the system

```sh
./start_server.sh

./stop_server.sh
```

### Real-time logs tracking
- Route should be the one configured in .env file
- Below is example of the command

```sh
tail -f var/logs/batch_scheduler.log
```

### Additional configuration notes

- Expose port can be changed inside start.sh file (Default 5500)
- When changing port, please also make sure to change it in stop.sh file to properly terminate the running process

================================================================================

# Code structure guide

## Routes folder

- Contain the api routes of the system
- If adding another route folder then make sure to also register it in "app.py" file

## Logic folder

- Contain the logic of object models (CRUD, model specific logics)

## Models folder

- Contain the models of object inside the project

## Utils folder

- Contain common functions, the helpers of external system

## Test folder

- Contain unit tests

================================================================================

# Important files

## app.py

- Control which route folders are registered

## config.py

- Contain the environment config of the system

## logger.py

- Contain the logging logic with multiple logging handlers.
- Console handler should be turned on when in local environment only for development purpose.

## manage.py

- Main file for running the system

================================================================================

# Test

## Commands

- Open a terminal inside test folder
- Running tests with the follow command:

```sh
coverage run -m pytest -vv
```

- Check coverage report with:

```sh
coverage report -m
```
