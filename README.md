# AMBRA_Backups
Code using the AMBRA SDK that is used to backup studies on AMBRA.

# Installation
From the directory, run:
```
pip install -e .
pip install -r requirements.txt
```

# Testing
To run tests, run:
```
pytest test
```

# Notes
May not be compatible with python 3.13. `mysql.connector.connect` returns None upon initialization. Works with python <=3.12
