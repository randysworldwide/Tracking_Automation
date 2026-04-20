import sys
print("Python version:", sys.version)
print("Testing imports...")
try:
    import pyodbc
    print("pyodbc OK:", pyodbc.version)
except Exception as e:
    print("pyodbc FAILED:", e)
try:
    import requests
    print("requests OK:", requests.__version__)
except Exception as e:
    print("requests FAILED:", e)
try:
    import paramiko
    print("paramiko OK:", paramiko.__version__)
except Exception as e:
    print("paramiko FAILED:", e)
try:
    import pandas as pd
    print("pandas OK:", pd.__version__)
except Exception as e:
    print("pandas FAILED:", e)
print("Done.")
