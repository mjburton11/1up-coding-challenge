# 1up-coding-challenge
FHIR resource count - 1up coding challenge

This script reads in FHIR data in the folder `data`.  It will count every occurance
of a resource that is associated with a patient specified by the user. The user must specify
either the patient name or the patient id.  

```python
python resource_counter_cli.py --firstname "Cleo27" --lastname "Bode78"
```
or 
```python
python resource_counter_cli.py --id "de9b4be6-5aa4-4d8f-85b6-d4fa9888e550"
```

This package can also be imported and used like

```python
from oneup_coding_challenge import resource_counter_cli
resource_counter_cli("Cleo27", "Bode89", None)
resource_counter_cli(None, None, "de9b4be6-5aa4-4d8f-85b6-d4fa9888e550")
```

This script requires `json`, `pandas`, and `tabulate`. 

Previous commit (`f419b66e8419a852bb888c631fca168a44e6e589`) was first attempted brute force
approach and took 6.384 seconds. 

Latest version has a much cleaner method and takes 2.215 seconds.  