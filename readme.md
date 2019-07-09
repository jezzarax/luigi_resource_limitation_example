Just


```
pipenv --three install
pipenv shell
PYTHONPATH=. luigi --local-scheduler --workers=20 --module resource_check UeberTask
```


Every task will sleep for 5 seconds. Each task consumes one piece of resource and there's 4 items of the resource defined in `luigi.cfg`.

In the output you'll see when each of the tasks `accessed` the resource. There should not be more than 4 tasks generating the data at the same time despite 20 processes working on tasks simultaneously.


