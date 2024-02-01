# data-and-analytics-etl
Lambda code and testing for the Data &amp; Analytics team's ETL project.

## Setting things up locally
To install requirements for this repo, run:
`pip install -r requirements.txt`

## Running tests against the repo
Tests in this repo have been implemented with unittest.

To run all tests, in the main directory run:
`py -m unittest`

To run tests from a specific file, run:
`py -m unittest testing/{file_path}.py`

To run tests from a specific class within a file, run:
`py -m unittest testing.{file_path}.{class_name}`

To run a specific test, run:
`py -m unittest testing.{file_path}.{class_name}.{test_name}`

## Creating new tests
All new tests should be created in a uniform way to make understanding them easier.  First, all test files should be created in the Testing folder or a subfolder of it and have a file name in the format of `tests_{name}.py`.  Next, tests need to be implemented as functions in a class inheriting from `unittest.TestCase`. Finally, tests should make sure to implement the proper mocks to run properly, and use functions from `testing/util.py` as needed.  A common structure that many current tests take the form of is the following:

```
import unittest
from testing.util import run_test_cases
...

{implement mocks here}

class TestClass(unittest.TestCase):
    def test_method(self):
        test_data = [
            {
                'name': 'test_case_name',
                'expect_exception': {False/True},
                'exception': {None/Exception('Reason')},
                ...
            },
            ...
        ]

        def test_function(self, test_case):
            {test_body}

        run_test_cases(self, test_data, test_function)

    ...
```
