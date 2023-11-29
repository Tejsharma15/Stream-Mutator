# Stream Mutator

## Goal

The project aims to understand and implement mutation testing through open-source tools and write effective test cases for a Python-based data stream processor.

## About the Code

- **GitHub Repository:** [Stream Mutator](https://github.com/Tejsharma15/Stream-Mutator)

Our real-time stream processing engine consists of two parallel threads:

1. **Data Generator:** Generates data every second with the specified attributes at the given input.
2. **Consumer:** Reads the generated data on the fly and contains several inbuilt algebraic aggregate functions such as sum, min/max, average, standard deviation, variance, palindrome finding, etc.
3. **Driver:** Runs the generator and consumer parallelly in threads. It collects results from both producer and consumer and plots the throughput graph.

## Mutation Testing

- **Unit Level Testing:** We have used the Python module - Mutpy for unit-level mutations. Mutpy supports the standard unittest module for loading test cases and applies mutation on the AST level.

- **Integration Level Testing:** We have written our own tool for integration mutation testing in Python. It does basic parsing of the given modules and extracts methods and parameters with their types. Then it performs the following mutations:
  - Parameter value replacement
  - Function call replacement
  - Unary operator insertion in arguments
  For each mutation applied, it runs the test cases on both the original and mutated program and generates the mutation score at the end.

## Steps to Run

1. Clone the repository:

    ```bash
    git clone https://github.com/Tejsharma15/Stream-Mutator
    ```

2. Create and activate a virtual environment:

    ```bash
    python3 -m venv env
    source env/bin/activate
    ```

3. Install requirements:

    ```bash
    pip install -r requirements.txt
    ```

4. Navigate to the mutationTesting directory:

    ```bash
    cd mutationTesting
    ```

5. To run unit tests:

    ```bash
    mut.py -target ../source/consumer.py -unit-test matpyTests.py -m
    ```
    
6. To run integration tests:

    ```bash
    python3 integration.py ../source/consumer.py
    ```
