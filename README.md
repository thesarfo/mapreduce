# MapReduce

A simple implementation of the MapReduce programming model for processing large text files.

This project implements a **word count** algorithm that:
- Reads text from an input file
- Counts the frequency of each word (case-insensitive)
- Outputs the results in a tab-separated format

## Execution Modes

### 1. Sequential (Milestone 1)
Single-threaded execution that processes data linearly.

### 2. Parallel (Milestone 2)
Multi-core execution that distributes work across CPU cores:
- Splits input into chunks
- Runs map tasks in parallel
- Uses hash partitioning to distribute keys across reducers
- Runs reduce tasks in parallel
- Automatically utilizes all available CPU cores

## Usage

```bash
dotnet run
```

Select execution mode when prompted:
- Option 1: Sequential execution
- Option 2: Parallel execution (multi-core)