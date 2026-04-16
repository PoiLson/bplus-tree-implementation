# B+ Tree Implementation

## About

This repository contains coursework developed for the *Database Systems Implementation* course at the Department of Informatics and Telecommunications, National and Kapodistrian University of Athens (NKUA).

The project focuses on implementing a B+ tree index structure on top of a block-level memory manager.

## **Authors:**  

- **Lytra Maria** — [GitHub](https://github.com/marialitra)
- **Mylonaki Danai** — [GitHub](https://github.com/PoiLson)

## Main Idea

The goal of this project is to understand how database systems use indexing structures to improve data retrieval performance.

Specifically, the project implements a B+ tree on top of the provided block file (BF) layer, which acts as a buffer manager between disk and main memory.

The B+ tree is used to store records based on a primary key (`id`) and supports efficient insertion and lookup operations.

The implementation includes:

- creation and initialization of B+ tree files
- insertion of records with node splitting
- search for records using tree traversal from root to leaf
- management of metadata (root, height, etc.)
- handling of duplicate keys (rejected insertions)

The project demonstrates how indexing significantly improves access performance compared to sequential file organization.

## Functionality

The following core functions are implemented:

- `BP_CreateFile` – creates and initializes a B+ tree file
- `BP_OpenFile` – opens a B+ tree file and loads metadata
- `BP_CloseFile` – closes the file and releases resources
- `BP_InsertEntry` – inserts a record into the B+ tree
- `BP_GetEntry` – retrieves a record based on its key

## Design Choices

- Node splitting follows a **right-heavy policy**, where the middle key is promoted during splits.
- Both **data nodes** and **index nodes** are handled explicitly.
- The B+ tree maintains a structure suitable for efficient disk-based access.

## Usage

The project includes a [`Makefile`](Makefile) for compilation and execution.

### Compile the project

```make bp```

### Run the program

```make run```

The execution runs the provided example ```main``` function, which:

- inserts a set of randomly generated records
- performs search operations on selected keys
- demonstrates the correctness of the implementation

## Execution & Testing Details

The ```make run``` target was modified to include:

```rm -f data.db```

ensuring that the database file is reset before each execution.

- The provided ```main()``` structure was preserved, and a sample lookup for key **340** was added based on the generated dataset.
- Test parameters were adjusted in [`src/record.c`](src/record.c):
	- maximum record id: **1000**
	- total records: **800**
- A custom testing function (```our_main()```) was implemented to further evaluate the system. This function demonstrates:
	- large-scale insertions (up to 2000 records)
	- rejection of duplicate keys
	- both successful and unsuccessful search operations
	- optional visualization of the B+ tree structure
 
	*Note*: To use this function, modify the [`examples/bp_main.c`](examples/bp_main.c) file accordingly.

- The tree printing functionality is used for debugging purposes and can be disabled by commenting the corresponding line in [`examples/bp_main.c`](examples/bp_main.c)

## Technologies

- C
- Block-based storage management
- B+ tree indexing
- File I/O

## Design Notes

The split strategy for both data nodes and index nodes follows a **right-heavy policy**.

More specifically:

- for **data nodes**, the promoted key is chosen after taking the newly inserted key into account, using zero-based indexing
- for **index nodes**, the middle key is promoted, and when the number of keys is odd, the promoted key corresponds to `floor(n / 2)`

This choice was made in order to maintain a consistent split policy throughout the implementation.

## Testing Notes

During testing, the implementation was evaluated with large input sizes (on the order of `10^6` operations), and no issues were observed.

Additional custom tests were also used to verify:

- correct handling of duplicate-key rejection
- successful and unsuccessful search cases
- proper tree growth and node splitting behavior
