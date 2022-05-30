# txprocesser

A toy tx engine written in Rust

## Run
```bash
$ cargo run -- <input_file_name>.csv > <output_file_name>.csv
```

## Test
```bash
$ cargo clippy --all
$ cargo run -- <input_file_name>.csv > <output_file_name>.csv
```

## Design

The txprocesser is built in order to be as simple as possible.

- A single executable file
- No internal modules
- "component testing" is being done against a memory database, easier and faster
- Simple functions instead of complex structs

The application is divided into 5 sections (module-like)

- CSV - all CSV related types and functions
- Database/SQL - all database and sql related types and functios
- CLI - all cli related types and functions
- Domain - the general types playing the most central role in the application
- Main - executable entrypoint
