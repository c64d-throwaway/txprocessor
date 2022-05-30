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

## Tests
The tests are minimal. But try to be efficient by testing from simple to complex scenarios.
There is no unit testing, although I believe there should be, due time constrains and so.
So you will notice that these are intergratin / components test, quite efficient.

## Thoughts and future improvements
### Lazy reading
The CSV is being read line for line with a Reader "object". A sensible implementation  
would be batch processing with a reasonable number of transactions on each batch.  
In this case I was piping the transactions into a "queue" in order to simulate an  
async event. I didn't want to complicate things too much, just to show I was aware  
and made reasonable decisions.

### Safety
I have used the type system as much as possible, creating custom types, enums  
canonical and consistent de/serialization of structs.

### Readability
The application is modular and tries to be testable and maintainable as possible  
most functions are striving to be pure, with a clear and consistent API.

### Future
In order to handle a large number of data flowing via concurrent connections  
it is possible to shard the transactions entirely. e.g splitting to 100 shards  
where each transaction is processed on shard number #mod(client_id,100).
