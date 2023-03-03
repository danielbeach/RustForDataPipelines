# RustForDataPipelines
Testing out if Rust can be used for a normal Data Engineering Pipeline.

Check out the full blog post here. 
https://www.confessionsofadataguy.com/using-rust-to-write-a-data-pipeline-thoughts-musings/

The idea was to try things like HTTP, File Systems, and Database calls (Postgres) out with Rust.

These are normal data engineering functions, is it too verbose to write data pipelines in Rust?
The data flow is as follows
- download `.zip` file.
- unpack the `.zip` file into `.csv`, mess with files.
- connect Rust to `postgres`
- Process CSV file into `postgres`
- Run some `sql` and push some `rows`
