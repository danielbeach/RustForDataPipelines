use reqwest;
use std::fs::File;
use std::io::prelude::*;
use std::time::Duration;
use std::fs;
use std::io;
use std::path::PathBuf;
use postgres;
use csv::Reader;
use postgres::{Client, Error, NoTls};


fn main() {
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(120))
        .build().expect("failed to build client");
    let quarters = vec!["Q1", "Q2", "Q3"];
    for q in &quarters {
        download_remote_file(&client, q);
    }
    unpack_zip_files(&quarters)
    process_local_files(&quarters)


}

fn download_remote_file(c: &reqwest::blocking::Client, quarter: &str) {
    let fname = format!("{}.zip", quarter);
    println!(" working on file data_{}_2022.zip", quarter);
    let remote_file =  format!("https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data/data_{}_2022.zip", quarter);
    let content = c.get(remote_file).send().expect("failed to get uri").bytes().expect("failed to get bytes");
    let mut f = File::create(&fname).expect("failed to create fileß");
    f.write_all(&content).expect("failed to write to file");
}

fn unpack_zip_files(quarters: &Vec<&str>) {
    for q in quarters {
        println!("working on {:?}", q);
        let fname = format!("{}.zip", q);
        let zip_file = fs::File::open(&fname).unwrap();
        let mut csv_archive = zip::ZipArchive::new(zip_file).unwrap();
        for i in 0..csv_archive.len() {
            let mut file = csv_archive.by_index(i).expect("error getting file from zip archive");
            if !(*file.name()).starts_with("__MACOSX") && (*file.name()).ends_with(".csv") {
                let dir = format!("data_{}_2022/", q);
                let fname = file.name();
                if !std::path::Path::new(&q).exists() {
                        std::fs::create_dir(q).expect("cannot create directory");
                    }
                let outfile = fname.replace(" ", "");
                let outfile = &outfile.replace(&dir, " ");
                let outfile = &outfile.trim();
                let file_path = PathBuf::from(format!("/Users/danielbeach/code/rust_data_pipeline/data_pipe/{}", q)).join(&outfile);
                println!("{:?}", file_path);
                let mut outfile = fs::File::create(&file_path).unwrap();
                io::copy(&mut file, &mut outfile).unwrap();
            }
        }
    }
}

fn process_local_files(quarters: &Vec<&str>){
    let client = Client::connect("postgresql://postgres:postgres@0.0.0.0:5432/postgres",
        NoTls,
    ).expect("failed to create Postgres client");
    ensure_table_exists(client);

    for q in quarters {
        let paths = fs::read_dir(q).unwrap();
        for path in paths {
            let file = path.unwrap().path();
            println!("Working on: {}", file.display());
            read_csv_file(file)
        }
    }

}

fn read_csv_file(file: PathBuf) {
    let mut client = Client::connect("postgresql://postgres:postgres@0.0.0.0:5432/postgres",
        NoTls,
    ).expect("failed to create Postgres client");
    let mut reader = Reader::from_path(file).expect("cannot open file");
    for result in reader.records() {
        let record = result.expect("cound not read record");
        insert_record_to_postgres(&mut client, record);
    }
}


fn ensure_table_exists(mut client: Client){
    client.batch_execute(
        "
        DROP TABLE IF EXISTS hard_drives;
        CREATE TABLE IF NOT EXISTS hard_drives (
            dt VARCHAR,
            serial_number VARCHAR,
            model VARCHAR,
            capacity VARCHAR,
            failure VARCHAR
        );
    ",
).expect("could not execute create statement");
}

fn insert_record_to_postgres(client: &mut postgres::Client, record: csv::StringRecord){
    let column_1 = &record[0];
    let column_2 = &record[1];
    let column_3 = &record[2];
    let column_4 = &record[3];
    let column_5 = &record[4];
    client.execute(
        "INSERT INTO hard_drives (dt, serial_number, model, capacity, failure) VALUES ($1, $2, $3, $4, $5)",
        &[&column_1, &column_2, &column_3, &column_4, &column_5],
    ).expect("failed to insert record");
}
