use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;
use anyhow::Result;
use rayon::prelude::*;

use ucsvgen::*;

fn main() -> Result<()> {
    let table1: Table = Table::new(
        "A".to_string(),
        vec![
            Column::new(
                ColumnTypes::STRING,
                10,
                0,
            )?,
            Column::new(
                ColumnTypes::DATE,
                0,
                0,
            )?,
            Column::new(
                ColumnTypes::LONG,
                10,
                0,
            )?,
            Column::new(
                ColumnTypes::DOUBLE,
                10,
                2,
            )?,
            Column::new(
                ColumnTypes::STRING,
                10,
                0,
            )?,
            Column::new(
                ColumnTypes::DATE,
                0,
                0,
            )?,
            Column::new(
                ColumnTypes::LONG,
                10,
                0,
            )?,
            Column::new(
                ColumnTypes::DOUBLE,
                10,
                2,
            )?,
            Column::new(
                ColumnTypes::STRING,
                10,
                0,
            )?,
            Column::new(
                ColumnTypes::DATE,
                0,
                0,
            )?,
            Column::new(
                ColumnTypes::LONG,
                10,
                0,
            )?,
            Column::new(
                ColumnTypes::DOUBLE,
                10,
                2,
            )?,
            Column::new(
                ColumnTypes::STRING,
                10,
                0,
            )?,
            Column::new(
                ColumnTypes::DATE,
                0,
                0,
            )?,
            Column::new(
                ColumnTypes::LONG,
                10,
                0,
            )?,
            Column::new(
                ColumnTypes::DOUBLE,
                10,
                2,
            )?,
            Column::new(
                ColumnTypes::STRING,
                10,
                0,
            )?,
            Column::new(
                ColumnTypes::DATE,
                0,
                0,
            )?,
            Column::new(
                ColumnTypes::LONG,
                10,
                0,
            )?,
            Column::new(
                ColumnTypes::DOUBLE,
                10,
                2,
            )?,
            Column::new(
                ColumnTypes::STRING,
                10,
                0,
            )?,
            Column::new(
                ColumnTypes::DATE,
                0,
                0,
            )?,
            Column::new(
                ColumnTypes::LONG,
                10,
                0,
            )?,
            Column::new(
                ColumnTypes::DOUBLE,
                10,
                2,
            )?,
        ],
        "|".to_string(),
        0.4,
    );


    let table2: Table = Table::new(
        "B".to_string(),
        vec![
            Column::new(
                ColumnTypes::STRING,
                10,
                0,
            )?,
            Column::new(
                ColumnTypes::DATE,
                0,
                0,
            )?,
            Column::new(
                ColumnTypes::LONG,
                10,
                0,
            )?,
        ],
        "|".to_string(),
        0.3,
    );

    let table3: Table = Table::new(
        "C".to_string(),
        vec![
            Column::new(
                ColumnTypes::STRING,
                10,
                0,
            )?,
            Column::new(
                ColumnTypes::DATE,
                0,
                0,
            )?,
            Column::new(
                ColumnTypes::LONG,
                10,
                0,
            )?,
            Column::new(
                ColumnTypes::DOUBLE,
                10,
                2,
            )?,
            Column::new(
                ColumnTypes::STRING,
                10,
                0,
            )?,
        ],
        "|".to_string(),
        0.3,
    );

    let schema = vec![table1, table2, table3];

    let export_path: &Path = Path::new("export");

    let export_sizes_gb = vec![1_u64, 5, 10];
    // let export_sizes_gb = vec![1_u64];

    let export_file_counts = vec![1_u64, 100, 1000, 4000];
    // let export_file_counts = vec![100_u64];

    let now = Instant::now();

    for export_size_gb in export_sizes_gb {
        let export_size_bytes = export_size_gb * 1024_u64.pow(3);
        for export_file_count in export_file_counts.iter() {
            let dir_path = PathBuf::new()
            .join(export_path)
            .join(format!("{}_{}GB", export_file_count, export_size_gb));

            fs::create_dir_all(dir_path.as_path())?;

            let export_file_size_bytes = export_size_bytes / export_file_count;
            let export_file = ExportFile::new(
                (&schema).to_vec(),
                export_file_size_bytes.clone() as usize,
            )?;
            (0..export_file_count.to_owned()).into_par_iter()
                .try_for_each(|x| -> Result<()> {
                    let file_path = dir_path
                        .join(format!(
                            "file_{}_{}_{}.txt",
                            &export_file_size_bytes,
                            &export_file_count,
                            &x
                        ));
                    export_file.generate_export_to_file(
                        file_path.as_path()
                    )?;
                    Ok(())
                })?;
        }
    }

    let elapsed = now.elapsed();

    println!("Elapsed {:?}", elapsed);

    Ok(())
}
