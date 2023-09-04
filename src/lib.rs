use std::fs::File;
use std::io::Write;
use std::path::Path;

use anyhow::Result;
use rand;
use rand::distributions::Alphanumeric;
use rand::Rng;
use rayon::prelude::*;
use strum_macros;
use thiserror::Error;

#[derive(Debug, strum_macros::Display, Eq, PartialEq, Clone)]
pub enum ColumnTypes {
    STRING,
    LONG,
    DOUBLE,
    DATE,
}


#[derive(Error, Debug)]
pub enum ColumnError {
    #[error("Exponent can only be set for double type. The type is {column_type}.")]
    IncorrectExponent {
        column_type: ColumnTypes
    },
    #[error("For double type exponent can not be zero.")]
    ExponentIsZero,
}


#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Column {
    column_type: ColumnTypes,
    size: u8,
    exponent: u8,
}

impl Column {
    pub fn new(
        column_type: ColumnTypes,
        size: u8,
        exponent: u8,
    ) -> Result<Column, ColumnError> {
        if column_type != ColumnTypes::DOUBLE && exponent != 0 {
            return Err(ColumnError::IncorrectExponent {
                column_type
            });
        }

        if column_type == ColumnTypes::DOUBLE && exponent == 0 {
            return Err(ColumnError::ExponentIsZero);
        }

        Ok(Column { column_type, size, exponent })
    }
}


#[derive(Debug, Clone)]
pub struct Table {
    id_value: String,
    columns: Vec<Column>,
    delimiter: String,
    percent_size: f64,
    row_size_bytes: usize,
}

impl Table {
    pub fn new(
        id_value: String,
        columns: Vec<Column>,
        delimiter: String,
        percent_size: f64,
    ) -> Table {
        let row_size_bytes: usize = columns
            .iter()
            .map(
                |x| match x.column_type {
                    ColumnTypes::STRING => x.size as usize, // For string add 2 quotes to size
                    ColumnTypes::DOUBLE => x.size as usize, // For double add 1 decimal to size
                    ColumnTypes::LONG => x.size as usize,
                    ColumnTypes::DATE => 10
                }
            )
            .sum::<usize>()
            + (columns.len() - 1) * delimiter.len() // Add the size of delimiters
            + id_value.len(); // Add the size of the ID
        Table { id_value, columns, delimiter, percent_size, row_size_bytes }
    }


    pub fn generate_table_row(&self) -> Result<String, NumberGeneratorError> {
        let mut buffer: Vec<String> = vec![self.id_value.clone()];
        buffer.append(
            &mut self.columns.iter()
                .map(|x| match x.column_type {
                    ColumnTypes::STRING => Ok(rand_string_of_len(x.size)),
                    ColumnTypes::DOUBLE => rand_number_of_len(x.size, x.exponent),
                    ColumnTypes::LONG => rand_number_of_len(x.size, x.exponent),
                    ColumnTypes::DATE => Ok(rand_date())
                })
                .collect::<Result<Vec<String>, NumberGeneratorError>>()?
        );

        Ok(buffer.join(&self.delimiter) + "\n")
    }


    pub fn generate_table(&self, file_size_bytes: usize) -> Result<String, NumberGeneratorError> {
        let table_size_bytes = (file_size_bytes as f64 * self.percent_size).ceil() as usize;
        let row_count = table_size_bytes / self.row_size_bytes;

        (0..row_count)
            .into_par_iter()
            .map(|_| self.generate_table_row())
            .try_reduce(|| "".to_string(), |x, y| Ok(x + &y))
    }
}


#[derive(Error, Debug)]
pub enum ExportFileError {
    #[error("Sum of table percentage sizes must be equal 1. It was {sum_percent_size}.")]
    SumPercentSizeIncorrect { sum_percent_size: f64 }
}


pub struct ExportFile {
    tables: Vec<Table>,
    file_size_bytes: usize,
}

impl ExportFile {
    pub fn new(
        tables: Vec<Table>,
        file_size_bytes: usize,
    ) -> Result<ExportFile, ExportFileError> {
        let sum_percent_size: f64 = tables.iter()
            .map(|x| x.percent_size)
            .sum();

        if sum_percent_size != 1.0 {
            return Err(ExportFileError::SumPercentSizeIncorrect { sum_percent_size });
        }

        Ok(ExportFile { tables, file_size_bytes })
    }


    pub fn generate_export_file(&self) -> Result<String, NumberGeneratorError> {
        self.tables.par_iter()
            .map(|x| x.generate_table(self.file_size_bytes))
            .try_reduce(|| "".to_string(), |x, y| Ok(x + &y))
    }


    pub fn generate_export_to_file(&self, path: &Path) -> Result<()> {
        let exported = self.generate_export_file()?;
        let mut file = File::create(path)?;
        file.write_all(exported.as_ref())?;
        Ok(())
    }
}


#[derive(Error, Debug)]
pub enum NumberGeneratorError {
    #[error("Size can only be in interval [1, 20>. It was {size}.")]
    SizeTooLarge { size: u8 },
    #[error("Exponent can not be larger than size. Size was {size}, and exponent was {exponent}")]
    ExponentTooLarge { size: u8, exponent: u8 },
}


fn rand_number_of_len(
    size: u8,
    exponent: u8,
) -> Result<String, NumberGeneratorError> {
    if size >= 20 || size < 1 {
        return Err(NumberGeneratorError::SizeTooLarge { size });
    }

    if exponent > size {
        return Err(NumberGeneratorError::ExponentTooLarge { size, exponent });
    }

    let digit_count = size as u32;
    let mut rng = rand::thread_rng();
    let mut number = rng.gen_range(
        (10_u64.pow(digit_count - 1))..(10_u64.pow(digit_count))
    ).to_string();

    if exponent > 0 {
        number.insert(number.len() - exponent as usize, '.');
    }

    Ok(number)
}


fn rand_date() -> String {
    let mut rng = rand::thread_rng();
    let year = rng.gen_range(1900_u16..=9999_u16);
    let month = rng.gen_range(1_u8..=12_u8);
    let day = rng.gen_range(1u8..=28_u8);

    format!("{}-{}-{}", year, month, day)
}


fn rand_string_of_len(size: u8) -> String {
    format!("\"{}\"",
            rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(size as usize)
                .map(char::from)
                .collect::<String>()
    )
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_tests() -> Result<()> {
        let column = Column::new(
            ColumnTypes::STRING,
            10,
            0,
        )?;
        assert_eq!(column, Column {
            column_type: ColumnTypes::STRING,
            size: 10,
            exponent: 0,
        });

        Ok(())
    }
}
