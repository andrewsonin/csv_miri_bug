use {
    csv::{Reader, StringRecord, StringRecordsIter, Trim},
    derive_more::Display,
    std::{fs::File, io::Read, path::Path},
};

/// CSV-reader struct.
///
/// # Examples
///
/// ```rust
/// use {
///     csv::StringRecord,
///     eyre::eyre,
///     std::io::BufReader,
///     csv_miri_bug::{CsvReader, CsvRowParser},
/// };
///
/// #[derive(Debug, PartialEq)]
/// struct FromRow {
///     x: f64,
///     y: i64,
/// }
///
/// struct HeaderIndexer {
///     x_idx: usize,
///     y_idx: usize,
/// }
///
/// #[derive(Debug)]
/// pub enum HeaderIndexerError {
///     NoX,
///     DuplicatedX,
///     NoY,
///     DuplicatedY,
/// }
///
/// impl HeaderIndexer
/// {
///     fn new(columns: &StringRecord) -> Result<HeaderIndexer, HeaderIndexerError>
///     {
///         let columns: Vec<_> = columns.into_iter().collect();
///         let x_idx = columns.iter().position(|col| col == &"X");
///         if x_idx != columns.iter().rposition(|col| col == &"X") {
///             return Err(HeaderIndexerError::DuplicatedX);
///         }
///         let x_idx = x_idx.ok_or(HeaderIndexerError::NoX)?;
///         let y_idx = columns.iter().position(|col| col == &"Y");
///         if y_idx != columns.iter().rposition(|col| col == &"Y") {
///             return Err(HeaderIndexerError::DuplicatedY);
///         }
///         let y_idx = y_idx.ok_or(HeaderIndexerError::NoY)?;
///         let result = HeaderIndexer {
///             x_idx,
///             y_idx,
///         };
///         Ok(result)
///     }
/// }
///
/// struct Parser;
///
/// impl CsvRowParser for Parser
/// {
///     type HeaderIndexer = HeaderIndexer;
///     type R = FromRow;
///     type E = eyre::Report;
///
///     fn parse_row(
///         &mut self,
///         header_indexer: &Self::HeaderIndexer,
///         row: StringRecord) -> Result<Self::R, Self::E>
///     {
///         let HeaderIndexer {
///             x_idx,
///             y_idx
///         } = *header_indexer;
///         let x = row.get(x_idx)
///             .ok_or_else(
///                 || eyre!("Parser::parse_row :: no x at position {:?}", row.position().unwrap())
///             )?;
///         let y = row.get(y_idx)
///             .ok_or_else(
///                 || eyre!("Parser::parse_row :: no y at position {:?}", row.position().unwrap())
///             )?;
///         let result = FromRow {
///             x: x.parse()?,
///             y: y.parse()?,
///         };
///         Ok(result)
///     }
/// }
///
/// const CSV_FILE: &str =
///     r#"X,Z, Y
///        10.2,23.3,11
///        1,0,12.9
///        ,,1
///        0,8,9
///        1,,2"#;
///
/// let reader = BufReader::new(CSV_FILE.as_bytes());
/// let mut reader = CsvReader::new_from_reader(
///     HeaderIndexer::new,
///     reader,
///     ',',
/// )
///     .unwrap();
/// let mut reader = reader.with_parser(Parser);
///
/// assert_eq!(reader.next().unwrap().unwrap(), FromRow { x: 10.2, y: 11 });
/// assert!(reader.next().unwrap().is_err());
/// assert!(reader.next().unwrap().is_err());
/// assert_eq!(reader.next().unwrap().unwrap(), FromRow { x: 0.0, y: 9 });
/// assert_eq!(reader.next().unwrap().unwrap(), FromRow { x: 1.0, y: 2 });
/// assert!(reader.next().is_none())
/// ```
pub struct CsvReader<R: Read, H>
{
    reader: Reader<R>,
    header_indexer: H,
}

#[derive(Display, Debug)]
/// CSV-reader creation error.
pub enum CsvReaderCreationError<E>
{
    CsvError(csv::Error),
    HeaderIndexerBuilderError(E),
}

impl<H> CsvReader<File, H>
{
    /// Creates a new instance of [`CsvReader`] from `path`.
    pub fn new_from_path<E>(
        header_indexer_builder: impl FnOnce(&StringRecord) -> Result<H, E>,
        path: impl AsRef<Path>,
        delimiter: char) -> Result<Self, CsvReaderCreationError<E>>
    {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .delimiter(delimiter as u8)
            .comment(b'#'.into())
            .trim(Trim::All)
            .from_path(path)
            .map_err(CsvReaderCreationError::CsvError)?;

        let headers = reader.headers()
            .map_err(CsvReaderCreationError::CsvError)?;

        let header_indexer = header_indexer_builder(headers)
            .map_err(CsvReaderCreationError::HeaderIndexerBuilderError)?;

        let result = Self {
            reader,
            header_indexer,
        };
        Ok(result)
    }
}

impl<R: Read, H> CsvReader<R, H>
{
    /// Creates a new instance of [`CsvReader`] from `reader`.
    pub fn new_from_reader<E>(
        header_indexer_builder: impl FnOnce(&StringRecord) -> Result<H, E>,
        reader: R,
        delimiter: char) -> Result<Self, CsvReaderCreationError<E>>
    {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .delimiter(delimiter as u8)
            .trim(Trim::All)
            .from_reader(reader);

        let headers = reader.headers()
            .map_err(CsvReaderCreationError::CsvError)?;

        let header_indexer = header_indexer_builder(headers)
            .map_err(CsvReaderCreationError::HeaderIndexerBuilderError)?;

        let result = Self {
            reader,
            header_indexer,
        };
        Ok(result)
    }

    /// Creates a new instance of [`CsvRowReader`].
    pub fn with_parser<P>(&mut self, row_parser: P) -> CsvRowReader<R, P>
        where
            P: CsvRowParser<HeaderIndexer=H>
    {
        let Self { reader, header_indexer } = self;
        CsvRowReader {
            row_reader: reader.records(),
            header_indexer,
            row_parser,
        }
    }
}

/// CSV-row reader.
pub struct CsvRowReader<'a, R: Read, P: CsvRowParser>
{
    row_reader: StringRecordsIter<'a, R>,
    header_indexer: &'a P::HeaderIndexer,
    row_parser: P,
}

#[derive(Display, Debug)]
/// CSV-row reader error.
pub enum CsvRowReaderError<E>
{
    CsvRecordError(csv::Error),
    RowParserError(E),
}

impl<'a, R: Read, P: CsvRowParser> Iterator for CsvRowReader<'a, R, P>
{
    type Item = Result<P::R, CsvRowReaderError<P::E>>;

    fn next(&mut self) -> Option<Self::Item>
    {
        let Self {
            row_reader,
            header_indexer,
            row_parser
        } = self;
        let row = match row_reader.next()? {
            Ok(row) => row,
            Err(err) => return Some(Err(CsvRowReaderError::CsvRecordError(err))),
        };
        let result = row_parser.parse_row(header_indexer, row)
            .map_err(CsvRowReaderError::RowParserError);
        Some(result)
    }
}

/// Trait that encapsulates CSV-row parsing logic.
///
/// # Examples
///
/// ```rust
/// use {
///     csv::StringRecord,
///     eyre::eyre,
///     std::io::BufReader,
///     csv_miri_bug::{CsvReader, CsvRowParser},
/// };
///
/// #[derive(Debug, PartialEq)]
/// struct FromRow {
///     x: f64,
///     y: i64,
/// }
///
/// struct HeaderIndexer {
///     x_idx: usize,
///     y_idx: usize,
/// }
///
/// #[derive(Debug)]
/// pub enum HeaderIndexerError {
///     NoX,
///     DuplicatedX,
///     NoY,
///     DuplicatedY,
/// }
///
/// impl HeaderIndexer
/// {
///     fn new(columns: &StringRecord) -> Result<HeaderIndexer, HeaderIndexerError>
///     {
///         let columns: Vec<_> = columns.into_iter().collect();
///         let x_idx = columns.iter().position(|col| col == &"X");
///         if x_idx != columns.iter().rposition(|col| col == &"X") {
///             return Err(HeaderIndexerError::DuplicatedX);
///         }
///         let x_idx = x_idx.ok_or(HeaderIndexerError::NoX)?;
///         let y_idx = columns.iter().position(|col| col == &"Y");
///         if y_idx != columns.iter().rposition(|col| col == &"Y") {
///             return Err(HeaderIndexerError::DuplicatedY);
///         }
///         let y_idx = y_idx.ok_or(HeaderIndexerError::NoY)?;
///         let result = HeaderIndexer {
///             x_idx,
///             y_idx,
///         };
///         Ok(result)
///     }
/// }
///
/// struct Parser;
///
/// impl CsvRowParser for Parser
/// {
///     type HeaderIndexer = HeaderIndexer;
///     type R = FromRow;
///     type E = eyre::Report;
///
///     fn parse_row(
///         &mut self,
///         header_indexer: &Self::HeaderIndexer,
///         row: StringRecord) -> Result<Self::R, Self::E>
///     {
///         let HeaderIndexer {
///             x_idx,
///             y_idx
///         } = *header_indexer;
///         let x = row.get(x_idx)
///             .ok_or_else(
///                 || eyre!("Parser::parse_row :: no x at position {:?}", row.position().unwrap())
///             )?;
///         let y = row.get(y_idx)
///             .ok_or_else(
///                 || eyre!("Parser::parse_row :: no y at position {:?}", row.position().unwrap())
///             )?;
///         let result = FromRow {
///             x: x.parse()?,
///             y: y.parse()?,
///         };
///         Ok(result)
///     }
/// }
///
/// const CSV_FILE: &str =
///     r#"X,Z, Y
///        10.2,23.3,11
///        1,0,12.9
///        ,,1
///        0,8,9
///        1,,2"#;
///
/// let reader = BufReader::new(CSV_FILE.as_bytes());
/// let mut reader = CsvReader::new_from_reader(
///     HeaderIndexer::new,
///     reader,
///     ',',
/// )
///     .unwrap();
/// let mut reader = reader.with_parser(Parser);
///
/// assert_eq!(reader.next().unwrap().unwrap(), FromRow { x: 10.2, y: 11 });
/// assert!(reader.next().unwrap().is_err());
/// assert!(reader.next().unwrap().is_err());
/// assert_eq!(reader.next().unwrap().unwrap(), FromRow { x: 0.0, y: 9 });
/// assert_eq!(reader.next().unwrap().unwrap(), FromRow { x: 1.0, y: 2 });
/// assert!(reader.next().is_none())
/// ```
pub trait CsvRowParser
{
    /// Header indexer type.
    type HeaderIndexer;
    /// Row-parsing resulting type.
    type R;
    /// Row-parsing error type.
    type E;

    /// Parses single row.
    fn parse_row(
        &mut self,
        header_indexer: &Self::HeaderIndexer,
        row: StringRecord) -> Result<Self::R, Self::E>;
}

#[cfg(test)]
mod tests
{
    use {
        csv::StringRecord,
        eyre::eyre,
        std::io::BufReader,
        super::{CsvReader, CsvRowParser},
    };

    #[derive(Debug, PartialEq)]
    struct FromRow {
        x: f64,
        y: i64,
    }

    struct HeaderIndexer {
        x_idx: usize,
        y_idx: usize,
    }

    #[derive(Debug)]
    pub enum HeaderIndexerError {
        NoX,
        DuplicatedX,
        NoY,
        DuplicatedY,
    }

    impl HeaderIndexer
    {
        fn new(columns: &StringRecord) -> Result<HeaderIndexer, HeaderIndexerError>
        {
            let columns: Vec<_> = columns.into_iter().collect();
            let x_idx = columns.iter().position(|col| col == &"X");
            if x_idx != columns.iter().rposition(|col| col == &"X") {
                return Err(HeaderIndexerError::DuplicatedX);
            }
            let x_idx = x_idx.ok_or(HeaderIndexerError::NoX)?;
            let y_idx = columns.iter().position(|col| col == &"Y");
            if y_idx != columns.iter().rposition(|col| col == &"Y") {
                return Err(HeaderIndexerError::DuplicatedY);
            }
            let y_idx = y_idx.ok_or(HeaderIndexerError::NoY)?;
            let result = HeaderIndexer {
                x_idx,
                y_idx,
            };
            Ok(result)
        }
    }

    struct Parser;

    impl CsvRowParser for Parser
    {
        type HeaderIndexer = HeaderIndexer;
        type R = FromRow;
        type E = eyre::Report;

        fn parse_row(
            &mut self,
            header_indexer: &Self::HeaderIndexer,
            row: StringRecord) -> Result<Self::R, Self::E>
        {
            let HeaderIndexer {
                x_idx,
                y_idx
            } = *header_indexer;
            let x = row.get(x_idx)
                .ok_or_else(
                    || eyre!("Parser::parse_row :: no x at position {:?}", row.position().unwrap())
                )?;
            let y = row.get(y_idx)
                .ok_or_else(
                    || eyre!("Parser::parse_row :: no y at position {:?}", row.position().unwrap())
                )?;
            let result = FromRow {
                x: x.parse()?,
                y: y.parse()?,
            };
            Ok(result)
        }
    }

    const CSV_FILE: &str =
        r#"X,Z, Y
           10.2,23.3,11
           1,0,12.9
           ,,1
           0,8,9
           1,,2"#;

    #[test]
    pub fn csv_reader()
    {
        let reader = BufReader::new(CSV_FILE.as_bytes());
        let mut reader = CsvReader::new_from_reader(
            HeaderIndexer::new,
            reader,
            ',',
        )
            .unwrap();
        let mut reader = reader.with_parser(Parser);
        assert_eq!(reader.next().unwrap().unwrap(), FromRow { x: 10.2, y: 11 });
        assert!(reader.next().unwrap().is_err());
        assert!(reader.next().unwrap().is_err());
        assert_eq!(reader.next().unwrap().unwrap(), FromRow { x: 0.0, y: 9 });
        assert_eq!(reader.next().unwrap().unwrap(), FromRow { x: 1.0, y: 2 });
        assert!(reader.next().is_none())
    }
}