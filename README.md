# nem12-etl
An ELT Pipeline written in Python and SQL, using Databricks to parse nem12 meter data 

## Documentation

### NEM12 Specification
This ETL pipeline implements parsing and processing of NEM12 format meter data as defined by AEMO (Australian Energy Market Operator).

- **NEM12/NEM13 Specification**: [MDFF Specification NEM12/NEM13 v2.6](https://www.aemo.com.au/-/media/files/electricity/nem/retail_and_metering/market_settlement_and_transfer_solutions/2024/mdff-specification-nem12-nem13-v26-clean-final.pdf?rev=c0145cdfe1114a6dad0abad6586c3cf9&sc_lang=en)

The specification details the following record types handled by this pipeline:
- **100** - Header Record (File identifier, version, date/time)
- **200** - NMI Details Record (NMI, time of use indicator, interval length)
- **300** - Meter Event Records (Meter Serial Number, Test Result)
- **400** - Interval Data Record (Interval date, consumption values, quality flags)
- **500** - B-Series Data Record (Billing data)
- **900** - End of File Record

## Notebooks

### nem12_meter_data_processor.py
Main Databricks notebook containing:
- **NEM12Parser**: Parses NEM12 format files and extracts structured data by record type
- **NEM12DataProcessor**: Transforms and processes meter data for analysis
- Data quality checks and validation
- Delta table creation for persistent storage
- SQL analysis examples and data export capabilities

## Getting Started

1. Update the `file_path` variable with your actual NEM12 file location
2. Configure Databricks mount points for data access
3. Run the notebook to process NEM12 files
4. Access processed data from Delta tables for further analysis
