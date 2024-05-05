# Real-Time Data Streaming using Apache Nifi, AWS, Snowpipe, Stream & Task

## Project Architecure:

<img src='attachments\project_architecture.png'>

## Agenda

- Create a Change Data Capture (CDC) pipeline to consume data from an S3 bucket and store it in SCD1 and SCD2 tables.

## Tools Used:

1. **Python & Faker Library**: Used for generating fake data.
2. **Apache NiFi**: Used to fetch data from the local environment to the S3 bucket.
3. **Snowflake**:
   - **Snowpipe**: Automatically fetches data from the S3 bucket.
   - **Stream & Task**: Implements the CDC pipeline for the SCD2 table.
4. **S3 Bucket**: Utilized for storing raw data.
5. **Docker**: Used for deployment.

## Implementation:

- [SCD1 Implementation](Docs/04.%20SCD%201%20Implementation.md)
- [SCD2 Implementation](Docs/05.%20SCD%202%20Implementation.md)