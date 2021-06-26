# simple-backups
A simple backup library, to backup stuff across a few projects, and throw it in google storage

Supports a few different types of inputs which can be backed up:
- File backup: backs up a file from local filesystem
- Directory backup: zips up a directory from local filesystem
- sqlite database: backs up a sqlite database

Backs up to local backup/ directory.
Can also specify output which will copy the backup there, such as google storage bucket.

## Creating google cloud bucket
- create new project, called "Backup", enable billing
- create bucket, coldline storage, or archival
- enable versioning in the bucket

https://googleapis.dev/python/storage/latest/index.html

## TODO
- Remote SQL database
- Flag to run all backups immediately, maybe one for not running scheduling