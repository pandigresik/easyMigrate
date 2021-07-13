# easyMigrate

## What is easyMigrate?

easyMigrate is small tool to convert dbf database file to MySql using python version 3.8.6

## Installation & setup

- clone this repository `git clone https://github.com/pandigresik/easyMigrate.git`
- `cd easyMigrate`
- `pip install -r requirement.txt` to install dependency this application
- for ubuntu OS you may need to install development package to connect mysql `sudo apt-get install python3-dev default-libmysqlclient-dev build-essential`

## Generate schema and migrate

- create database in mysql
- example connection string `mysql://user:password@host/database`
- `python main.py --conn=connection_string --dir=your_dbf_directory --output-schema` to generate schema database
- execute schema database script to your database in mysql
- `python main.py --conn=connection_string --dir=your_dbf_directory` to copy all dbf to your table in database mysql

## Video demo installation

[![Install easyMigrate](http://img.youtube.com/vi/J0N-_T69XRA/0.jpg)](http://www.youtube.com/watch?v=J0N-_T69XRA "Install easyMigrate")

## Copyright

easyMigrate dikembangkan dan dimaintain oleh [asligresik](https://github.com/pandigresik)

## Lisensi

Lisensi dari easyMigrate adalah [MIT License](LICENSE) namun proyek yang dibangun menyeseuaikan dengan kebijakan masing-masing
