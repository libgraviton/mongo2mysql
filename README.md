# mongo2mysql

A small PHP command line script that reads structures from MongoDB collections and loads them into MySQL/MariaDB via CSV and
`LOAD DATA INFILE`.

It is designed that you execute the script for each collection you want to transport.

## Installation

You need PHP 7.* and `composer` installed.

```
git clone <url>
composer install
```

## Example

Let's say you want to transport the local MongoDB collection `Example` in the database `application` to the MySQL server
located at `192.168.1.2` in a existing database `etl` (MySQL username is `joe` with password `mypw`).

The command would be:

```
php bin/app 'mongodb://localhost:27017' application Example 'mysql:host=192.168.1.2;dbname=etl' joe mypw
```

For all options and usage information, see

```
php bin/app --help
```

## Naming convention and limitations

As we all know, MongoDB is document orientated and schemaless (with _unlimited_ depths) - something MySQL/MariaDB cannot really represent.

Let's assume this MongoDB structure:

```json
{
  "_id": "record",
  "arrayField": [
    {
      "anotherProp": "value"
    },
    {
      "anotherProp": "value2"
    }
  ],
  "anotherObject": {
    "intProp": 3,
    "boolProp": true
  },
  "someProp": "text"
}
```

Will lead to this DDL:

```sql
CREATE TABLE `Hans` (
	`_id` VARCHAR(255) NOT NULL DEFAULT '',
	`arrayField_0_anotherProp` TEXT NULL,
	`arrayField_1_anotherProp` TEXT NULL,
	`anotherObject_intProp` INT(11) NULL DEFAULT NULL,
	`anotherObject_boolProp` TINYINT(1) NULL DEFAULT NULL,
	`someProp` TEXT NULL,
	PRIMARY KEY (`_id`)
)
COLLATE='utf8_general_ci'
ENGINE=InnoDB
;
```

The following principles and limitations apply:

* Object hierarchies are flattend out with `_` between object levels
* Array structures are numbered up: `[fieldname].[index].[propertyName]`
* There is no _recursion limit_ per se (level depth). But any fieldname **longer than 64 characters is dropped** from the CSV output and thus from import to not hit [MySQL limits in fieldname length](https://mariadb.com/kb/en/library/identifier-names/).
* Every `string` field (except `_id`) will be imported as `TEXT` field to not [hit the maximum row size](https://mariadb.com/kb/en/library/identifier-names/) 
* If there is a field `_id` in the set, a primary key index will be created for that field. 

## Datetimes

MongoDB Date objects are inserted as `DATETIME` colums. As `DATETIME` has no timezone information and Mongo deals all in UTC,
we must specify a representation timezone for the MySQL import.

You can specify your desired import timezone with the `--tz=` argument (see `--help`). Example would be `--tz=Europe/Berlin`.
