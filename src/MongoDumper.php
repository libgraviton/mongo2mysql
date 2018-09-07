<?php
namespace Graviton\Mongo2Mysql;

use Graviton\Mongo2Mysql\Model\DumpResult;
use MongoDB\BSON\UTCDateTime;
use MongoDB\Client;
use MongoDB\Collection;
use MongoDB\Model\BSONArray;
use MongoDB\Model\BSONDocument;
use Monolog\Logger;

/**
 * @author   List of contributors <https://github.com/libgraviton/mongo2mysql/graphs/contributors>
 * @license  https://opensource.org/licenses/MIT MIT License
 * @link     http://swisscom.ch
 */
class MongoDumper {

    /**
     * @var Logger
     */
    private $logger;

    /**
     * @var Client
     */
    private $client;

    /**
     * @var Collection
     */
    private $collection;

    /**
     * name of database
     *
     * @var string
     */
    private $databaseName;

    /**
     * name of collection
     *
     * @var string
     */
    private $collectionName;

    /**
     * directory for temp files
     *
     * @var string
     */
    private $tempDir;

    /**
     * @var int
     * @see https://mariadb.com/kb/en/library/identifier-names/
     */
    private $maxMysqlFieldLength = 64;

    /**
     * how many records we select to determine schema fields
     *
     * @var int
     */
    private $schemaSampleSize = 5000;

    /**
     * array of fieldnames
     *
     * @var array
     */
    private $fields = [];

    /**
     * array of fieldtypes
     *
     * @var array
     */
    private $fieldTypes = [];

    /**
     * array of fieldlengths
     *
     * @var array
     */
    private $fieldLengths = [];

    /**
     * @var string
     */
    private $timezone = 'UTC';

    /**
     * MongoDumper constructor.
     *
     * @param Logger $logger         logger
     * @param string $dsn            Mongo DSN
     * @param string $databaseName   database name
     * @param string $collectionName collection name
     * @param string $tempDir        temp dir
     */
    public function __construct(Logger $logger, $dsn, $databaseName, $collectionName, $tempDir)
    {
        $this->logger = $logger;
        $this->client = new Client($dsn);
        $this->databaseName = $databaseName;
        $this->collectionName = $collectionName;
        $this->tempDir = $tempDir;
        $this->collection = $this->client->{$databaseName}->{$collectionName};
    }

    /**
     * set SchemaSampleSize
     *
     * @param int $schemaSampleSize schemaSampleSize
     *
     * @return void
     */
    public function setSchemaSampleSize($schemaSampleSize)
    {
        $this->schemaSampleSize = $schemaSampleSize;
    }

    /**
     * set Timezone
     *
     * @param string $timezone timezone
     *
     * @return void
     */
    public function setTimezone($timezone)
    {
        $this->timezone = $timezone;
    }

    /**
     * dumps mongo stuff into a file
     *
     * @return DumpResult result
     */
    public function dump()
    {
        $dumpResult = new DumpResult();
        $dumpResult->setEntityName($this->collectionName);

        $this->logger->info(
            'Starting first pass for schema and field types',
            ['sampleSize' => $this->schemaSampleSize, 'collection' => $this->collectionName]
        );

        /**
         * first pass: schema and types
         */
        foreach ($this->collection->find([], ['limit' => $this->schemaSampleSize]) as $record) {
            $flatRecord = $this->makeFlat($record);

            $this->fields = array_unique(
                array_merge(array_keys($flatRecord), $this->fields)
            );

            foreach ($flatRecord as $name => $value) {
                if (
                    !isset($this->fieldLengths[$name]) ||
                    (isset($this->fieldLengths[$name]) && strlen($value) > $this->fieldLengths[$name])
                ) {
                    $this->fieldLengths[$name] = strlen($value);
                }

                if (isset($this->fieldTypes[$name]) && $this->fieldTypes[$name] == 'string') {
                    continue;
                }

                if (is_bool($value)) {
                    $this->fieldTypes[$name] = DumpResult::FIELDTYPE_BOOL;
                } elseif ($value instanceof UTCDateTime) {
                    $this->fieldTypes[$name] = DumpResult::FIELDTYPE_DATETIME;
                } elseif (preg_match('/^[0-9]+$/', $value)) {
                    $this->fieldTypes[$name] = DumpResult::FIELDTYPE_INT;
                } else {
                    $this->fieldTypes[$name] = DumpResult::FIELDTYPE_STRING;
                }
            }
        }

        // correct lengths for numbers
		$maxSize = 9;
        foreach ($this->fieldLengths as $name => $length) {
        	if ($length > $maxSize && $this->fieldTypes[$name] == DumpResult::FIELDTYPE_INT) {
        		$this->fieldTypes[$name] = DumpResult::FIELDTYPE_STRING;
			}
		}

        $this->logger->info('Collected field count', ['count' => count($this->fields)]);

        // check for long fieldnames and drop them
        $this->fields = array_filter(
            $this->fields,
            function ($name) {
                if (strlen($name) > $this->maxMysqlFieldLength) {
                    $this->logger->warning('Dropped field, field name too long for mysql', ['fieldName' => $name]);
                    return false;
                }
                return true;
            }
        );

        $dumpResult->setFields($this->fields);
        $dumpResult->setFieldTypes($this->fieldTypes);
        $dumpResult->setFieldLengths($this->fieldLengths);

        // write into file
        $tempFile = tempnam($this->tempDir, 'grvmd');
        $this->logger->info('Starting writing to CSV File', ['filename' => $tempFile]);

        $fp = fopen($tempFile, 'w+');

        $i = 0;
        foreach ($this->collection->find([]) as $record) {
            $flatRecord = $this->makeFlat($record);
            $thisRecord = [];

            foreach ($this->fields as $fieldName) {
                if (isset($flatRecord[$fieldName]) && !is_null($flatRecord[$fieldName])) {
                    $thisRecord[] = $this->convertValue($flatRecord[$fieldName]);
                } else {
                    $thisRecord[] = 'NULL';
                }
            }

            fputcsv($fp, $thisRecord);
            $i++;

            if (($i % 10000) === 0) {
                $this->logger->info('CSV Writing Status report', ['currentCount' => $i]);
            }
        }

        fclose($fp);

        $this->logger->info('Finished writing to CSV File', ['filename' => $tempFile, 'count' => $i]);

        $dumpResult->setDumpFile($tempFile);

        return $dumpResult;
    }

    /**
     * converts a value so it can be represented in the csv
     *
     * @param mixed $value value
     *
     * @return string value for csv
     */
    private function convertValue($value)
    {
        if ($value instanceof UTCDateTime) {
            $datetime = $value->toDateTime();
            return $datetime->setTimezone(new \DateTimeZone($this->timezone))->format('Y-m-d H:i:s');
        } elseif (is_bool($value)) {
            if ($value === true) {
                return 1;
            }
            return 0;
        }

        return $value;
    }

    /**
     * takes an object from mongodb and flattens it out
     *
     * @param \ArrayObject $document   the mongo document
     * @param array        $flatRecord previous record for iteration
     * @param string       $namePrefix name prefix for iteration
     *
     * @return array mongo record as a flat array
     */
    private function makeFlat(\ArrayObject $document, $flatRecord = [], $namePrefix = '')
    {
        foreach ($document as $prop => $value) {
            if ($value instanceof BSONDocument) {
                if (empty($namePrefix)) {
                    $subNamePrefix = $prop;
                } else {
                    $subNamePrefix = $namePrefix;
                }

                $flatRecord = $this->makeFlat($value, $flatRecord, $subNamePrefix);
            } elseif ($value instanceof BSONArray) {
                $i = 0;
                foreach ($value as $member) {
                    if (empty($namePrefix)) {
                        $subNamePrefix = $prop.'_'.$i;
                    } else {
                        $subNamePrefix = $namePrefix.'_'.$prop.'_'.$i;
                    }

                    $flatRecord = $this->makeFlat($member, $flatRecord, $subNamePrefix);

                    $i++;
                }
            } else {
                if (empty($namePrefix)) {
                    $propertyName = $prop;
                } else {
                    $propertyName = $namePrefix.'_'.$prop;
                }

                // clean dollar sign
				$propertyName = str_replace('$', '_', $propertyName);

                $flatRecord[$propertyName] = $value;
            }
        }

        $flatRecord = array_filter($flatRecord, function($val) {
            return !is_null($val);
        });

        return $flatRecord;
    }
}
