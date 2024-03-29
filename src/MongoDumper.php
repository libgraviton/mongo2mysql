<?php
namespace Graviton\Mongo2Mysql;

use Graviton\Mongo2Mysql\Model\DumpResult;
use MongoDB\BSON\UTCDateTime;
use MongoDB\Client;
use MongoDB\Collection;
use MongoDB\Driver\ReadPreference;
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
	 * filter to select mongo data
	 *
	 * @var array
	 */
    private $selectFilter = [];

    /**
     * a projection to use
     *
     * @var array
     */
    private $projection = [];

	/**
	 * @var array filter ops map
	 */
    private $selectFilterOps = [
    	'==' => 'eq',
		'!=' => 'ne',
		'>=' => 'gte',
		'<=' => 'lte',
		'>' => 'gt',
		'<' => 'lt'
	];

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
     * array of paths to an alternative field value expression
     *
     * @var array
     */
    private $fieldPaths = [];

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
     * array of nullable flags
     *
     * @var array
     */
    private $fieldNullables = [];

    /**
     * array of primary flags
     *
     * @var array
     */
    private $fieldPrimary = [];

    /**
     * @var string
     */
    private $timezone = 'UTC';

    /**
     * @var string
     */
    private $pipelineFile;

    /**
     * @var string
     */
    private $outputFile;

    /**
     * @var bool
     */
    private $writeFieldNames = false;

    /**
     * @var string
     */
    private $nullValue = 'NULL';

    /**
     * @var bool
     */
    private $skipTooLongFields = true;

    /**
     * MongoDumper constructor.
     *
     * @param Logger $logger         logger
     * @param string $dsn            Mongo DSN
     * @param string $databaseName   database name
     * @param string $collectionName collection name
     * @param string $tempDir        temp dir
	 * @param array  $selectFilter   filter to select for
     */
    public function __construct(Logger $logger, $dsn, $databaseName, $collectionName, $tempDir, array $selectFilter)
    {
        $this->logger = $logger;
        $this->client = new Client($dsn);
        $this->databaseName = $databaseName;
        $this->collectionName = $collectionName;
        $this->tempDir = $tempDir;
        $this->collection = $this->client->{$databaseName}->{$collectionName};
        $this->selectFilter = $selectFilter;
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
     * @param string $pipelineFile
     */
    public function setPipelineFile($pipelineFile)
    {
        $this->pipelineFile = $pipelineFile;
    }

    /**
     * set OutputFile
     *
     * @param string $outputFile outputFile
     *
     * @return void
     */
    public function setOutputFile($outputFile) {
        $this->outputFile = $outputFile;
    }

    /**
     * set WriteFieldNames
     *
     * @param bool $writeFieldNames writeFieldNames
     *
     * @return void
     */
    public function setWriteFieldNames($writeFieldNames) {
        $this->writeFieldNames = $writeFieldNames;
    }

    /**
     * set NullValue
     *
     * @param string $nullValue nullValue
     *
     * @return void
     */
    public function setNullValue($nullValue) {
        $this->nullValue = $nullValue;
    }

    /**
     * set SkipTooLongFields
     *
     * @param bool $skipTooLongFields skipTooLongFields
     *
     * @return void
     */
    public function setSkipTooLongFields($skipTooLongFields) {
        $this->skipTooLongFields = $skipTooLongFields;
    }

    /**
     * dumps mongo stuff into a file
     *
     * @return DumpResult result
     */
    public function dump() : ?DumpResult
    {
        $dumpResult = new DumpResult();
        $dumpResult->setEntityName($this->collectionName);

        // is there a fieldSpec collection?
        $fieldSpecCollectionName = $this->collectionName . 'FieldSpec';
        if ($this->client->{$this->databaseName}->selectCollection($fieldSpecCollectionName)->count() > 0) {
            $this->determineSchemaFieldsByFieldSpec($fieldSpecCollectionName);
            $dumpResult->setHasFieldSpec(true);
        } else {
            $this->determineSchemaFieldsBySampleSize();
        }

        $this->logger->info('Collected field count', ['count' => count($this->fields)]);

        // check for long fieldnames and drop them
        if ($this->skipTooLongFields) {
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
        }

        $dumpResult->setFields($this->fields);
        $dumpResult->setFieldTypes($this->fieldTypes);
        $dumpResult->setFieldLengths($this->fieldLengths);
        $dumpResult->setFieldNullables($this->fieldNullables);
        $dumpResult->setFieldsPrimary($this->fieldPrimary);

        if (is_null($this->outputFile)) {
            $tempFile = tempnam($this->tempDir, 'grvmd');
        } else {
            $tempFile = $this->outputFile;
        }

        // write into file

        $this->logger->info('Starting writing to CSV File', ['filename' => $tempFile]);

        $fp = fopen($tempFile, 'w+');

        // write fieldnames?
        if ($this->writeFieldNames) {
            fputcsv($fp, $this->fields);
        }

        $i = 0;
        foreach ($this->getMongoIterator($this->getSelectFilter()) as $record) {
            $flatRecord = $this->makeFlat($record);
            $thisRecord = [];

            foreach ($this->fields as $fieldName) {
                // alternative field value path?
                $fieldValueSelector = $fieldName;
                if (isset($this->fieldPaths[$fieldName])) {
                    $fieldValueSelector = $this->fieldPaths[$fieldName];
                }

                if (isset($flatRecord[$fieldValueSelector]) && !is_null($flatRecord[$fieldValueSelector])) {
                    $thisRecord[] = $this->convertValue($flatRecord[$fieldValueSelector]);
                } else {
                    $thisRecord[] = $this->nullValue;
                }
            }

            fputcsv($fp, $thisRecord);
            $i++;

            if (($i % 10000) === 0) {
                $this->logger->info(
                	'CSV Writing Status report',
					[
						'currentCount' => $i
					]
				);
            }
        }

        fclose($fp);

        $this->logger->info('Finished writing to CSV File', ['filename' => $tempFile, 'count' => $i]);

        $dumpResult->setDumpFile($tempFile);
        $dumpResult->setRowCount($i);

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
	 * returns a filter to select data
	 *
	 * @return array filter
	 */
    private function getSelectFilter()
	{
		$filter = [];
		foreach ($this->selectFilter as $expression) {
			$op = null;
			$expressionParts = [];
			foreach ($this->selectFilterOps as $sign => $name) {
				if (strpos($expression, $sign) !== false) {
					$op = $name;
					$expressionParts = explode($sign, $expression);
				}
				if (!is_null($op)) {
					break;
				}
			}

			if (is_null($op) || count($expressionParts) != 2) {
				throw new \LogicException('Wrong select query "'.$expression.'" - check --help of the app.');
			}

			// parse date..
			if (preg_match('/^[0-9]{4}-[0-9]{2}-[0-9]{2}$/', $expressionParts[1])) {
				$dt = \DateTime::createFromFormat(
					'Y-m-d H:i:s',
					$expressionParts[1].' 00:00:00',
					new \DateTimeZone($this->timezone)
				);
				$expressionParts[1] = new UTCDateTime($dt);
			}

			$filter[$expressionParts[0]] = ['$'.$op => $expressionParts[1]];
		}

		if (!empty($filter)) {
			$this->logger->info('Using source data filter', ['filter' => $filter]);
		}

		return $filter;
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
                    $subNamePrefix = $namePrefix.'_'.$prop;
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

                if ($value === true) {
                    $value = 1;
                }
                if ($value === false) {
                    $value = 0;
                }

                $flatRecord[$propertyName] = $value;
            }
        }

        $flatRecord = array_filter($flatRecord, function($val) {
            return !is_null($val);
        });

        return $flatRecord;
    }

    /**
     * returns either a find() or aggreation pipeline iterator
     *
     * @param array $selectFilter
     * @param array $limit
     * @return \MongoDB\Driver\Cursor|\Traversable
     */
    private function getMongoIterator(array $selectFilter = [], ?int $limit = null) {
        $readPreference = new ReadPreference(ReadPreference::RP_SECONDARY_PREFERRED);

        if (is_null($this->pipelineFile)) {
            $options = [];
            if (is_numeric($limit)) {
                $options['limit'] = $limit;
            }

            if (!empty($this->projection)) {
                $options['projection'] = $this->projection;
            }

            $options['readPreference'] = $readPreference;

            return $this->collection->find($selectFilter, $options);
        }

        $pipeline = json_decode(
            file_get_contents($this->pipelineFile),
        true
        );

        $pipeline = $this->replaceInPipeline($pipeline);

        // add limit
        if (is_numeric($limit)) {
            $pipeline[] = ['$limit' => $limit];
        }

        $this->logger->info(
            'Using a pipeline to select the data to export',
            [
                'file' => $this->pipelineFile,
                'pipeline' => $pipeline
            ]
        );

        return $this->collection->aggregate($pipeline, ['readPreference' => $readPreference]);
    }

    /**
     * a duplication from mother project in order to clean up dates..
     *
     * @param array $pipeline
     */
    private function replaceInPipeline(array $pipeline) {
        foreach ($pipeline as $key => $prop) {
            if (is_array($prop)) {
                $pipeline[$key] = $this->replaceInPipeline($prop);
            }
            if (is_string($prop) && $prop == '#newDate#') {
                $pipeline[$key] = new UTCDateTime();
            }
        }
        return $pipeline;
    }

    private function determineSchemaFieldsBySampleSize()
    {
        $this->logger->info(
            'Starting first pass for schema and field types',
            ['sampleSize' => $this->schemaSampleSize, 'collection' => $this->collectionName]
        );

        /**
         * first pass: schema and types
         */
        foreach ($this->getMongoIterator([], $this->schemaSampleSize) as $record) {
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

                // workaround for *Id named fields -> make them strings
                if (substr($name, -2) == 'Id') {
                    $this->fieldTypes[$name] = DumpResult::FIELDTYPE_STRING;
                }
                // workaround for fields named 'number'
                if ($name == 'number') {
                    $this->fieldTypes[$name] = DumpResult::FIELDTYPE_STRING;
                }

                if (isset($this->fieldTypes[$name]) && $this->fieldTypes[$name] == DumpResult::FIELDTYPE_STRING) {
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
    }

    private function determineSchemaFieldsByFieldSpec($collectionName)
    {
        $this->logger->info(
            'Fields are specified in FieldSpec collection, will not do any guessing...',
            ['collection' => $collectionName]
        );

        $this->fields = [];
        $this->projection = [];

        foreach ($this->client->{$this->databaseName}->selectCollection($collectionName)->find() as $field) {
            $this->fields[] = $field['field'];

            $thisFieldLength = 0;
            if (isset($field['length'])) {
                $this->fieldLengths[$field['field']] = $field['length'];
                $thisFieldLength = $field['length'];
            }

            $type = DumpResult::FIELDTYPE_STRING;
            switch ($field['type']) {
                case "int":
                    $type = DumpResult::FIELDTYPE_INT;
                    break;
                case "tinyint":
                    $type = DumpResult::FIELDTYPE_SMALLINT;
                    if ($thisFieldLength < 2) {
                        $type = DumpResult::FIELDTYPE_BOOL;
                    }
                    break;
                case "datetime":
                    $type = DumpResult::FIELDTYPE_DATETIME;
                    break;
            }
            $this->fieldTypes[$field['field']] = $type;

            // nullable?
            $this->fieldNullables[$field['field']] = true;
            if (isset($field['nullable']) && $field['nullable'] == false) {
                $this->fieldNullables[$field['field']] = false;
            }

            if (isset($field['key']) && $field['key'] == 'PRI') {
                $this->fieldPrimary[$field['field']] = true;
            }

            // add projection and alternative field path
            if (isset($field['path'])) {
                $this->projection[$field['path']] = 1;
                $this->fieldPaths[$field['field']] = str_replace('.', '_', $field['path']);
            }
        }
    }
}
