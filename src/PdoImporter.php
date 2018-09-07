<?php
namespace Graviton\Mongo2Mysql;

use Graviton\Mongo2Mysql\Db\CompilerGetter;
use Graviton\Mongo2Mysql\Model\DumpResult;
use Monolog\Logger;
use Opis\Database\Connection;
use Opis\Database\Database;
use Opis\Database\Schema\CreateTable;
use Symfony\Component\Filesystem\Filesystem;

/**
 * @author   List of contributors <https://github.com/libgraviton/mongo2mysql/graphs/contributors>
 * @license  https://opensource.org/licenses/MIT MIT License
 * @link     http://swisscom.ch
 */
class PdoImporter {

    /**
     * @var Logger
     */
    private $logger;

    /**
     * PDO DSN
     *
     * @var string
     */
    private $dsn;

    private $mysqlUser;

    private $mysqlPassword;

    /**
     * @var \PDO
     */
    private $pdo;

	/**
	 * @var Connection
	 */
    private $connection;

    private $compiler;

    /**
     * @var Filesystem
     */
    private $fs;

    private $insertStack = [];
    private $insertBulkSize;

    public function __construct(Logger $logger, $dsn, $mysqlUser, $mysqlPassword, $insertBulkSize)
    {
        $this->logger = $logger;
        $this->dsn = $dsn;
        $this->mysqlUser = $mysqlUser;
        $this->mysqlPassword = $mysqlPassword;
        $this->fs = new Filesystem();
        $this->insertBulkSize = (int) $insertBulkSize;
    }

    /**
     * dumps mongo stuff into a file
     *
     * @return DumpResult result
     */
    public function import(DumpResult $dumpResult)
    {
        $this->pdo = new \PDO(
            $this->dsn,
            $this->mysqlUser,
            $this->mysqlPassword,
            [
                \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION
            ]
        );

        $this->connection = Connection::fromPDO($this->pdo);
        $this->compiler = CompilerGetter::getInstance($this->connection);

        try {
            $this->logger->info('Creating target PDO table', ['name' => $dumpResult->getEntityName()]);
            $this->createTableSchema($dumpResult);
            $this->importData($dumpResult);
        } catch (\Exception $e) {
            $this->logger->crit('Error in creating the target schema or importing data', ['exception' => $e]);
        }

        $this->logger->info('Removing CSV File', ['filename' => $dumpResult->getDumpFile()]);
        $this->fs->remove($dumpResult->getDumpFile());
    }

    private function createTableSchema(DumpResult $dumpResult)
    {
        $database = new Database($this->connection);

        // drop if exists
        $this->logger->info('Dropping target table', ['tableName' => $dumpResult->getEntityName()]);

        try {
            $database->schema()->drop($dumpResult->getEntityName());
        } catch (\Exception $e) {
            $this->logger->warn(
                'Could not drop table, maybe it does not exist',
                ['tableName' => $dumpResult->getEntityName()]
            );
        }

        $database->schema()->create($dumpResult->getEntityName(), function (CreateTable $creater) use ($dumpResult) {
            $fieldTypes = $dumpResult->getFieldTypes();

            foreach ($dumpResult->getFields() as $fieldName) {
                if (isset($fieldTypes[$fieldName])) {
                    $type = $fieldTypes[$fieldName];
                } else {
                    $type = DumpResult::FIELDTYPE_STRING;
                }

                // workaround for *Id named fields -> make them strings
                if (substr($fieldName, -2) == 'Id') {
                    $type = DumpResult::FIELDTYPE_STRING;
                }

                switch ($type) {
                    case DumpResult::FIELDTYPE_STRING:
                        $creater->text($fieldName);
                        break;
                    case DumpResult::FIELDTYPE_BOOL:
                        $creater->boolean($fieldName);
                        break;
                    case DumpResult::FIELDTYPE_INT:
                        $creater->integer($fieldName);
                        break;
                    case DumpResult::FIELDTYPE_DATETIME:
                        $creater->dateTime($fieldName);
                        break;
                }
            }

            // has index?
			$primarySet = false;
            if (in_array('_id', $dumpResult->getFields())) {
                $creater->string('_id');
                $creater->primary('_id');
                $primarySet = true;
            }
            if (in_array('id', $dumpResult->getFields())) {
                $creater->string('id');
                if (!$primarySet) {
					$creater->primary('id');
				}
            }

            $creater->engine('InnoDB');
        });

        $this->logger->info('Created table as derived from schema', ['tableName' => $dumpResult->getEntityName()]);
    }

    private function importData(DumpResult $dumpResult)
    {
        $fp = fopen($dumpResult->getDumpFile(), 'r+');
        $fieldNames = $dumpResult->getFields();
        $insertCounter = 0;

        $this->logger->info('Starting to execute INSERT queries...');

        while (($data = fgetcsv($fp)) !== false) {
            $row = array_combine(
                $fieldNames,
                $data
            );

            $this->insertRecord($dumpResult, $row);
        }

        // flush again
		$this->flushBulk($dumpResult);

        fclose($fp);

        $this->logger->info('Finished PDO import', ['totalCount' => $insertCounter]);
    }

    private function insertRecord(DumpResult $dumpResult, $record)
    {
        $this->insertStack[] = $record;

        if (count($this->insertStack) >= $this->insertBulkSize) {
            $this->flushBulk($dumpResult);
        }
    }

    private function flushBulk(DumpResult $dumpResult)
    {
        if (empty($this->insertStack)) {
            return;
        }

		$this->logger->info('Flushing bulk stack', ['rowCount' => count($this->insertStack)]);

        $sql = 'INSERT INTO '.$this->compiler->wrap($dumpResult->getEntityName());
        $fields = $this->quoteArray($dumpResult->getFields(), false);

        // field list
        $sql .= ' ('.implode(',', $fields).')';

        // values
		$rows = [];
		foreach ($this->insertStack as $row) {
			$rows[] = implode(',', $this->quoteArray($row));
		}

		$sql .= ' VALUES ('.implode('),(', $rows).')';

		$this->pdo->query($sql);

		// empty stack
        $this->insertStack = [];
    }

    private function quoteArray($array, $isData = true) {
    	return array_map(function($value) use ($isData) {
    		if ($value == 'NULL') {
    			return 'null';
			}
			if ($isData) {
				return $this->compiler->quote($value);
			}
    		return $this->compiler->wrap($value);
		}, $array);
	}
}