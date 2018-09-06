<?php
namespace Graviton\Mongo2Mysql;

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
class MariaImporter {

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
     * @var Filesystem
     */
    private $fs;

    private $insertStack = [];
    private $insertBulkSize = 200;

    public function __construct(Logger $logger, $dsn, $mysqlUser, $mysqlPassword)
    {
        $this->logger = $logger;
        $this->dsn = $dsn;
        $this->mysqlUser = $mysqlUser;
        $this->mysqlPassword = $mysqlPassword;
        $this->fs = new Filesystem();
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
        $connection = Connection::fromPDO($this->pdo);
        $database = new Database($connection);

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
            if (in_array('_id', $dumpResult->getFields())) {
                $creater->string('_id');
                $creater->primary('_id');
            } elseif (in_array('id', $dumpResult->getFields())) {
                $creater->string('id');
                $creater->primary('id');
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

            $row = array_map(function ($val) {
                if ($val == 'NULL') {
                    return null;
                }
                return $val;
            }, $row);

            $this->insertRecord($dumpResult, $row);


            /*
            try {
                $database->insert($row)->into($dumpResult->getEntityName());
                $insertCounter++;
            } catch (\Exception $e) {
                $this->logger->warn('Insert Error', [$e]);
            }

            if (($insertCounter % 1000) === 0) {
                $this->logger->info('SQL row insert report', ['currentCount' => $insertCounter]);
            }
            */
        }

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

        $sql = 'INSERT INTO '.$this->pdo->quote($dumpResult->getEntityName()).' ';
        $sql .= '(';

        // build query - must be same all db's
        $fieldList = $dumpResult->getFields();
        $sql .= implode(', ', $fieldList);

        $sql .= ') VALUES ';

        $els = array_map(function($record) {
            return '('.impl
        }, $this->insertStack);


        $this->insertStack = [];
    }
}
