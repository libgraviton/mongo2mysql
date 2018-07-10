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
                \PDO::MYSQL_ATTR_LOCAL_INFILE => true,
                \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION
            ]
        );

        try {
            $this->logger->info('Creating MySQL table', ['name' => $dumpResult->getEntityName()]);
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

        // drop if exists
        $dropStatement = 'DROP TABLE IF EXISTS `'.$dumpResult->getEntityName().'`';
        $this->logger->info('Dropping table if exists', ['tableName' => $dumpResult->getEntityName()]);
        $this->pdo->query($dropStatement);

        $database = new Database($connection);

        $database->schema()->create($dumpResult->getEntityName(), function (CreateTable $creater) use ($dumpResult) {
            $fieldTypes = $dumpResult->getFieldTypes();
            $totalLength = 0;

            foreach ($dumpResult->getFields() as $fieldName) {
                if (isset($fieldTypes[$fieldName])) {
                    $type = $fieldTypes[$fieldName];
                } else {
                    $type = DumpResult::FIELDTYPE_STRING;
                }

                switch ($type) {
                    case DumpResult::FIELDTYPE_STRING:
                        $creater->text($fieldName);
                        break;
                    case DumpResult::FIELDTYPE_BOOL:
                        $creater->boolean($fieldName);
                        $totalLength += 4;
                        break;
                    case DumpResult::FIELDTYPE_INT:
                        $creater->integer($fieldName);
                        $totalLength += 20;
                        break;
                    case DumpResult::FIELDTYPE_DATETIME:
                        $creater->dateTime($fieldName);
                        $totalLength += 12;
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
        $query = 'LOAD DATA LOCAL INFILE ';
        $query .= $this->pdo->quote($dumpResult->getDumpFile(), \PDO::PARAM_STMT);
        $query .= ' INTO TABLE `'.$dumpResult->getEntityName().'` ';
        $query .= ' CHARACTER SET utf8 ';
        $query .= ' FIELDS TERMINATED BY \',\' ';
        $query .= ' ENCLOSED BY \'"\' ';
        $query .= ' LINES TERMINATED BY \'\n\' ';

        $this->logger->info('Starting import query', ['query' => $query]);

        $this->pdo->query($query);
    }
}
