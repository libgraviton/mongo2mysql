<?php
namespace Graviton\Mongo2Mysql;

use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Schema\Schema;
use Graviton\Mongo2Mysql\Model\DumpResult;
use Graviton\Mongo2Mysql\Model\ImportResult;
use Monolog\Logger;
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
     * @var \PDO
     */
    private $pdo;

	/**
	 * @var Connection
	 */
    private $connection;

    /**
     * @var Filesystem
     */
    private $fs;

    private $insertCounter = 0;
	private $insertCounterError = 0;

    public function __construct(Logger $logger, \PDO $pdo)
    {
        $this->logger = $logger;
        $this->pdo = $pdo;
        $this->fs = new Filesystem();
    }

    /**
     * dumps mongo stuff into a file
     *
     * @return DumpResult result
     */
    public function import(DumpResult $dumpResult) : ImportResult
    {
        $this->connection = DriverManager::getConnection(['pdo' => $this->pdo]);

        $this->insertCounter = 0;
        $this->insertCounterError = 0;

        try {
            $this->logger->info('Creating target PDO table', ['name' => $dumpResult->getEntityName()]);

            $this->createTableSchema($dumpResult);
            $this->insertDataLoadDataInfile($dumpResult);

			$this->logger->info(
				'Finished PDO import',
				[
					'totalCount' => $this->insertCounter,
					'errorCount' => $this->insertCounterError
				]
			);
        } catch (\Exception $e) {
            $this->logger->critical('Error in creating the target schema or importing data', ['exception' => $e]);
            throw $e;
        } finally {
            $this->logger->info('Removing CSV File', ['filename' => $dumpResult->getDumpFile()]);
            $this->fs->remove($dumpResult->getDumpFile());
        }

        $result = new ImportResult();
        $result->setInsertCounter($this->insertCounter);
        $result->setInsertCounterError($this->insertCounterError);

        return $result;
    }

    private function createTableSchema(DumpResult $dumpResult)
    {
        $connection = DriverManager::getConnection(['pdo' => $this->pdo]);
        $schemaManager = $connection->getSchemaManager();

        // drop if exists
        if ($schemaManager->createSchema()->hasTable($dumpResult->getEntityName())) {
            $this->logger->info('Dropping target table', ['tableName' => $dumpResult->getEntityName()]);
            $schemaManager->dropTable($dumpResult->getEntityName());
        }

        $schema = new Schema();
        $table = $schema->createTable($dumpResult->getEntityName());

        $fieldLengths = $dumpResult->getFieldLengths();
        $fieldTypes = $dumpResult->getFieldTypes();
        $fieldNullable = $dumpResult->getFieldNullables();

        foreach ($dumpResult->getFields() as $fieldName) {
            if (isset($fieldTypes[$fieldName])) {
                $type = $fieldTypes[$fieldName];
            } else {
                $type = DumpResult::FIELDTYPE_STRING;
            }

            $options = [];

            if (isset($fieldLengths[$fieldName])) {
                $options['length'] = ((int) $fieldLengths[$fieldName]);;

                if (!$dumpResult->isHasFieldSpec()) {
                    // double if no field spec
                    $options['length'] = $options['length'] * 2;
                }
            }

            $options['notnull'] = true;
            if (in_array($fieldName, $fieldNullable)) {
                $options['notnull'] = false;
            }

            $table->addColumn(
                $fieldName,
                $type,
                $options
            );
        }

        if (!empty($dumpResult->getFieldsPrimary())) {
            $table->setPrimaryKey(array_keys($dumpResult->getFieldsPrimary()));
        } else {
            // set on _id/id if exists
            if (in_array('_id', $dumpResult->getFields())) {
                $table->setPrimaryKey(['_id']);
            }
            if (in_array('id', $dumpResult->getFields())) {
                $table->setPrimaryKey(['id']);
            }
        }

        // migrate to this schema
        foreach ($schema->toSql($schemaManager->getDatabasePlatform()) as $query) {
            $this->pdo->query($query);
        }

        //$table->addOption('engine', 'InnoDB');

        $this->logger->info('Created table as derived from schema', ['tableName' => $dumpResult->getEntityName()]);
    }

    private function insertDataLoadDataInfile(DumpResult $dumpResult) {
        $query = 'LOAD DATA LOCAL INFILE ';
        $query .= $this->pdo->quote($dumpResult->getDumpFile(), \PDO::PARAM_STMT);
        $query .= ' INTO TABLE `'.$dumpResult->getEntityName().'` ';
        $query .= ' CHARACTER SET utf8 ';
        $query .= ' FIELDS TERMINATED BY \',\' ';
        $query .= ' ENCLOSED BY \'"\' ';
        $query .= ' LINES TERMINATED BY \'\n\' ';

        $this->logger->info('LOAD DATA INFILE capable target recognized, using that method..');
        $this->logger->info('Starting import query', ['query' => $query]);

        $res = $this->pdo->query($query);

        if ($res == false) {
            throw new \RuntimeException('Was unable to do LOAD DATA LOCAL INFILE');
        }

        $this->insertCounter = $res->rowCount();
    }
}
