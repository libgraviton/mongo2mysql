<?php
namespace Graviton\Mongo2Mysql;

use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Schema\Schema;
use Graviton\Mongo2Mysql\Model\DumpResult;
use Graviton\Mongo2Mysql\Util\MetaLogger;
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
	 * @var MetaLogger
	 */
    private $metaLogger;

    private $reportLoadId;

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

    /**
     * @var Filesystem
     */
    private $fs;

    private $insertCounter = 0;
	private $insertCounterError = 0;

    public function __construct(Logger $logger, $dsn, $mysqlUser, $mysqlPassword)
    {
        $this->logger = $logger;
        $this->dsn = $dsn;
        $this->mysqlUser = $mysqlUser;
        $this->mysqlPassword = $mysqlPassword;
        $this->fs = new Filesystem();
    }

    /**
     * set ReportLoadId
     *
     * @param mixed $reportLoadId reportLoadId
     *
     * @return void
     */
    public function setReportLoadId($reportLoadId) {
        $this->reportLoadId = $reportLoadId;
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
                \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
                \PDO::MYSQL_ATTR_LOCAL_INFILE => true
            ]
        );

        $this->connection = DriverManager::getConnection(['pdo' => $this->pdo]);

        $this->metaLogger = new MetaLogger($this->logger, $this->pdo);
        $this->metaLogger->setReportLoadId($this->reportLoadId);

        try {
            $this->logger->info('Creating target PDO table', ['name' => $dumpResult->getEntityName()]);
			$this->metaLogger->start($dumpResult->getEntityName());

            $this->createTableSchema($dumpResult);
            $this->insertDataLoadDataInfile($dumpResult);

			$this->metaLogger->stop($dumpResult->getEntityName(), $this->insertCounter, $this->insertCounterError);

			$this->logger->info(
				'Finished PDO import',
				[
					'totalCount' => $this->insertCounter,
					'errorCount' => $this->insertCounterError
				]
			);
        } catch (\Exception $e) {
            $this->logger->critical('Error in creating the target schema or importing data', ['exception' => $e]);
        } finally {
            $this->logger->info('Removing CSV File', ['filename' => $dumpResult->getDumpFile()]);
            $this->fs->remove($dumpResult->getDumpFile());
        }
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

        $table->addOption('engine', 'InnoDB');

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

        $this->pdo->query($query);

        $this->insertCounter = $dumpResult->getRowCount();
    }
}
