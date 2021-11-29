<?php

namespace Graviton\Mongo2Mysql\Util;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Schema\Comparator;
use Doctrine\DBAL\Schema\Schema;
use Doctrine\DBAL\Types\Types;
use Psr\Log\LoggerInterface;
use Symfony\Component\Filesystem\Filesystem;

/**
 * @author   List of contributors <https://github.com/libgraviton/mongo2mysql/graphs/contributors>
 * @license  https://opensource.org/licenses/MIT MIT License
 * @link     http://swisscom.ch
 */
class MetaLogger
{

	/**
	 * @var LoggerInterface
	 */
	private $logger;

	/**
	 * @var Connection
	 */
	private $db;

    /**
     * @var Filesystem
     */
	private $fs;

	private $tableName = 'MongoImporterMetadata';

	private $dateFormat = 'Y-m-d H:i:s';

	private $recordId;

	private $reportLoadId;

	public function __construct(LoggerInterface $logger) {
		$this->logger = $logger;
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

	public function start(Connection $db, $elementName)
	{
        $this->db = $db;
		$this->ensureSchema();

		try {
		    $startTime = (new \DateTime())->format($this->dateFormat);

            // local file report
            $this->localFileReport($elementName, $startTime);

            $insertData = [
                'element_name' => $elementName,
                'started_at' => $startTime
            ];

		    $sql = $this->db
                ->createQueryBuilder()
                ->insert($this->tableName)
                ->values([
                    'element_name' => '?',
                    'started_at' => '?'
                ])
                ->setParameters(array_values($insertData));

            $sql->executeStatement();

		    $sql = $this->db
                ->createQueryBuilder()
                ->select(['max(id)'])
                ->from($this->tableName);

            $row = $sql->fetchFirstColumn();

			if (isset($row[0])) {
				$this->recordId = $row[0];
			}

			$this->logger->info('Inserted metadata entry.', ['data' => $insertData, 'recordId' => $this->recordId]);
		} catch (\Exception $e) {
			$this->logger->warning('Error creating metadata entry', ['e' => $e]);
		}
	}

	public function stop(Connection $db, $elementName, $recordCount, $errorRecordCount, int $errored = 0, ?string $exception = null)
	{
		try {
		    $endTime = (new \DateTime())->format($this->dateFormat);

		    // local file report
            $this->localFileReport($elementName, null, $endTime, $recordCount, $errorRecordCount, $errored, $exception);

		    $updateData = [
                'finished_at' => $endTime,
                'record_count' => $recordCount,
                'error_record_count' => $errorRecordCount,
                'errored' => $errored,
                'error_exception' => $exception
            ];

		    $db
                ->createQueryBuilder()
                ->update($this->tableName)
                ->set('finished_at', '?')
                ->set('record_count', '?')
                ->set('error_record_count', '?')
                ->set('errored', '?')
                ->set('error_exception', '?')
                ->where('id = ?')
                ->setParameters(
                    array_merge(
                        array_values($updateData),
                        [$this->recordId]
                    )
                )
                ->executeStatement();

            $this->logger->info('Updated metadata entry.', ['data' => $updateData, 'recordId' => $this->recordId]);
		} catch (\Exception $e) {
			$this->logger->warning('Error updating metadata entry', ['e' => $e]);
		}
	}

	private function ensureSchema()
	{
		try {

            $schema = new Schema();
            $table = $schema->createTable($this->tableName);

            $col = $table->addColumn('id', Types::INTEGER);
            $col->setAutoincrement(true);
            $table->setPrimaryKey(['id']);

            $table->addColumn('element_name', Types::STRING);
            $table->addColumn('started_at', Types::DATETIME_MUTABLE);
            $table->addColumn('finished_at', Types::DATETIME_MUTABLE)->setNotnull(false);
            $table->addColumn('record_count', Types::INTEGER)->setDefault(0)->setNotnull(false);
            $table->addColumn('error_record_count', Types::INTEGER)->setDefault(0)->setNotnull(false);
            $table->addColumn('errored', Types::BOOLEAN)->setDefault(false);
            $table->addColumn('error_exception', Types::STRING)->setNotnull(false);
            $table->addOption('engine', 'Aria');

            $currentSchema = $this->db->createSchemaManager()->createSchema();
            foreach ($currentSchema->getTables() as $existingTable) {
                if ($existingTable->getName() != $this->tableName) {
                    $currentSchema->dropTable($existingTable->getName());
                }
            }

            $comparator = new Comparator();
            $schemaDiff = $comparator->compareSchemas($currentSchema, $schema);

            $queries = $schemaDiff->toSql($this->db->getDatabasePlatform()); // queries to get from one to another schema.

            foreach ($queries as $query) {
                $this->logger->info('Migrating meta table', ['sql' => $query]);
                $this->db->executeStatement($query);
            }

		} catch (\Exception $e) {
		    $message = $e->getMessage();
		    if (strpos($message, 'already exists') == false) {
                $this->logger->warning('Error creating metadata table', ['e' => $e]);
            }
		}
	}

	private function localFileReport($elementName, $startTime = null, $endTime = null, $recordCount = null, $errorRecordCount = null, $errored = 0, $exception = null)
    {
        $reportFile = $this->getReportLoadIdFile();
        if (is_null($reportFile)) {
            return;
        }

        $this->logger->info('Writing local report file', ['file' => $reportFile]);

        if ($this->fs->exists($reportFile)) {
            $baseData = json_decode(file_get_contents($reportFile), true);
        } else {
            $baseData = [];
        }

        if (isset($baseData[$elementName])) {
            $elementData = $baseData[$elementName];
        } else {
            $elementData = [];
        }

        if (!is_null($startTime)) {
            $elementData['startTime'] = $startTime;
        }
        if (!is_null($endTime)) {
            $elementData['endTime'] = $endTime;
        }
        if (!is_null($recordCount)) {
            $elementData['recordCount'] = $recordCount;
        }
        if (!is_null($errorRecordCount)) {
            $elementData['errorRecordCount'] = $errorRecordCount;
        }
        if (!is_null($errored)) {
            $elementData['errored'] = $errored;
        }
        if (!is_null($exception)) {
            $elementData['exception'] = $exception;
        }

        $baseData[$elementName] = $elementData;

        $this->fs->dumpFile($reportFile, json_encode($baseData));
    }

	private function getReportLoadIdFile()
    {
        if (!is_null($this->reportLoadId)) {
            return sys_get_temp_dir().'/'.$this->reportLoadId.'.json';
        }
        return null;
    }
}
