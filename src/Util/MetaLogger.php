<?php

namespace Graviton\Mongo2Mysql\Util;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Types\Type;
use Symfony\Component\Filesystem\Filesystem;

/**
 * @author   List of contributors <https://github.com/libgraviton/mongo2mysql/graphs/contributors>
 * @license  https://opensource.org/licenses/MIT MIT License
 * @link     http://swisscom.ch
 */
class MetaLogger
{

	/**
	 * @var Logger
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

	public function __construct(\Monolog\Logger $logger, \PDO $pdo) {
		$this->logger = $logger;
		$this->db = DriverManager::getConnection(['pdo' => $pdo]);
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

	public function start($elementName)
	{
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

		    $sql->execute();

		    $sql = $this->db
                ->createQueryBuilder()
                ->select(['max(id)'])
                ->from($this->tableName)
                ->execute();
		    $row = $sql->fetch(\PDO::FETCH_BOTH);

			if (isset($row[0])) {
				$this->recordId = $row[0];
			}

			$this->logger->info('Inserted metadata entry.', ['data' => $insertData, 'recordId' => $this->recordId]);
		} catch (\Exception $e) {
			$this->logger->warn('Error creating metadata entry', ['e' => $e]);
		}
	}

	public function stop($elementName, $recordCount, $errorRecordCount)
	{
		try {
		    $endTime = (new \DateTime())->format($this->dateFormat);

		    // local file report
            $this->localFileReport($elementName, null, $endTime, $recordCount, $errorRecordCount);

		    $updateData = [
                'finished_at' => $endTime,
                'record_count' => $recordCount,
                'error_record_count' => $errorRecordCount
            ];

		    $this->db
                ->createQueryBuilder()
                ->update($this->tableName)
                ->set('finished_at', '?')
                ->set('record_count', '?')
                ->set('error_record_count', '?')
                ->where('id = ?')
                ->setParameters(
                    array_merge(
                        array_values($updateData),
                        [$this->recordId]
                    )
                )
                ->execute();

            $this->logger->info('Updated metadata entry.', ['data' => $updateData, 'recordId' => $this->recordId]);
		} catch (\Exception $e) {
			$this->logger->warn('Error updating metadata entry', ['e' => $e]);
		}
	}

	private function ensureSchema()
	{
		try {
		    $schemaManager = $this->db->getSchemaManager();
		    $schema = $schemaManager->createSchema();
		    $newSchema = clone $schema;

		    if ($newSchema->hasTable($this->tableName)) {
		        $table = $newSchema->getTable($this->tableName);
            } else {
		        $table = $newSchema->createTable($this->tableName);
            }

		    if (!$table->hasColumn('id')) {
		        $col = $table->addColumn('id', Type::INTEGER);
                $col->setAutoincrement(true);
                $table->setPrimaryKey(['id']);
            }

            if (!$table->hasColumn('element_name')) {
                $table->addColumn('element_name', Type::STRING);
            }
            if (!$table->hasColumn('started_at')) {
                $table->addColumn('started_at', Type::DATETIME);
            }
            if (!$table->hasColumn('finished_at')) {
                $table->addColumn('finished_at', Type::DATETIME)->setNotnull(false);
            }
            if (!$table->hasColumn('record_count')) {
                $table->addColumn('record_count', Type::INTEGER)->setDefault(0)->setNotnull(false);
            }
            if (!$table->hasColumn('error_record_count')) {
                $table->addColumn('error_record_count', Type::INTEGER)->setDefault(0)->setNotnull(false);
            }

            $migrations = $schema->getMigrateToSql($newSchema, $schemaManager->getDatabasePlatform());
            foreach ($migrations as $migration) {
                $this->db->exec($migration);
            }
		} catch (\Exception $e) {
		    $message = $e->getMessage();
		    if (strpos($message, 'already exists') == false) {
                $this->logger->warn('Error creating metadata table', ['e' => $e]);
            }
		}
	}

	private function localFileReport($elementName, $startTime = null, $endTime = null, $recordCount = null, $errorRecordCount = null)
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
