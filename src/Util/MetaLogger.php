<?php

namespace Graviton\Mongo2Mysql\Util;

use Opis\Database\Connection;
use Opis\Database\Database;
use Opis\Database\Schema\CreateTable;
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
	 * @var Database
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

	public function __construct(\Monolog\Logger $logger, Connection $conn) {
		$this->logger = $logger;
		$this->db = new Database($conn);
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

			$this->db->insert($insertData)->into($this->tableName);

			// fetch id
			$data = $this->db->from($this->tableName)
				->select(function ($include) {
					$include->max('id', 'maxid');
				})
				->first();

			if (isset($data['maxid'])) {
				$this->recordId = (int)$data['maxid'];
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

			$this->db->update($this->tableName)
				->where('id')->is($this->recordId)
				->set($updateData);

            $this->logger->info('Updated metadata entry.', ['data' => $updateData, 'recordId' => $this->recordId]);
		} catch (\Exception $e) {
			$this->logger->warn('Error updating metadata entry', ['e' => $e]);
		}
	}

	private function ensureSchema()
	{
		try {
			$this->db->schema()->create($this->tableName, function (CreateTable $creater) {
				$idCol = $creater->integer('id');
				$creater->autoincrement($idCol);
				$creater->string('element_name');
				$creater->dateTime('started_at');
				$creater->dateTime('finished_at');
				$creater->integer('record_count');
				$creater->integer('error_record_count');
			});
		} catch (\Exception $e) {
		    $message = $e->getMessage();
		    if (strpos($message, 'already exists') == false) {
		        // we don't need to know that it already exists
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
