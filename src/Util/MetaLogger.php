<?php

namespace Graviton\Mongo2Mysql\Util;
use Opis\Database\Connection;
use Opis\Database\Database;
use Opis\Database\Schema\CreateTable;

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

	private $tableName = 'MongoImporterMetadata';

	private $dateFormat = 'Y-m-d H:i:s';

	private $recordId;

	public function __construct(\Monolog\Logger $logger, Connection $conn) {
		$this->logger = $logger;
		$this->db = new Database($conn);
	}

	public function start($elementName)
	{
		$this->ensureSchema();

		try {
			$this->db->insert(
				[
					'element_name' => $elementName,
					'started_at' => (new \DateTime())->format($this->dateFormat)
				]
			)->into($this->tableName);

			// fetch id
			$data = $this->db->from($this->tableName)
				->select(function ($include) {
					$include->max('id', 'maxid');
				})
				->first();

			if (isset($data['maxid'])) {
				$this->recordId = (int)$data['maxid'];
			}
		} catch (\Exception $e) {
			$this->logger->warn('Error creating metadata entry', ['e' => $e]);
		}
	}

	public function stop($elementName, $recordCount, $errorRecordCount)
	{
		try {
			$this->db->update($this->tableName)
				->where('id')->is($this->recordId)
				->set([
					'finished_at' => (new \DateTime())->format($this->dateFormat),
					'record_count' => $recordCount,
					'error_record_count' => $errorRecordCount
				]);
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
			$this->logger->warn('Error creating metadata table', ['e' => $e]);
		}
	}
}
