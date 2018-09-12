<?php
namespace Graviton\Mongo2Mysql\Model;

/**
 * @author   List of contributors <https://github.com/libgraviton/mongo2mysql/graphs/contributors>
 * @license  https://opensource.org/licenses/MIT MIT License
 * @link     http://swisscom.ch
 */
class DumpResult {

    public const FIELDTYPE_STRING = 'string';
    public const FIELDTYPE_DATETIME = 'datetime';
    public const FIELDTYPE_BOOL = 'bool';
    public const FIELDTYPE_INT = 'int';

    private $success = true;

    private $rowCount = 0;

    private $entityName;

    private $dumpFile;

    private $fields = [];

    private $fieldTypes = [];

    private $fieldLengths = [];

    /**
     * get Success
     *
     * @return bool Success
     */
    public function isSuccess()
    {
        return $this->success;
    }

    /**
     * set Success
     *
     * @param bool $success success
     *
     * @return void
     */
    public function setSuccess($success)
    {
        $this->success = $success;
    }

	/**
	 * get RowCount
	 *
	 * @return int RowCount
	 */
	public function getRowCount() {
		return $this->rowCount;
	}

	/**
	 * set RowCount
	 *
	 * @param int $rowCount rowCount
	 *
	 * @return void
	 */
	public function setRowCount($rowCount) {
		$this->rowCount = $rowCount;
	}

    /**
     * get EntityName
     *
     * @return mixed EntityName
     */
    public function getEntityName()
    {
        return $this->entityName;
    }

    /**
     * set EntityName
     *
     * @param mixed $entityName entityName
     *
     * @return void
     */
    public function setEntityName($entityName)
    {
        $this->entityName = $entityName;
    }

    /**
     * get DumpFile
     *
     * @return mixed DumpFile
     */
    public function getDumpFile()
    {
        return $this->dumpFile;
    }

    /**
     * set DumpFile
     *
     * @param mixed $dumpFile dumpFile
     *
     * @return void
     */
    public function setDumpFile($dumpFile)
    {
        $this->dumpFile = $dumpFile;
    }

    /**
     * get Fields
     *
     * @return array Fields
     */
    public function getFields()
    {
        return $this->fields;
    }

    /**
     * set Fields
     *
     * @param array $fields fields
     *
     * @return void
     */
    public function setFields($fields)
    {
        $this->fields = $fields;
    }

    /**
     * get FieldTypes
     *
     * @return array FieldTypes
     */
    public function getFieldTypes()
    {
        return $this->fieldTypes;
    }

    /**
     * set FieldTypes
     *
     * @param array $fieldTypes fieldTypes
     *
     * @return void
     */
    public function setFieldTypes($fieldTypes)
    {
        $this->fieldTypes = $fieldTypes;
    }

    /**
     * get FieldLengths
     *
     * @return array FieldLengths
     */
    public function getFieldLengths()
    {
        return $this->fieldLengths;
    }

    /**
     * set FieldLengths
     *
     * @param array $fieldLengths fieldLengths
     *
     * @return void
     */
    public function setFieldLengths($fieldLengths)
    {
        $this->fieldLengths = $fieldLengths;
    }
}
