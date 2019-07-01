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
    public const FIELDTYPE_BOOL = 'boolean';
    public const FIELDTYPE_INT = 'integer';
    public const FIELDTYPE_SMALLINT = 'smallint';

    private $success = true;

    private $hasFieldSpec = false;

    private $rowCount = 0;

    private $entityName;

    private $dumpFile;

    private $fields = [];

    private $fieldTypes = [];

    private $fieldLengths = [];

    private $fieldNullables = [];

    private $fieldsPrimary = [];

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
     * @return bool
     */
    public function isHasFieldSpec(): bool
    {
        return $this->hasFieldSpec;
    }

    /**
     * @param bool $hasFieldSpec
     */
    public function setHasFieldSpec(bool $hasFieldSpec): void
    {
        $this->hasFieldSpec = $hasFieldSpec;
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

    /**
     * get FieldNullables
     *
     * @return array FieldNullables
     */
    public function getFieldNullables() {
        return $this->fieldNullables;
    }

    /**
     * set FieldNullables
     *
     * @param array $fieldNullables fieldNullables
     *
     * @return void
     */
    public function setFieldNullables($fieldNullables) {
        $this->fieldNullables = $fieldNullables;
    }

    /**
     * get FieldsPrimary
     *
     * @return array FieldsPrimary
     */
    public function getFieldsPrimary() {
        return $this->fieldsPrimary;
    }

    /**
     * set FieldsPrimary
     *
     * @param array $fieldsPrimary fieldsPrimary
     *
     * @return void
     */
    public function setFieldsPrimary($fieldsPrimary) {
        $this->fieldsPrimary = $fieldsPrimary;
    }
}
