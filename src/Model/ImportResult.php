<?php
namespace Graviton\Mongo2Mysql\Model;

/**
 * @author   List of contributors <https://github.com/libgraviton/mongo2mysql/graphs/contributors>
 * @license  https://opensource.org/licenses/MIT MIT License
 * @link     http://swisscom.ch
 */
class ImportResult {

    private $insertCounter = 0;

    private $insertCounterError = 0;

    /**
     * @return int
     */
    public function getInsertCounter(): int
    {
        return $this->insertCounter;
    }

    /**
     * @param int $insertCounter
     */
    public function setInsertCounter(int $insertCounter): void
    {
        $this->insertCounter = $insertCounter;
    }

    /**
     * @return int
     */
    public function getInsertCounterError(): int
    {
        return $this->insertCounterError;
    }

    /**
     * @param int $insertCounterError
     */
    public function setInsertCounterError(int $insertCounterError): void
    {
        $this->insertCounterError = $insertCounterError;
    }
}
