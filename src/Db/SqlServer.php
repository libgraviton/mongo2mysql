<?php
namespace Graviton\Mongo2Mysql\Db;

/**
 * @author   List of contributors <https://github.com/libgraviton/mongo2mysql/graphs/contributors>
 * @license  https://opensource.org/licenses/MIT MIT License
 * @link     http://swisscom.ch
 */
class SqlServer extends \Opis\Database\SQL\Compiler\SQLServer {

	function wrap($value) {
		return parent::wrap($value);
	}

}
