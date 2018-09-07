<?php
namespace Graviton\Mongo2Mysql\Db;

use Opis\Database\SQL\Compiler\MySQL;

/**
 * @author   List of contributors <https://github.com/libgraviton/mongo2mysql/graphs/contributors>
 * @license  https://opensource.org/licenses/MIT MIT License
 * @link     http://swisscom.ch
 */
class Maria extends MySQL {

	function wrap($value) {
		return parent::wrap($value);
	}

}
