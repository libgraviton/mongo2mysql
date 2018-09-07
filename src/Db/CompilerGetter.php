<?php
namespace Graviton\Mongo2Mysql\Db;

use Opis\Database\Connection;

/**
 * @author   List of contributors <https://github.com/libgraviton/mongo2mysql/graphs/contributors>
 * @license  https://opensource.org/licenses/MIT MIT License
 * @link     http://swisscom.ch
 */
class CompilerGetter {

	public static function getInstance(Connection $connection)
	{
		switch ($connection->getDriver()) {
			case 'mysql':
				return new Maria();
				break;
			case 'dblib':
			case 'mssql':
			case 'sqlsrv':
			case 'sybase':
				return new SqlServer();
				break;
		}
	}

}
