<?php

namespace Graviton\Mongo2Mysql\Util;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use League\Uri\Uri;
use League\Uri\Components\Query;
use Symfony\Component\Console\Input\InputInterface;

/**
 * @author   List of contributors <https://github.com/libgraviton/mongo2mysql/graphs/contributors>
 * @license  https://opensource.org/licenses/MIT MIT License
 * @link     http://swisscom.ch
 */
class DbUtil
{
    public static function getPdo(InputInterface $input) : \PDO {
        return new \PDO(
            $input->getArgument('targetMysqlDsn'),
            $input->getArgument('targetMysqlUser'),
            $input->getArgument('targetMysqlPassword'),
            [
                \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
                \PDO::MYSQL_ATTR_LOCAL_INFILE => true
            ]
        );
    }

    /**
     * @return \Doctrine\DBAL\Connection
     */
    public static function getConnection(InputInterface $input) : Connection {

        $pdoDsn = $input->getArgument('targetMysqlDsn');

        // dirty way to parse a pdo psn
        $pdoDsn = str_replace(
            [
                ':',
                ';'
            ],
            [
                '://host?',
                '&'
            ],
            $pdoDsn
        );

        // parse it
        $uri = Uri::createFromString($pdoDsn);
        $uriQuery = Query::createFromUri($uri);

        $params = [
            'driver' => 'pdo_'.$uri->getScheme(),
            'host' => $uriQuery->get('host'),
            'dbname' => $uriQuery->get('dbname'),
            'user' => $input->getArgument('targetMysqlUser'),
            'password' => $input->getArgument('targetMysqlPassword'),
            'driverOptions' => [
                \PDO::MYSQL_ATTR_LOCAL_INFILE => 1
            ]
        ];

        if (!empty($uri->getPort())) {
            $params['port'] = $uri->getPort();
        }

        return DriverManager::getConnection($params);
    }
}
