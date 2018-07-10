<?php

namespace Graviton\Mongo2Mysql\Util;
use Symfony\Bridge\Monolog\Handler\ConsoleHandler;
use Symfony\Component\Console\Output\OutputInterface;

/**
 * @author   List of contributors <https://github.com/libgraviton/mongo2mysql/graphs/contributors>
 * @license  https://opensource.org/licenses/MIT MIT License
 * @link     http://swisscom.ch
 */
class Logger
{

    /**
     * @param OutputInterface $output
     * @param $name
     * @return \Monolog\Logger
     */
    public static function getLogger(OutputInterface $output, $name)
    {
        $logger = new \Monolog\Logger($name);
        $logger->pushHandler(new ConsoleHandler($output), \Monolog\Logger::DEBUG);
        return $logger;
    }
}
