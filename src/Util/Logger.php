<?php

namespace Graviton\Mongo2Mysql\Util;
use Monolog\Formatter\LineFormatter;
use Psr\Log\LoggerInterface;
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
    public static function getLogger(OutputInterface $output, $name) : LoggerInterface
    {
        $format = "[%datetime%] %level_name%: %message% %context%\n";
		$formatter = new LineFormatter($format, \DateTimeInterface::ISO8601);
		$handler = new ConsoleHandler($output);
		$handler->setFormatter($formatter);

        $logger = new \Monolog\Logger($name);
        $logger->pushHandler($handler, \Monolog\Logger::DEBUG);
        return $logger;
    }
}
