<?php
/**
 * import command
 */
namespace Graviton\Mongo2Mysql\Command;

use Graviton\Mongo2Mysql\PdoImporter;
use Graviton\Mongo2Mysql\MongoDumper;
use Graviton\Mongo2Mysql\Util\Logger;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputDefinition;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

/**
 * @author   List of contributors <https://github.com/libgraviton/mongo2mysql/graphs/contributors>
 * @license  https://opensource.org/licenses/MIT MIT License
 * @link     http://swisscom.ch
 */
class ImportCommand extends Command
{

    /**
     * Configures the current command.
     *
     * @return void
     */
    protected function configure()
    {
        $this
            ->setName('import')
            ->setDescription('Import collections from MongoDB into MySQL tables.')
            ->setDefinition(
                new InputDefinition(
                    [
                        new InputArgument(
                            'sourceMongoDsn',
                            InputArgument::REQUIRED,
                            'Mongo DSN for the data source (like "mongodb://localhost:27017/db")'
                        ),
                        new InputArgument(
                            'sourceMongoDb',
                            InputArgument::REQUIRED,
                            'Database in the MongoDB to select'
                        ),
                        new InputArgument(
                            'sourceMongoCollection',
                            InputArgument::REQUIRED,
                            'Collection in the MongoDB to select'
                        ),
                        new InputArgument(
                            'targetMysqlDsn',
                            InputArgument::REQUIRED,
                            'MySQL PHP PDO DSN (like "mysql:host=192.168.1.5;dbname=demo")'
                        ),
                        new InputArgument(
                            'targetMysqlUser',
                            InputArgument::REQUIRED,
                            'MySQL user'
                        ),
                        new InputArgument(
                            'targetMysqlPassword',
                            InputArgument::REQUIRED,
                            'MySQL password'
                        ),
                        new InputOption(
                            'tempDir',
                            null,
                            InputOption::VALUE_OPTIONAL,
                            'Directory for temporary files',
                            sys_get_temp_dir()
                        ),
                        new InputOption(
                            'tz',
                            null,
                            InputOption::VALUE_OPTIONAL,
                            'When writing dates to MySQL, in which timezone should we represent DATETIMEs?',
                            'UTC'
                        ),
                        new InputOption(
                            'schemaSampleSize',
                            null,
                            InputOption::VALUE_OPTIONAL,
                            'How many records we should read in the first pass to determine schema fields (limit)',
                            1000
                        ),
						new InputOption(
							'bulkInsertSize',
							null,
							InputOption::VALUE_OPTIONAL,
							'How many records should be inserted with one INSERT statement',
							300
						),
						new InputOption(
							'mongoFilter',
							null,
							InputOption::VALUE_IS_ARRAY + InputOption::VALUE_OPTIONAL,
							'Possible filter to select data. Format is "fieldName[OP]fieldValue". '.
							'Possible ops: [==, !=, >=, <=, >, <]. '.
							'If you want to filter date value, use YYYY-MM-DD format, that will be parsed. '.
							'This parameter can be supplied multiple times. '.
							'Example: fieldName>=2010-10-10'
						)
                    ]
                )
            );
    }

    /**
     * Executes the current command.
     *
     * @param InputInterface  $input  User input on console
     * @param OutputInterface $output Output of the command
     *
     * @return void
     */
    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $output->setVerbosity(OutputInterface::VERBOSITY_VERY_VERBOSE);
        $logger = Logger::getLogger($output, 'mysql2mongo');

        $dumper = new MongoDumper(
            $logger,
            $input->getArgument('sourceMongoDsn'),
            $input->getArgument('sourceMongoDb'),
            $input->getArgument('sourceMongoCollection'),
            $input->getOption('tempDir'),
			$input->getOption('mongoFilter')
        );
        $dumper->setTimezone($input->getOption('tz'));
        $dumper->setSchemaSampleSize(intval($input->getOption('schemaSampleSize')));

        $dumpResult = $dumper->dump();

        $importer = new PdoImporter(
            $logger,
            $input->getArgument('targetMysqlDsn'),
            $input->getArgument('targetMysqlUser'),
            $input->getArgument('targetMysqlPassword'),
			$input->getOption('bulkInsertSize')
        );
        $importer->import($dumpResult);
    }
}
