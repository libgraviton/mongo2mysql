<?php
/**
 * import command
 */
namespace Graviton\Mongo2Mysql\Command;

use Graviton\Mongo2Mysql\PdoImporter;
use Graviton\Mongo2Mysql\MongoDumper;
use Graviton\Mongo2Mysql\Util\DbUtil;
use Graviton\Mongo2Mysql\Util\Logger;
use Graviton\Mongo2Mysql\Util\MetaLogger;
use Psr\Log\LoggerInterface;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputDefinition;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Filesystem\Filesystem;

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
                        new InputOption(
                            'sourceMongoPipelineFile',
                            null,
                            InputOption::VALUE_OPTIONAL,
                            'Path to a php compliant aggregation pipeline file to export'
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
                            InputArgument::OPTIONAL,
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
                            'stringFieldLimit',
                            null,
                            InputOption::VALUE_OPTIONAL,
                            'Limit when a field length is a TEXT; not a VARCHAR',
                            3000
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
							'targetTableName',
							null,
							InputOption::VALUE_OPTIONAL,
							'How the target table should be named'
						),
                        new InputOption(
                            'reportLoadId',
                            null,
                            InputOption::VALUE_OPTIONAL,
                            'If given, we report our progress as this load ID'
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

        $entityName = $input->getArgument('sourceMongoCollection');
        if (!is_null($input->getOption('targetTableName'))) {
            $entityName = $input->getOption('targetTableName');
        }

        $metaLogger = new MetaLogger($logger);
        if (!is_null($input->getOption('reportLoadId'))) {
            $metaLogger->setReportLoadId($input->getOption('reportLoadId'));
        }

        // start entity...
        $metaLogger->start(DbUtil::getConnection($input), $entityName);

        try {

            $dumpResult = $this->mongoDump(
                $logger,
                $input->getArgument('sourceMongoDsn'),
                $input->getArgument('sourceMongoDb'),
                $input->getArgument('sourceMongoCollection'),
                $input->getOption('tempDir'),
                $input->getOption('mongoFilter'),
                $input->getOption('tz'),
                $input->getOption('schemaSampleSize'),
                $input->getOption('sourceMongoPipelineFile')
            );

            $dumpResult->setEntityName($entityName);

            $importer = new PdoImporter(
                $logger,
                DbUtil::getConnection($input)
            );
            $importResult = $importer->import($dumpResult);

            $metaLogger->stop(DbUtil::getConnection($input), $entityName, $importResult->getInsertCounter(), $importResult->getInsertCounterError());
        } catch (\Exception $e) {
            $metaLogger->stop(DbUtil::getConnection($input), $entityName, 0, 0, 1, get_class($e).': '.$e->getMessage());
            $logger->critical('Exception happened during execution', ['e' => $e]);
        }

        return 0;
    }

    protected function mongoDump(
        LoggerInterface $logger,
        $sourceDsn,
        $sourceDb,
        $sourceMongoCollection,
        $outDir,
        $mongoFilter,
        $timezone,
        $sampleSchemaSize,
        $sourceMongoPipelineFile,
        $outputFileName = null,
        $writeFieldNames = false,
        $nullValue = null,
        $skipTooLongFields = true
    ) {
        $dumper = new MongoDumper(
            $logger,
            $sourceDsn,
            $sourceDb,
            $sourceMongoCollection,
            $outDir,
            $mongoFilter
        );
        $dumper->setTimezone($timezone);
        $dumper->setSchemaSampleSize(intval($sampleSchemaSize));

        // pipeline file?
        if (!is_null($sourceMongoPipelineFile)) {
            $pipelineFile = $sourceMongoPipelineFile;

            if (!(new Filesystem())->exists($pipelineFile)) {
                throw new \LogicException('File ' . $pipelineFile . ' does not exist!');
            }

            $dumper->setPipelineFile($pipelineFile);
        }

        $dumper->setOutputFile($outputFileName);
        $dumper->setWriteFieldNames($writeFieldNames);

        if (!is_null($nullValue)) {
            $dumper->setNullValue($nullValue);
        }

        $dumper->setSkipTooLongFields($skipTooLongFields);

        return $dumper->dump();
    }
}
