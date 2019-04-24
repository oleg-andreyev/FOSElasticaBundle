<?php
/*
* This file is part of the eCORE CART software.
*
* (c) 2017, ecentria group, inc
*
* For the full copyright and license information, please view the LICENSE
* file that was distributed with this source code.
*/

declare(strict_types = 1);
declare(ticks = 1);

namespace FOS\ElasticaBundle\Command;

use Ecentria\Bundle\ElasticBundle\Exception\CancelTaskFailureException;
use Ecentria\Bundle\ElasticBundle\Exception\CreateIndexFailureException;
use Ecentria\Bundle\ElasticBundle\Exception\DocumentCountDoesNotMatchException;
use Ecentria\Ecentria\Bundle\ElasticBundle\Exception\MultipleAliasesException;
use Ecentria\Ecentria\Bundle\ElasticBundle\Exception\ShardAllocationFailureException;
use Elastica\Client;
use Elastica\Cluster\Health;
use Elastica\Index;
use Elastica\Index\Settings;
use Elastica\Request;
use Elastica\Response;
use Symfony\Bundle\FrameworkBundle\Command\ContainerAwareCommand;
use Symfony\Component\Console\ConsoleEvents;
use Symfony\Component\Console\Event\ConsoleExceptionEvent;
use Symfony\Component\Console\Helper\ProgressBar;
use Symfony\Component\Console\Helper\QuestionHelper;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\NullOutput;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Question\ConfirmationQuestion;
use Symfony\Component\EventDispatcher\EventDispatcherInterface;
use Symfony\Component\Process\Process;

/**
 * Reindex command
 *
 * @author Oleg Andreyev <oleg.andreyev@intexsys.lv>
 */
class ReindexCommand extends ContainerAwareCommand
{
    const INDEX = 'index';
    const SOURCE = 'source';
    const DESTINATION = 'destination';
    const SWITCH_ALIAS = 'switch-alias';
    const TIMEOUT = 'timeout';
    const NUMBER_OF_REPLICAS = 'number_of_replicas';
    const REFRESH_INTERVAL = 'refresh_interval';
    const BATCH = 'batch';
    const NUMBER_OF_SEGMENTS = 'segments';
    const ROUTING_ALLOCATION_INCLUDE_INDEXING = 'routing.allocation.include.indexing';
    const ROUTING_ALLOCATION_INCLUDE_SEARCH = 'routing.allocation.include.search';

    /**
     * Input
     *
     * @var InputInterface
     */
    private $input;

    /**
     * Output
     *
     * @var OutputInterface
     */
    private $output;

    /**
     * Question helper
     *
     * @var QuestionHelper
     */
    private $questionHelper;

    /**
     * Tasks IDs
     *
     * @var array
     */
    private $tasksIds = [];

    /**
     * Client
     *
     * @var \FOS\ElasticaBundle\Elastica\Client
     */
    private $client;

    /**
     * Dispatcher
     *
     * @var EventDispatcherInterface
     */
    private $dispatcher;

    /**
     * Batch
     *
     * @var int
     */
    private $batch;

    /**
     * Segments
     *
     * @var int
     */
    private $segments;

    /**
     * Cancelable task
     *
     * @var bool
     */
    private $cancelableTask = false;

    /**
     * {@inheritdoc}
     */
    protected function configure()
    {
        $this
            ->setName('ecentria:elastica:reindex')
            ->addArgument(self::INDEX, InputArgument::REQUIRED, 'Configured index name (in elasticsearch.yml)')
            ->addArgument(self::SOURCE, InputArgument::REQUIRED, 'Source index (full index name)')
            ->addArgument(self::DESTINATION, InputArgument::OPTIONAL, 'Destination index (full index name), if not provided index will be created')
            ->addOption(self::SWITCH_ALIAS, null, InputOption::VALUE_NONE, 'Switch alias between source and dest')
            // timeout from configuration won't be enough in case of populate we can increase timeout
            // @see EXP-1800, GRS-1683, EXP-2371, EXP-2389 for details
            ->addOption(self::TIMEOUT, null, InputOption::VALUE_OPTIONAL, 'Client timeout in seconds', 3600)
            ->addOption(self::BATCH, null, InputOption::VALUE_OPTIONAL, 'Batch size for reindexing', 1000)
            ->addOption(self::NUMBER_OF_SEGMENTS, null, InputOption::VALUE_OPTIONAL, 'Optimization segment size', 5)
            ->setDescription('Reindexing Elastic index');
    }

    /**
     * {@inheritdoc}
     */
    protected function initialize(InputInterface $input, OutputInterface $output)
    {
        parent::initialize($input, $output);

        $this->input          = $input;
        $this->output         = $output;

        $this->batch          = $input->getOption(self::BATCH);
        $this->segments       = $input->getOption(self::NUMBER_OF_SEGMENTS);

        $this->questionHelper = $this->getHelper('question');

        $this->registerPcntlSignal();

        $container        = $this->getContainer();
        $this->client     = $container->get('fos_elastica.client');
        $this->dispatcher = $container->get('event_dispatcher');
    }

    /**
     * {@inheritdoc}
     */
    protected function execute(InputInterface $input, OutputInterface $output)
    {
        if (!$this->askConfirmation('<question>WARNING! You are about to execute a reindex. Are you sure you wish to continue? (y/n)</question>')) {
            return 1;
        }

        // configuring timeout
        $timeout = $this->input->getOption(self::TIMEOUT);
        $this->client->getConnection()->setTimeout($timeout);

        // command options
        $configuredIndex = $this->input->getArgument(self::INDEX);
        $sourceIndex = $this->input->getArgument(self::SOURCE);
        $destIndex = $this->input->getArgument(self::DESTINATION);
        $switchAlias = $this->input->getOption(self::SWITCH_ALIAS);
        $sourceIndex = clone $this->client->getIndex($sourceIndex);

        // default settings for index
        $settings = $sourceIndex->getSettings();
        $numberOfReplicas = $settings->get(self::NUMBER_OF_REPLICAS);
        $refreshInterval = $settings->get(self::REFRESH_INTERVAL);

        // attache exception handler
        $this->dispatcher->addListener(ConsoleEvents::EXCEPTION, [$this, 'handleConsoleException']);

        // getting destination index or creating it
        $destIndex = $this->getDestinationIndex($destIndex, $configuredIndex);

        // updating settings before reindexing, moving target to special node, removing replicas and disabling refresh interval
        $this->updateSettings(
            '<info>Updating destination index settings</info>',
            $destIndex,
            [
                self::NUMBER_OF_REPLICAS                  => 0,
                self::REFRESH_INTERVAL                    => -1,
                self::ROUTING_ALLOCATION_INCLUDE_SEARCH   => 'no',
                self::ROUTING_ALLOCATION_INCLUDE_INDEXING => 'yes'
            ]
        );

        // starting reindex and waiting
        $this->executeReindex($sourceIndex, $destIndex);

        // staring forcemerge and waiting
        $this->executeForceMerge($destIndex);

        // moving target index to search nodes
        $this->updateSettings(
            '<info>Moving index to search nodes</info>',
            $destIndex,
            [
                self::ROUTING_ALLOCATION_INCLUDE_SEARCH   => 'yes',
                self::ROUTING_ALLOCATION_INCLUDE_INDEXING => 'no'
            ]
        );

        // adding replicas
        $this->updateSettings(
            \sprintf('<info>Set replicas to %d</info>', $numberOfReplicas),
            $destIndex,
            [
                self::NUMBER_OF_REPLICAS => $numberOfReplicas
            ]
        );

        // updating refresh interval
        $this->updateSettings(
            \sprintf('<info>Set refresh_interval to %s</info>', $refreshInterval),
            $destIndex,
            [
                self::REFRESH_INTERVAL   => $refreshInterval
            ]
        );

        $this->validateNumberOfDocuments($sourceIndex, $destIndex);

        if ($switchAlias) {
            if (!$this->askConfirmation('<question>WARNING! You are about to switch aliases. Are you sure you wish to continue? (y/n)</question>')) {
                return 1;
            }

            $this->switchAlias($sourceIndex, $destIndex);
        }

        return 0;
    }

    /**
     * Reindex
     *
     * @param Index $sourceIndex
     * @param Index $destIndex
     *
     * @return void
     */
    private function executeReindex(Index $sourceIndex, Index $destIndex)
    {
        $this->cancelableTask = true;

        $response = $this->client->request(
            '_reindex',
            Request::POST,
            [
                'source' => [
                    'index' => $sourceIndex->getName(),
                    'size'  => $this->batch
                ],
                'dest'   => [
                    'index' => $destIndex->getName()
                ]
            ],
            [
                'wait_for_completion' => false
            ]
        );

        $this->tasksIds[] = $response->getData()['task'];
        $this->waitReindex();

        $this->tasksIds       = [];
        $this->cancelableTask = false;
    }

    /**
     * Optimize index
     *
     * @param Index $destIndex
     *
     * @return void
     */
    private function executeForceMerge(Index $destIndex)
    {
        // _optimize/_forcemerge is not cancelable
        // @see https://github.com/elastic/elasticsearch/issues/17094
        $this->cancelableTask = false;

        // because this operation is not cancelable, it does not have wait_for_completion parameter request is blocked
        // to overcome this issue we're running detached process
        $process = $this->getProcessForceMergeInDetachedMode($this->forceMergeUri($destIndex));
        $process->start();

        $this->waitForceMerge('<info>Running optimization</info>');

        $this->tasksIds = [];
    }

    /**
     * Reallocate index
     *
     * @param string $message
     * @param Index  $index
     * @param array  $settings
     *
     * @return void
     */
    private function updateSettings(string $message, Index $index, array $settings)
    {
        $index->setSettings($settings);
        $this->waitAllocation($index, $message);
    }

    /**
     * Switch alias
     *
     * @param Index $sourceIndex
     * @param Index $destIndex
     *
     * @return void
     */
    private function switchAlias(Index $sourceIndex, Index $destIndex)
    {
        $alias = $this->findAlias($sourceIndex);

        $this->client->request(
            '_aliases',
            Request::POST,
            [
                'actions' => [
                    [
                        'add'    => [
                            'index' => $destIndex->getName(),
                            'alias' => $alias
                        ],
                        'remove' => [
                            'index' => $sourceIndex->getName(),
                            'alias' => $alias
                        ]
                    ]
                ]
            ]
        );
    }

    /**
     * Wait for executeReindex
     *
     * @return void
     */
    private function waitReindex()
    {
        $this->output->writeln('<info>Reindexing</info>');
        /** @var ProgressBar|null $progress */
        $progress = null;

        $taskId = \current($this->tasksIds);

        while (true) {
            $response = $this->client->request('_tasks/' . $taskId, Request::GET, [], ['detailed' => 1]);
            $data = $response->getData();

            $nodes = $data['nodes'];
            if (!\count($nodes)) {
                if ($progress) {
                    $progress->finish();
                    $this->output->write("\n\n");
                }
                return;
            }

            $node = \current($nodes);
            $task = $node['tasks'][$taskId];

            if (!$progress && 0 !== $task['status']['total']) {
                $progress = $this->createProgressBar($task['status']['total']);
            }

            if ($progress) {
                $total = $task['status']['updated'] + $task['status']['created'] + $task['status']['deleted'];
                $progress->setProgress($total);
            }

            $this->pcntlSignalDispatch();
            $this->wait();
        }

        $progress->finish();
        $this->output->write("\n\n");
    }

    /**
     * Get destination index
     *
     * @param string $name
     * @param string $configuredIndexName
     *
     * @return Index
     */
    protected function getDestinationIndex($name, $configuredIndexName): Index
    {
        if ($name) {
            $index = $this->client->getIndex($name);
        } else {
            $index = $this->createDestinationIndex($configuredIndexName);
        }

        return $index;
    }

    /**
     * Handle pcntl signal
     *
     * @param int $signal
     *
     * @return void
     */
    private function handlePcntlSignal(int $signal)
    {
        $this->output->writeln(\sprintf('<comment>Received SIGNAL %d</comment>', $signal));

        // if task is not cancelable ignore signal
        if (!$this->cancelableTask && !$this->askConfirmation('<question>WARNING! Task is not cancelable. Are you sure you want to detach from the process? (y/n)</question>')) {
            return;
        }

        if ($this->cancelableTask) {
            foreach ($this->tasksIds as $taskId) {
                $this->cancelTask($taskId, $signal);
            }
        }

        exit(\sprintf('Received Kill signal %d', $signal));
    }

    /**
     * Handle console exception
     *
     * @param ConsoleExceptionEvent $event
     *
     * @return void
     */
    public function handleConsoleException(ConsoleExceptionEvent $event)
    {
        // if task is not cancelable ignore signal
        if ($this->cancelableTask) {
            foreach ($this->tasksIds as $taskId) {
                $this->cancelTask($taskId, $event);
            }
        } else {
            $this->output->writeln('<comment>Task is not cancelable</comment>');
        }
    }

    /**
     * Cancel task
     *
     * @param string                    $taskId
     * @param ConsoleExceptionEvent|int $event
     *
     * @return void
     *
     * @throws \RuntimeException
     */
    public function cancelTask($taskId, $event = null)
    {
        // sending _cancel task request
        $this->client->request('_tasks/' . $taskId . '/_cancel', Request::POST);

        $timeout = 10;
        $taskCanceled = false;
        $start = microtime(true);

        while (!$taskCanceled) {
            // validation result
            $response = $this->client->request('_tasks/' . $taskId);

            $timeDiff = (\microtime(true) - $start);
            $responseData = $response->getData();
            $taskCanceled = \count($responseData['nodes']) === 0;

            if ($timeDiff >= $timeout && !$taskCanceled) {
                $code = $event;
                $prevException = null;

                if ($event instanceof ConsoleExceptionEvent) {
                    $prevException = $event->getException();
                    $code = $prevException->getCode();
                }

                throw new CancelTaskFailureException(
                    \sprintf('Failed to cancel task %s within %d seconds', $taskId, $timeout),
                    $code,
                    $prevException
                );
            }
        }

        $this->tasksIds = [];
    }

    /**
     * Wait for allocation
     *
     * @param Index  $destIndex
     * @param string $message
     *
     * @return void
     *
     * @throws ShardAllocationFailureException
     */
    protected function waitAllocation(Index $destIndex, string $message)
    {
        // wait for allocation
        $timeout   = 3600; // 1h in seconds

        $response = $this->client->request('_cluster/health/' . $destIndex->getName(), Request::GET, [], ['level' => 'shards']);

        $clusterHealth = new class ($this->client, $response) extends Health {
            /**
             * constructor.
             *
             * @param Client   $client
             * @param Response $response
             */
            public function __construct(Client $client, Response $response)
            {
                $this->_client = $client;
                $this->_data   = $response->getData();
            }
        };

        $start = microtime(true);
        $this->output->writeln($message);

        $progress = $this->createProgressBar();

        do {
            $indices = $clusterHealth->getIndices();
            $indices = \array_filter($indices, function (Health\Index $indexHealth) use ($destIndex) {
                return $indexHealth->getName() === $destIndex->getName();
            });

            $indexHealth = \array_pop($indices);

            $allocated = $indexHealth->getActiveShards() === (($indexHealth->getNumberOfReplicas() + 1) * $indexHealth->getNumberOfShards()) && 0 === $indexHealth->getInitializingShards() && 0 === $indexHealth->getRelocatingShards();

            $timeDiff = (\microtime(true) - $start);
            if (!$allocated) {
                $progress->advance();
                $clusterHealth->refresh();
                $this->pcntlSignalDispatch();
                $this->wait(); // 0.5s
            }

            if ($timeDiff >= $timeout) {
                throw new ShardAllocationFailureException('Shard allocation failed after 1h');
            }
        } while (false === $timeout || false === $allocated);

        $progress->finish();

        $this->output->write("\n\n");
    }

    /**
     * Wait for force merge
     *
     * @param string $message
     *
     * @return void
     */
    private function waitForceMerge(string $message)
    {
        $progress = $this->createProgressBar();

        $response = $this->client->request('_nodes/_local');
        $data = $response->getData();

        // response should return current node
        $nodes = \array_keys($data['nodes']);
        $targetNode = \current($nodes);

        $this->output->writeln($message);

        do {
            $response = $this->client->request(
                '_tasks',
                Request::GET,
                [],
                [
                    'detailed' => 1,
                    'actions'  => '*optimize*',
                    'node_id'  => $targetNode
                ]
            );
            $data = $response->getData();
            $nodes = $data['nodes'];
            if (!\count($nodes)) {
                $progress->finish();
                $this->output->write("\n\n");

                $this->tasksIds = [];
                return;
            }

            $tasks = $nodes[$targetNode]['tasks'];

            // removing child tasks
            $rootTasks = \array_filter($tasks, function ($task) {
                return !isset($task['parent_task_id']);
            });

            $this->tasksIds = \array_values(\array_map(function ($task) {
                return $task['node'] . ':' . $task['id'];
            }, $rootTasks));

            $progress->advance();

            $this->pcntlSignalDispatch();
            $this->wait(); // 0.5s
        } while ($this->tasksIds);

        $progress->finish();

        $this->output->write("\n\n");
    }

    /**
     * Find alias
     *
     * @param Index $sourceIndex
     *
     * @return string|null
     *
     * @throws MultipleAliasesException
     */
    private function findAlias(Index $sourceIndex)
    {
        $aliases = $sourceIndex->getAliases();
        if (\count($aliases) > 1) {
            throw new MultipleAliasesException(
                sprintf(
                    'Index %s has multiple aliases: [%s]',
                    $sourceIndex->getName(),
                    implode(', ', $aliases)
                )
            );
        }

        return array_shift($aliases);
    }

    /**
     * Register pcntl signal
     *
     * @return bool
     *
     * @throws \BadFunctionCallException
     */
    private function registerPcntlSignal(): bool
    {
        $success = false;

        if (\extension_loaded('pcntl')) {
            if (!\function_exists('pcntl_signal')) {
                throw new \BadFunctionCallException("Function 'pcntl_signal' is referenced in the php.ini 'disable_functions' and can't be called.");
            }

            foreach ([\SIGTSTP, \SIGTERM, \SIGINT] as $signal) {
                $success = \pcntl_signal($signal, [$this, 'handlePcntlSignal']);
            }
        }

        return $success;
    }

    /**
     * Pcntl signal dispatch
     *
     * @return bool
     *
     * @throws \BadFunctionCallException
     */
    private function pcntlSignalDispatch(): bool
    {
        if (\extension_loaded('pcntl')) {
            if (!\function_exists('pcntl_signal_dispatch')) {
                throw new \BadFunctionCallException("Function 'pcntl_signal_dispatch' is referenced in the php.ini 'disable_functions' and can't be called.");
            }
        }

        return \pcntl_signal_dispatch();
    }

    /**
     * Build uri
     *
     * @param Index $destIndex
     *
     * @return string
     */
    private function forceMergeUri(Index $destIndex)
    {
        $connection = $this->client->getConnection();

        // If url is set, url is taken. Otherwise port, host and path
        $url = $connection->hasConfig('url') ? $connection->getConfig('url') : '';

        if (!empty($url)) {
            $baseUri = $url;
        } else {
            $baseUri = 'http://' . $connection->getHost() . ':' . $connection->getPort() . '/' . $connection->getPath();
        }

        $baseUri .= $destIndex->getName() . '/_forcemerge';

        $query = ['max_num_segments' => $this->segments];

        if (!empty($query)) {
            $baseUri .= '?' . http_build_query($query);
        }

        return $baseUri;
    }

    /**
     * Build detached command
     *
     * @param string $uri
     *
     * @return Process
     */
    private function getProcessForceMergeInDetachedMode(string $uri): Process
    {
        $connection = $this->client->getConnection();

        $connectTimeout = $connection->getConnectTimeout();
        $proxy          = $connection->getProxy();
        $username       = $connection->getUsername();
        $password       = $connection->getPassword();
        $curlConfig     = $connection->hasConfig('curl') ? $connection->getConfig('curl') : [];
        $headersConfig  = $connection->hasConfig('headers') ? $connection->getConfig('headers') : [];

        $data = \compact('connectTimeout', 'proxy', 'username', 'password', 'curlConfig', 'headersConfig');
        $dataSerialized = \serialize($data);

        $script = <<<SCRIPT
<?php

extract(unserialize('$dataSerialized'));

\$conn = curl_init();
curl_setopt(\$conn, CURLOPT_URL, "$uri");
curl_setopt(\$conn, CURLOPT_FORBID_REUSE, 0);
curl_setopt(\$conn, CURLOPT_TIMEOUT, 3600);
curl_setopt(\$conn, CURLOPT_ENCODING, '');
curl_setopt(\$conn, CURLOPT_NOBODY, false);
curl_setopt(\$conn, CURLOPT_CUSTOMREQUEST, 'POST');
if (\$connectTimeout > 0) { curl_setopt(\$conn, CURLOPT_CONNECTTIMEOUT, \$connectTimeout); }
if (!is_null(\$proxy)) { curl_setopt(\$conn, CURLOPT_PROXY, \$proxy); }
if (!is_null(\$username) && !is_null(\$password)) {
    curl_setopt(\$conn, CURLOPT_HTTPAUTH, CURLAUTH_ANY);
    curl_setopt(\$conn, CURLOPT_USERPWD, "\$username:\$password");
}
foreach (\$curlConfig as \$key => \$param) {
    curl_setopt(\$conn, \$key, \$param);
}
ob_start();
curl_exec(\$conn);
\$responseString = ob_get_clean();
\$end = microtime(true);
\$errorNumber = curl_errno(\$conn);

echo \$responseString;
return \$errorNumber;

SCRIPT;

        $tempnam = \tempnam('/tmp', 'ecentria-executeReindex-command');
        \file_put_contents($tempnam, $script);

        $command = \sprintf('unset XDEBUG_CONFIG && php -f %s & > /dev/null', $tempnam);

        return new Process($command);
    }

    /**
     * Wait
     *
     * @return void
     */
    private function wait()
    {
        \usleep(500000); // 0.5s
    }

    /**
     * Create progress bar
     *
     * @param int|null $max
     *
     * @return ProgressBar
     */
    private function createProgressBar(int $max = null): ProgressBar
    {
        return new ProgressBar($this->output, $max);
    }

    /**
     * Create destination index
     *
     * @param string $index
     *
     * @return Index
     *
     * @throws \RuntimeException
     */
    private function createDestinationIndex(string $index): Index
    {
        $this->output->writeln('<comment>WARNING! Destination index was not provided, running `fos:elastica:create`</comment>');

        $indexManager = $this->getContainer()->get('fos_elastica.index_manager');

        /** @var Index[] $indices */
        $indices = $indexManager->getAllIndexes();

        $command = $this->getApplication()->find('ecentria:elastica:create');
        $args = [
            '--index'   => $index,
            '--env'     => $this->input->getOption('env'),
            'command'   => $command->getName()
        ];

        $exitCode = $command->run(new ArrayInput($args), new NullOutput());
        if ($exitCode) {
            throw new CreateIndexFailureException(\sprintf('Failed to create index exited with code [%s]', $exitCode));
        }

        return $indices[$index];
    }

    /**
     * Ask confirmation
     *
     * @param string $message
     *
     * @return bool
     */
    private function askConfirmation(string $message): bool
    {
        return $this->questionHelper->ask($this->input, $this->output, new ConfirmationQuestion($message));
    }

    /**
     * Validate number of documents
     *
     * @param Index $sourceIndex
     * @param Index $destIndex
     *
     * @return void
     *
     * @throws DocumentCountDoesNotMatchException
     */
    private function validateNumberOfDocuments(Index $sourceIndex, Index $destIndex)
    {
        if ($sourceIndex->count() === $destIndex->count()) {
            throw new DocumentCountDoesNotMatchException('Document count does not match with original index');
        }
    }
}
