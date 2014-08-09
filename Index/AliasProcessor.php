<?php

/**
 * This file is part of the FOSElasticaBundle project.
 *
 * (c) Infinite Networks Pty Ltd <http://www.infinite.net.au>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace FOS\ElasticaBundle\Index;

use Elastica\Exception\ExceptionInterface;
use FOS\ElasticaBundle\Configuration\IndexConfig;
use FOS\ElasticaBundle\Elastica\Client;
use FOS\ElasticaBundle\Elastica\Index;

class AliasProcessor
{
    /**
     * Sets the randomised root name for an index.
     *
     * @param IndexConfig $indexConfig
     * @param Index $index
     */
    public function setRootName(IndexConfig $indexConfig, Index $index)
    {
        $index->overrideName(
            sprintf('%s_%s',
                $indexConfig->getElasticSearchName(),
                date('Y-m-d-His')
            )
        );
    }

    /**
     * Switches an index to become the new target for an alias. Only applies for
     * indexes that are set to use aliases.
     *
     * @param IndexConfig $indexConfig
     * @param Index $index
     * @throws \RuntimeException
     */
    public function switchIndexAlias(IndexConfig $indexConfig, Index $index)
    {
        $client = $index->getClient();

        $aliasName = $indexConfig->getElasticSearchName();
        $oldIndexName = false;
        $newIndexName = $index->getName();

        $aliasedIndexes = $this->getAliasedIndexes($client, $aliasName);

        if (count($aliasedIndexes) > 1) {
            throw new \RuntimeException(
                sprintf(
                    'Alias %s is used for multiple indexes: [%s].
                    Make sure it\'s either not used or is assigned to one index only',
                    $aliasName,
                    join(', ', $aliasedIndexes)
                )
            );
        }

        $aliasUpdateRequest = array('actions' => array());
        if (count($aliasedIndexes) == 1) {
            // if the alias is set - add an action to remove it
            $oldIndexName = $aliasedIndexes[0];
            $aliasUpdateRequest['actions'][] = array(
                'remove' => array('index' => $oldIndexName, 'alias' => $aliasName)
            );
        }

        // add an action to point the alias to the new index
        $aliasUpdateRequest['actions'][] = array(
            'add' => array('index' => $newIndexName, 'alias' => $aliasName)
        );

        try {
            $client->request('_aliases', 'POST', $aliasUpdateRequest);
        } catch (ExceptionInterface $renameAliasException) {
            $additionalError = '';
            // if we failed to move the alias, delete the newly built index
            $this->deleteIndexes($index, $newIndexName);

            throw new \RuntimeException(
                sprintf(
                    'Failed to updated index alias: %s. %s',
                    $renameAliasException->getMessage(),
                    $additionalError ?: sprintf('Newly built index %s was deleted', $newIndexName)
                ), 0, $renameAliasException
            );
        }

        // Delete the old index(es) after the alias has been switched
        $similarIndexes = $this->findSimilarIndexes($index, $newIndexName);
        if ($oldIndexName) {
            $similarIndexes[] = $oldIndexName;
            $similarIndexes = array_unique($similarIndexes);
            sort($similarIndexes);
        }

        $offset = count($similarIndexes) - $indexConfig->getSaveLastIndex();
        $toDelete = array_slice($similarIndexes, 0, $offset);
        $similarIndexes = array_slice($similarIndexes, $offset);
ld($index->getName(), $oldIndexName, $toDelete, $similarIndexes);
        $this->deleteIndexes($index, $toDelete);
    }

    /**
     * Finding simalar indexes by new index name
     * Index is similar, if index name is similar for >= 90%
     *
     * @param Index $index
     *
     * @return array
     */
    private function findSimilarIndexes(Index $index, $newIndexName)
    {
        $client = $index->getClient();
        $indexesInfo = $client->request('_aliases', 'GET')->getData();

        $similarIndexes = array();
        foreach ($indexesInfo as $indexName => $indexInfo) {
            if ($newIndexName === $indexName) {
                continue;
            }

            similar_text($newIndexName, $indexName, $percent);
            if ($percent >= 90) { //90% of similarity is more then enough
                $similarIndexes[] = $indexName;
            }
        }
        sort($similarIndexes); //sort in asc oldest first, newest last
        return $similarIndexes;
    }

    /**
     * Deleting index(es)
     *
     * @param Index        $index
     * @param string|array $indexes
     *
     * @throws \RuntimeException
     */
    private function deleteIndexes(Index $index, $indexes)
    {
        $client = $index->getClient();

        if (!is_array($indexes)) {
            $indexes = array($indexes);
        }

        while ($indexName = array_pop($indexes)) {
            try {
                $oldIndex = new Index($client, $indexName);
                $oldIndex->delete();
            } catch (ExceptionInterface $e) {
                throw new \RuntimeException(
                    sprintf(
                        'Failed to delete index %s with message: %s',
                        $indexName,
                        $e->getMessage()
                    ),
                    $e->getCode(),
                    $e
                );
            }
        }
    }

    /**
     * Returns array of indexes which are mapped to given alias
     *
     * @param Client $client
     * @param string $aliasName Alias name
     *
     * @return array
     */
    private function getAliasedIndexes(Client $client, $aliasName)
    {
        $aliasesInfo = $client->request('_aliases', 'GET')->getData();
        $aliasedIndexes = array();

        foreach ($aliasesInfo as $indexName => $indexInfo) {
            $aliases = array_keys($indexInfo['aliases']);
            if (in_array($aliasName, $aliases)) {
                $aliasedIndexes[] = $indexName;
            }
        }

        return $aliasedIndexes;
    }
}
