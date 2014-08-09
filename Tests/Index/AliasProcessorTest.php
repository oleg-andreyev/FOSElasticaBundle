<?php
/**
 * This file is part of the FOSElasticaBundle project.
 *
 * (c) Infinite Networks Pty Ltd <http://www.infinite.net.au>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace FOS\ElasticaBundle\Tests\Index;

use FOS\ElasticaBundle\Index\AliasProcessor;

/**
 * AliasProcessorTest
 *
 * @author Oleg Andreyev <oleg.andreyev@intexsys.lv>
 */
class AliasProcessorTest extends \PHPUnit_Framework_TestCase
{
    public function testSetRootName()
    {
        $indexConfig = $this->getIndexConfig();
        $index       = $this->getIndex();
        $data        = date('Y-m-d-His');

        $indexConfig->expects($this->once())->method('getElasticSearchName')->willReturn('index_name');
        $index->expects($this->once())->method('overrideName')->with('index_name_' . $data);

        $aliasProcessor = new AliasProcessor();
        $aliasProcessor->setRootName($indexConfig, $index);
    }

    public function testSwitchIndexAliasThrowsExceptionOnManyAliasedIndexes()
    {
        $indexConfig = $this->getIndexConfig();
        $index       = $this->getIndex();
        $client      = $this->getClient();
        $response    = $this->getResponse();

        $response->expects($this->once())
            ->method('getData')
            ->willReturn(
                array(
                    'index1' => array(
                        'aliases' => array('alias')
                    ),
                    'index2' => array(
                        'aliases' => array('alias')
                    )
                )
            );

        $client->expects($this->once())
            ->method('request')
            ->with('_aliases', 'GET')
            ->willReturn($response);

        $indexConfig->expects($this->once())->method('getElasticSearchName')->willReturn('index_alias');
        $index->expects($this->once())->method('getClient')->willReturn($client);

        $this->setExpectedException(
            'RuntimeException',
            'Alias index_alias is used for multiple indexes: [index1, index2]. Make sure it\'s either not used or is assigned to one index only'
        );

        $aliasProcessor = new AliasProcessor();
        $aliasProcessor->switchIndexAlias($indexConfig, $index);
    }

    /**
     * @return array
     */
    public function switchIndexAliasThrowsExceptionOnFailedSwitchAliasesProvider()
    {
        return array(
            array(
                array('index1' => array('aliases' => array()), 'index2' => array('aliases' => array('alias'))),
                array(
                    'actions' => array(
                        array(
                            'add' => array(
                                'index' => 'index_name',
                                'alias' => 'index_alias'
                            )
                        )
                    )
                )
            ),
            array(
                array('index1' => array('aliases' => array()), 'index2' => array('aliases' => array())),
                array(
                    'actions' => array(
                        array(
                            'add' => array(
                                'index' => 'index_name',
                                'alias' => 'index_alias'
                            )
                        )
                    )
                )
            )
        );
    }

    /**
     * @dataProvider switchIndexAliasThrowsExceptionOnFailedSwitchAliasesProvider
     */
    public function testSwitchIndexAliasThrowsExceptionOnFailedSwitchAliasesAndDeleteIndexesThrowsException($response, $aliasUpdateRequest)
    {
        $indexConfig                 = $this->getIndexConfig();
        $index                       = $this->getIndex();
        $client                      = $this->getClient();
        $response                    = $this->getResponse();

        $renameAliasException        = $this->getException();
        $deleteNewIndexException     = $this->getException();

        $response->expects($this->at(0))
            ->method('getData')
            ->willReturn($response);

        $client->expects($this->at(0))
            ->method('request')
            ->with('_aliases', 'GET')
            ->willReturn($response);

        $client->expects($this->at(1))
            ->method('request')
            ->with('_aliases', 'POST', $aliasUpdateRequest)
            ->willThrowException($renameAliasException);

        $client->expects($this->at(2))
            ->method('request')
            ->with('index_name/', 'DELETE')
            ->willThrowException($deleteNewIndexException);

        $indexConfig->expects($this->once())->method('getElasticSearchName')->willReturn('index_alias');
        $index->expects($this->exactly(2))->method('getClient')->willReturn($client);
        $index->expects($this->once())->method('getName')->willReturn('index_name');

        $this->setExpectedException(
            'RuntimeException',
            'Failed to updated index alias: . Tried to delete newly built index index_name, but also failed: '
        );

        $aliasProcessor = new AliasProcessor();
        $aliasProcessor->switchIndexAlias($indexConfig, $index);
    }

    /**
     * @return \PHPUnit_Framework_MockObject_MockObject
     */
    public function getIndexConfig()
    {
        return $this->getMockBuilder('FOS\ElasticaBundle\Configuration\IndexConfig')
            ->disableOriginalConstructor()
            ->getMock();
    }

    /**
     * @return \PHPUnit_Framework_MockObject_MockObject
     */
    public function getIndex()
    {
        return $this->getMockBuilder('FOS\ElasticaBundle\Elastica\Index')
            ->disableOriginalConstructor()
            ->getMock();
    }

    /**
     * @return \PHPUnit_Framework_MockObject_MockObject
     */
    public function getClient()
    {
        return $this->getMockBuilder('FOS\ElasticaBundle\Elastica\Client')
            ->disableOriginalConstructor()
            ->getMock();
    }

    /**
     * @return \PHPUnit_Framework_MockObject_MockObject
     */
    public function getResponse()
    {
        return $this->getMockBuilder('\Elastica\Response')
            ->disableOriginalConstructor()
            ->getMock();
    }

    /**
     * @return \PHPUnit_Framework_MockObject_MockObject
     */
    public function getException()
    {
        return $this->getMockBuilder('Elastica\Exception\RuntimeException')
            ->disableOriginalConstructor()
            ->getMock();
    }
}
 