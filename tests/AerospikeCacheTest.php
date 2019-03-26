<?php
declare(strict_types=1);

namespace Lmc\AerospikeCache;

use PHPUnit\Framework\TestCase;

class AerospikeCacheTest extends TestCase
{
    /**
     * @dataProvider provideStatusCodes
     */
    public function testShouldConfirmItemPresence(int $statusCode, bool $expectedReturnedValue): void
    {
        $aerospikeMock = $this->createMock(\Aerospike::class);
        $aerospikeCache = new AerospikeCache($aerospikeMock, 'test', 'cache');

        $aerospikeKey = ['ns' => 'test', 'set' => 'cache', 'key' => 'foo'];
        $aerospikeMock->expects($this->once())
            ->method('initKey')
            ->willReturn($aerospikeKey);

        $aerospikeMock->expects($this->once())
            ->method('get')
            ->with($aerospikeKey)
            ->willReturn($statusCode);

        $hasItem = $aerospikeCache->hasItem('foo');

        $this->assertSame($expectedReturnedValue, $hasItem);
    }

    /**
     * @dataProvider provideStatusCodes
     */
    public function testShouldSaveItem(int $aerospikeStatusCode, bool $expectedSaveSuccessful): void
    {
        $aerospikeMock = $this->createMock(\Aerospike::class);
        $aerospikeCache = new AerospikeCache($aerospikeMock, 'test', 'cache');

        $aerospikeMock->method('put')
                ->willReturn($aerospikeStatusCode);

        $aerospikeMock->method('get')
                ->willReturn(\Aerospike::OK);

        $aerospikeKey = ['ns' => 'test', 'set' => 'cache', 'key' => 'foo'];
        $aerospikeMock->method('initKey')
                ->willReturn($aerospikeKey);

        $cacheItem = $aerospikeCache->getItem('foo');
        $saveSuccessful = $aerospikeCache->save($cacheItem);

        $this->assertSame($expectedSaveSuccessful, $saveSuccessful);
    }

    /**
     * @dataProvider provideStatusCodes
     */
    public function testShouldClearWithEmptyNamespaceName(int $aerospikeStatusCode, bool $expectedClearSuccessful): void
    {
        $aerospikeMock = $this->createMock(\Aerospike::class);
        $aerospikeCache = new AerospikeCache($aerospikeMock, 'test', 'cache');

        $aerospikeMock->method('truncate')
            ->willReturn($aerospikeStatusCode);

        $clearSuccessful = $aerospikeCache->clear();

        $this->assertSame($expectedClearSuccessful, $clearSuccessful);
    }

    public function doClearWithEmptyNamespaceProvider(): array
    {
        return [[\Aerospike::OK, true] , [\Aerospike::ERR_CLIENT, false]];
    }

    /**
     * @dataProvider provideStatusCodesForClearingNamespace
     */
    public function testShouldClearNamespace(int $statusCodeForRemove, int $statusCodeForScan, bool $expectedValue): void
    {
        $aerospikeMock = $this->createMock(\Aerospike::class);
        $aerospikeCache = new AerospikeCache($aerospikeMock, 'test', 'cache', 'testNamespace');

        $aerospikeMock->method('remove')
            ->willReturn($statusCodeForRemove);

        $aerospikeMock->method('scan')
            ->willReturnCallback(function ($namespace, $set, $callback) use ($statusCodeForScan) {
                $callback(['key' => ['key' => 'testNamespace::test']]);

                return $statusCodeForScan;
            });

        $clearResult = $aerospikeCache->clear();

        $this->assertSame($expectedValue, $clearResult);
    }

    public function provideStatusCodesForClearingNamespace(): array
    {
        return [
            [\Aerospike::OK, \Aerospike::OK, true],
            [\Aerospike::OK, \Aerospike::ERR_CLIENT, false],
            [\Aerospike::ERR_CLIENT, \Aerospike::ERR_CLIENT, false],
            [\Aerospike::ERR_CLIENT, \Aerospike::OK, false],
        ];
    }

    /**
     * @dataProvider provideStatusCodes
     */
    public function testShouldDeleteItem(int $aerospikeStatusCode, bool $expectedDeleteToSucced): void
    {
        $aerospikeMock = $this->createMock(\Aerospike::class);
        $aerospikeCache = new AerospikeCache($aerospikeMock, 'test', 'cache', 'testNamespace');

        $aerospikeMock->method('initKey')
            ->willReturn(['foo']);
        $aerospikeMock->method('remove')
            ->willReturn($aerospikeStatusCode);

        $deleteSuccessful = $aerospikeCache->deleteItem('foo');

        $this->assertSame($expectedDeleteToSucced, $deleteSuccessful);
    }

    public function provideStatusCodes(): array
    {
        return [
            [\Aerospike::OK, true],
            [\Aerospike::ERR_CLIENT, false],
        ];
    }

    /**
     * @dataProvider doFetchProvider
     */
    public function testShouldReadExistingRecordsFromAerospike(array $cacheItemKeys, array $valuesReturnedByAerospike): void
    {
        $mockedAerospikeKeys = array_map(
            function ($key) {
                return ['ns' => 'aerospike', 'set' => 'cache', 'key' => $key];
            },
            $cacheItemKeys
        );

        $aerospikeMock = $this->createMock(\Aerospike::class);
        $aerospikeCache = new AerospikeCache($aerospikeMock, 'test', 'cache');

        $aerospikeMock
            ->method('initKey')
            ->willReturnOnConsecutiveCalls(...$mockedAerospikeKeys);

        $aerospikeMock->expects($this->once())
            ->method('getMany')
            ->with($this->equalTo($mockedAerospikeKeys))
            ->willReturnCallback(function ($keys, &$records) use ($valuesReturnedByAerospike) {
                foreach ($keys as $key) {
                    if (isset($valuesReturnedByAerospike[$key['key']])) {
                        $record = [
                            'key' => $key,
                            'bins' => ['data' => $valuesReturnedByAerospike[$key['key']]],
                            'metadata' => ['ttl' => 1000, 'generation' => 2],
                        ];
                    } else {
                        $record = [
                            'key' => $key,
                            'bins' => null,
                            'metadata' => null,
                        ];
                    }

                    $records[] = $record;
                }

                return \Aerospike::OK;
            });

        $items = $aerospikeCache->getItems($cacheItemKeys);

        foreach ($items as $item) {
            if (isset($valuesReturnedByAerospike[$item->getKey()])) {
                $this->assertTrue($item->isHit());
                $this->assertEquals($valuesReturnedByAerospike[$item->getKey()], $item->get());
            } else {
                $this->assertFalse($item->isHit());
            }
        }
    }

    public function doFetchProvider(): array
    {
        return [
            [['foo'], ['foo' => 'bar']],
            [['foo'], []],
            [['foo', 'hello'], ['foo' => 'bar', 'hello' => 'world']],
            [['foo', 'hello'], ['foo' => 'bar']],
        ];
    }
}
