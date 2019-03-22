<?php
declare(strict_types=1);

namespace Lmc\AerospikeCache;

use PHPUnit\Framework\TestCase;

class AerospikeCacheTest extends TestCase
{
    /**
     * @dataProvider doHaveProvider
     */
    public function testDoHave(int $statusCode, bool $expectedValue): void
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

        $this->assertEquals($expectedValue, $hasItem);
    }

    public function doHaveProvider(): array
    {
        return [
            [\Aerospike::OK, true],
            [\Aerospike::ERR_RECORD_NOT_FOUND, false],
        ];
    }

    /**
     * @dataProvider doSaveProvider
     */
    public function testDoSave(int $statusCode, bool $expectedResult): void
    {
        $aerospikeMock = $this->createMock(\Aerospike::class);
        $aerospikeCache = new AerospikeCache($aerospikeMock, 'test', 'cache');

        $aerospikeMock->method('put')
                ->willReturn($statusCode);

        $aerospikeMock->method('get')
                ->willReturn(\Aerospike::OK);

        $aerospikeKey = ['ns' => 'test', 'set' => 'cache', 'key' => 'foo'];
        $aerospikeMock->method('initKey')
                ->willReturn($aerospikeKey);

        $cacheItem = $aerospikeCache->getItem('foo');
        $result = $aerospikeCache->save($cacheItem);

        $this->assertEquals($expectedResult, $result);
    }

    public function doSaveProvider(): array
    {
        return [
            [\Aerospike::OK, true],
            [\Aerospike::ERR_CLIENT, false],
        ];
    }

    /**
     * @dataProvider doClearWithEmptyNamespaceProvider
     */
    public function testDoClearWithEmptyNamespace(int $statusCode, bool $expectedResult): void
    {
        $aerospikeMock = $this->createMock(\Aerospike::class);
        $aerospikeCache = new AerospikeCache($aerospikeMock, 'test', 'cache');

        $aerospikeMock->method('truncate')->willReturn($statusCode);

        $result = $aerospikeCache->clear();

        $this->assertEquals($expectedResult, $result);
    }

    public function doClearWithEmptyNamespaceProvider(): array
    {
        return [[\Aerospike::OK, true] , [\Aerospike::ERR_CLIENT, false]];
    }

    /**
     * @dataProvider doClearNamespaceProvider
     * @param mixed $expectedValue
     * @param mixed $statusCodeForRemove
     * @param mixed $statusCodeForScan
     */
    public function testDoClearNamespace(int $statusCodeForRemove, int $statusCodeForScan, bool $expectedValue): void
    {
        $aerospikeMock = $this->createMock(\Aerospike::class);
        $aerospikeCache = new AerospikeCache($aerospikeMock, 'test', 'cache', 'testNamespace');

        $aerospikeMock->method('remove')->willReturn($statusCodeForRemove);

        $aerospikeMock->method('scan')->willReturnCallback(function ($namespace, $set, $callback) use ($statusCodeForScan) {
            call_user_func($callback, ['key' => ['key' => 'testNamespace::test']]);

            return $statusCodeForScan;
        });

        $clearResult = $aerospikeCache->clear('testNamespace');

        $this->assertEquals($expectedValue, $clearResult);
    }

    public function doClearNamespaceProvider(): array
    {
        return [
            [\Aerospike::OK, \Aerospike::OK, true],
            [\Aerospike::OK, \Aerospike::ERR_CLIENT, false],
            [\Aerospike::ERR_CLIENT, \Aerospike::ERR_CLIENT, false],
            [\Aerospike::ERR_CLIENT, \Aerospike::OK, false],
        ];
    }

    /**
     * @dataProvider doDeleteProvider
     */
    public function testDoDelete(int $aerospikeStatus, bool $expectedValue): void
    {
        $aerospikeMock = $this->createMock(\Aerospike::class);
        $aerospikeCache = new AerospikeCache($aerospikeMock, 'test', 'cache', 'testNamespace');

        $aerospikeMock->method('initKey')->willReturn(['foo']);
        $aerospikeMock->method('remove')->willReturn($aerospikeStatus);

        $result = $aerospikeCache->deleteItem('foo');

        $this->assertEquals($expectedValue, $result);
    }

    public function doDeleteProvider(): array
    {
        return [
            [\Aerospike::OK, true],
            [\Aerospike::ERR_CLIENT, false],
        ];
    }
}
