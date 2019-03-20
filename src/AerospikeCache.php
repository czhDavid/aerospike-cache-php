<?php declare(strict_types=1);

namespace Lmc\AerospikeCache;

use Symfony\Component\Cache\Adapter\AbstractAdapter;
use Symfony\Component\Cache\Exception\CacheException;

class AerospikeCache extends AbstractAdapter
{
    private const WRAPPER_NAME = 'data';

    /** @var \Aerospike */
    private $aerospike;

    /** @var string */
    private $namespace;

    /** @var string */
    private $set;

    public function __construct(
        \Aerospike $aerospike,
        string $namespace,
        string $set = 'cache',
        string $cacheNamespace = '',
        int $defaultLifetime = 0
    ) {
        $this->aerospike = $aerospike;
        $this->namespace = $namespace;
        $this->set = $set;
        parent::__construct($cacheNamespace, $defaultLifetime);
    }

    protected function doFetch(array $ids): array
    {
        $result = [];

        $keys = $this->initializeKeysForAerospike($ids);

        $this->aerospike->getMany($keys, $records);

        foreach ($records as $record) {
            if ($record['metadata'] !== null) {
                $result[$record['key']['key']] = isset($record['bins'][self::WRAPPER_NAME]) ?
                    $record['bins'][self::WRAPPER_NAME] : null;
            }
        }

        return $result;
    }

    private function initializeKeysForAerospike(array $ids): array
    {
        return array_reduce(
            $ids,
            function (array $keys, $id) {
                $keys[] = $this->createKey($id);

                return $keys;
            },
            []
        );
    }

    protected function doHave($id)
    {
        $statusCode = $this->aerospike->get($this->createKey($id), $read);

        return $statusCode === \Aerospike::OK;
    }

    protected function doClear($namespace = ''): bool
    {
        if ($namespace === '') {
            $statusCode = $this->aerospike->truncate($this->namespace, $this->set, 0);
        } else {
            $clearNamespace = function ($record) use ($namespace): void {
                if ($namespace === mb_substr($record['key']['key'], 0, mb_strlen($namespace))) {
                    $status = $this->aerospike->remove($record['key']);
                    if (!$this->isStatusOkOrNotFound($status)) {
                        throw new CacheException($this->aerospike->error());
                    }
                }
            };

            $this->aerospike->scan($this->namespace, $this->set, $clearNamespace);

            $statusCode = 0;
        }

        return $this->isStatusOkOrNotFound($statusCode);
    }

    protected function doDelete(array $ids): bool
    {
        $removedAllItems = true;

        foreach ($ids as $id) {
            $statusCode = $this->aerospike->remove($this->createKey($id));
            if (!$this->isStatusOkOrNotFound($statusCode)) {
                $removedAllItems = false;
            }
        }

        return $removedAllItems;
    }

    protected function doSave(array $values, $lifetime): bool
    {
        foreach ($values as $key => $value) {
            $data = [self::WRAPPER_NAME => $value];
            $statusCode = $this->aerospike->put($this->createKey($key), $data, $lifetime, [\Aerospike::OPT_POLICY_KEY => \Aerospike::POLICY_KEY_SEND]);
        }

        return $statusCode === \Aerospike::OK;
    }

    private function createKey(string $key): array
    {
        return $this->aerospike->initKey($this->namespace, $this->set, $key);
    }

    private function isStatusOkOrNotFound(int $statusCode): bool
    {
        return $statusCode === \Aerospike::ERR_RECORD_NOT_FOUND || $statusCode === \Aerospike::OK;
    }
}
