<?php
declare(strict_types=1);

namespace Lmc\AerospikeCache;

use Doctrine\Common\Cache\CacheProvider;
use Psr\Cache\CacheItemPoolInterface;

class DoctrineSymfonyAdapter extends CacheProvider
{
    /** @var CacheItemPoolInterface */
    private $cacheItemPool;

    public function __construct(CacheItemPoolInterface $cacheItemPool)
    {
        $this->cacheItemPool = $cacheItemPool;
    }

    protected function doFetch($id)
    {
        $cacheItem = $this->cacheItemPool->getItem($id);

        if ($cacheItem->isHit()) {
            return $cacheItem->get();
        }

        return false;
    }

    protected function doContains($id)
    {
        return $this->cacheItemPool->hasItem($id);
    }

    protected function doSave($id, $data, $lifeTime = 0)
    {
        $cacheItem = $this->cacheItemPool->getItem($id);

        $cacheItem->set($data);
        $cacheItem->expiresAfter($lifeTime);

        return $this->cacheItemPool->save($cacheItem);
    }

    protected function doDelete($id)
    {
        return $this->cacheItemPool->deleteItem($id);
    }

    protected function doFlush()
    {
        return $this->cacheItemPool->clear();
    }

    protected function doGetStats()
    {
        return null;
    }
}
